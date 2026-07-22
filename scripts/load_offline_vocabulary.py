#!/usr/bin/env python3
"""Load one vocabulary's offline artifacts into MongoDB, Neo4j, and Qdrant.

Database connection settings use the application's existing ``BTS_*`` environment
variables. Import tuning is intentionally conservative and uses separate ``BTS_IMPORT_*``
variables; see ``--help`` for the defaults.

Annotations are deliberately not imported by this script.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import csv
import json
import math
import os
import random
import struct
import sys
from collections.abc import Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar
from uuid import UUID

import numpy as np
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import ClientError
from pymongo import ASCENDING, AsyncMongoClient, ReplaceOne, UpdateOne
from qdrant_client import AsyncQdrantClient
from qdrant_client.http.models import HnswConfigDiff
from qdrant_client.models import Distance, PointStruct, VectorParams

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod


T = TypeVar('T')
DEFAULT_BATCH_SIZE = 250
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_RETRIES = 5
EMBEDDING_HEADER = struct.Struct('<4sH I H H')
EMBEDDING_BLOCK_HEADER = struct.Struct('<I')
GRAPH_EDGE_UPSERT_QUERY = """
UNWIND $edges AS edge
MERGE (source:Concept {id: edge.source, prefix: $prefix})
MERGE (target:Concept {id: edge.target, prefix: $prefix})
WITH source, target, edge
CALL apoc.merge.relationship(source, edge.type, {}, {}, target) YIELD rel
WITH rel, edge.key AS rel_key
FOREACH (_ IN CASE WHEN rel_key IS NULL THEN [] ELSE [1] END |
    SET rel.label = reduce(
        unique_labels = [],
        item IN (
            CASE
                WHEN rel.label IS NULL THEN []
                WHEN rel.label IS TYPED LIST<ANY> THEN rel.label
                ELSE [rel.label]
            END + [rel_key]
        ) |
        CASE
            WHEN item IN unique_labels THEN unique_labels
            ELSE unique_labels + item
        END
    )
)
RETURN count(rel) AS upserted
"""


@dataclass(frozen=True)
class EmbeddingRecord:
    concept_id: str
    vector_id: UUID
    vector: np.ndarray


def _env_int(name: str, default: int) -> int:
    value = int(os.getenv(name, default))
    if value < 1:
        raise ValueError(f'{name} must be at least 1')
    return value


def _env_float(name: str, default: float) -> float:
    value = float(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f'{name} must be greater than 0')
    return value


def batched(items: Iterable[T], size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


async def retry(
    label: str,
    operation: Callable[[], Awaitable[T]],
    *,
    timeout: float,
    attempts: int,
) -> T:
    """Run an idempotent operation with timeout and exponential backoff."""
    for attempt in range(1, attempts + 1):
        try:
            return await asyncio.wait_for(operation(), timeout=timeout)
        except (KeyboardInterrupt, asyncio.CancelledError):
            raise
        except Exception as exc:
            if isinstance(exc, ClientError):
                raise RuntimeError(f'{label} failed with a non-retryable Neo4j client error') from exc
            if attempt == attempts:
                raise RuntimeError(f'{label} failed after {attempts} attempts') from exc
            delay = min(30.0, 2 ** (attempt - 1)) + random.uniform(0, 0.25)
            print(
                f'  {label}: attempt {attempt}/{attempts} failed '
                f'({type(exc).__name__}: {exc}); retrying in {delay:.1f}s',
                file=sys.stderr,
            )
            await asyncio.sleep(delay)
    raise AssertionError('unreachable')


def count_lines(path: Path) -> int:
    with path.open('rb') as file:
        return sum(1 for _ in file)


def read_json_lines(path: Path) -> Iterator[dict]:
    with path.open(encoding='utf-8') as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f'Invalid JSON in {path}:{line_number}') from exc


def read_csv_rows(path: Path) -> Iterator[list[str]]:
    with path.open(encoding='utf-8', newline='') as file:
        yield from csv.reader(file)


def progress(label: str, completed: int, total: int | None = None) -> None:
    suffix = f'/{total}' if total is not None else ''
    print(f'  {label}: {completed}{suffix}', flush=True)


async def ensure_mongo_index(collection, field: str, *, unique: bool = False) -> None:
    """Create the project's named index only when an equivalent index is absent."""
    indexes = await collection.index_information()
    wanted_key = [(field, ASCENDING)]
    for name, definition in indexes.items():
        if definition.get('key') == wanted_key:
            if unique and not definition.get('unique', False):
                raise RuntimeError(
                    f'MongoDB index {name!r} already covers {field!r} but is not unique; '
                    'refusing to create a duplicate/conflicting index.'
                )
            return
    await collection.create_index(field, unique=unique, name=f'{field}_index')


async def load_documents(
    mongo: AsyncMongoClient,
    prefix: ConceptPrefix,
    path: Path,
    *,
    batch_size: int,
    timeout: float,
    attempts: int,
) -> None:
    print(f'Loading MongoDB documents from {path.name}')
    db = mongo[CONFIG.mongodb_db_name]
    collection = db[prefix.value]
    total = count_lines(path)
    completed = 0
    for documents in batched(read_json_lines(path), batch_size):
        for document in documents:
            if document.get('prefix') != prefix.value:
                raise ValueError(f'{path} contains a non-{prefix.value} document')
            if not document.get('conceptId'):
                raise ValueError(f'{path} contains a document without conceptId')
        operations = [
            ReplaceOne({'conceptId': document['conceptId']}, document, upsert=True)
            for document in documents
        ]
        await retry(
            'MongoDB document batch',
            lambda operations=operations: collection.bulk_write(operations, ordered=True),
            timeout=timeout,
            attempts=attempts,
        )
        completed += len(documents)
        progress('documents', completed, total)


async def ensure_neo4j_indexes(neo4j, timeout: float, attempts: int) -> None:
    statements = (
        'CREATE INDEX concept_prefix_index IF NOT EXISTS FOR (n:Concept) ON (n.prefix)',
        'CREATE INDEX concept_id_index IF NOT EXISTS FOR (n:Concept) ON (n.id)',
        'CREATE CONSTRAINT concept_prefix_id_unique IF NOT EXISTS '
        'FOR (n:Concept) REQUIRE (n.prefix, n.id) IS UNIQUE',
    )
    async with neo4j.session(database=CONFIG.neo4j_db_name) as session:
        for statement in statements:
            async def create(statement=statement):
                result = await session.run(statement)
                await result.consume()
            await retry('Neo4j index/constraint', create, timeout=timeout, attempts=attempts)


def parse_concept_types(value: str) -> list[str]:
    if not value:
        return []
    parsed = ast.literal_eval(value)
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise ValueError(f'Invalid conceptTypes value: {value!r}')
    return [item for item in parsed if item.strip()]


async def load_graph(
    neo4j,
    prefix: ConceptPrefix,
    node_path: Path,
    edge_path: Path,
    *,
    batch_size: int,
    timeout: float,
    attempts: int,
) -> None:
    print(f'Loading Neo4j nodes from {node_path.name}')
    node_total = count_lines(node_path)
    completed = 0
    async with neo4j.session(database=CONFIG.neo4j_db_name) as session:
        for rows in batched(read_csv_rows(node_path), batch_size):
            nodes = []
            for row in rows:
                if len(row) < 2:
                    raise ValueError(f'Malformed node row in {node_path}: {row!r}')
                nodes.append({'id': row[0], 'types': parse_concept_types(row[1])})

            async def save_nodes(nodes=nodes):
                result = await session.run(
                    """
                    UNWIND $nodes AS node
                    MERGE (n:Concept {id: node.id, prefix: $prefix})
                    WITH n, [t IN node.types WHERE t IS NOT NULL AND trim(t) <> ''] AS labels
                    SET n:$(labels)
                    RETURN count(n) AS upserted
                    """,
                    nodes=nodes,
                    prefix=prefix.value,
                )
                record = await result.single()
                if record is None or record['upserted'] != len(nodes):
                    raise RuntimeError('Neo4j did not upsert every node in the batch')

            await retry('Neo4j node batch', save_nodes, timeout=timeout, attempts=attempts)
            completed += len(nodes)
            progress('nodes', completed, node_total)

        print(f'Loading Neo4j relationships from {edge_path.name}')
        edge_total = count_lines(edge_path)
        completed = 0
        for rows in batched(read_csv_rows(edge_path), batch_size):
            edges = []
            for row in rows:
                if len(row) < 3:
                    raise ValueError(f'Malformed edge row in {edge_path}: {row!r}')
                edges.append({
                    'source': row[0],
                    'target': row[1],
                    'type': row[2] or 'related_to',
                    'key': row[3] if len(row) > 3 and row[3] else None,
                })

            async def save_edges(edges=edges):
                result = await session.run(
                    GRAPH_EDGE_UPSERT_QUERY,
                    edges=edges,
                    prefix=prefix.value,
                )
                record = await result.single()
                if record is None or record['upserted'] != len(edges):
                    raise RuntimeError(
                        'Neo4j did not upsert every relationship in the batch'
                    )

            await retry('Neo4j relationship batch', save_edges, timeout=timeout, attempts=attempts)
            completed += len(edges)
            progress('relationships', completed, edge_total)


def read_embeddings(path: Path) -> Iterator[EmbeddingRecord]:
    """Stream the project's EMB1 format without importing the ML stack."""
    with path.open('rb') as file:
        header = file.read(EMBEDDING_HEADER.size)
        if len(header) != EMBEDDING_HEADER.size:
            raise EOFError(f'Truncated embedding header in {path}')
        magic, version, dimension, flags, _padding = EMBEDDING_HEADER.unpack(header)
        if magic != b'EMB1' or version != 1 or flags != 0 or dimension < 1:
            raise ValueError(f'Unsupported embedding header in {path}')
        vector_bytes = dimension * 4

        while block_header := file.read(EMBEDDING_BLOCK_HEADER.size):
            if len(block_header) != EMBEDDING_BLOCK_HEADER.size:
                raise EOFError(f'Truncated embedding block header in {path}')
            (block_length,) = EMBEDDING_BLOCK_HEADER.unpack(block_header)
            payload = file.read(block_length)
            if len(payload) != block_length:
                raise EOFError(f'Truncated embedding block in {path}')
            view = memoryview(payload)
            if len(view) < 4:
                raise EOFError(f'Malformed embedding block in {path}')
            row_count = struct.unpack_from('<I', view, 0)[0]
            offset = 4
            for _ in range(row_count):
                if offset + 2 > len(view):
                    raise EOFError(f'Malformed embedding concept ID in {path}')
                id_length = struct.unpack_from('<H', view, offset)[0]
                offset += 2
                end = offset + id_length
                if end + 16 + vector_bytes > len(view):
                    raise EOFError(f'Malformed embedding row in {path}')
                concept_id = bytes(view[offset:end]).decode()
                offset = end
                vector_id = UUID(bytes=bytes(view[offset:offset + 16]))
                offset += 16
                vector = np.frombuffer(
                    view[offset:offset + vector_bytes], dtype='<f4', count=dimension,
                ).copy()
                offset += vector_bytes
                yield EmbeddingRecord(concept_id, vector_id, vector)
            if offset != len(view):
                raise ValueError(f'Embedding block has trailing data in {path}')


async def load_embeddings(
    qdrant: AsyncQdrantClient,
    mongo: AsyncMongoClient,
    prefix: ConceptPrefix,
    path: Path,
    *,
    batch_size: int,
    timeout: float,
    attempts: int,
) -> None:
    print(f'Loading Qdrant embeddings from {path.name}')
    first = next(read_embeddings(path), None)
    if first is None:
        print('  embedding file is empty; skipping')
        return
    dimension = len(first.vector)
    collection_name = prefix.value
    exists = await retry(
        'Qdrant collection check',
        lambda: qdrant.collection_exists(collection_name),
        timeout=timeout,
        attempts=attempts,
    )
    if not exists:
        await retry(
            'Qdrant collection creation',
            lambda: qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=dimension, distance=Distance.COSINE),
            ),
            timeout=timeout,
            attempts=attempts,
        )
    else:
        info = await retry(
            'Qdrant collection metadata',
            lambda: qdrant.get_collection(collection_name),
            timeout=timeout,
            attempts=attempts,
        )
        vectors = info.config.params.vectors
        existing_dimension = vectors.size if hasattr(vectors, 'size') else None
        if existing_dimension != dimension:
            raise RuntimeError(
                f'Qdrant collection {collection_name!r} has dimension {existing_dimension}; '
                f'the dump has dimension {dimension}. Refusing to overwrite the collection.'
            )

    await retry(
        'disable Qdrant HNSW indexing',
        lambda: qdrant.update_collection(
            collection_name=collection_name,
            hnsw_config=HnswConfigDiff(m=0),
        ),
        timeout=timeout,
        attempts=attempts,
    )
    collection = mongo[CONFIG.mongodb_db_name][prefix.value]
    completed = 0
    try:
        for containers in batched(read_embeddings(path), batch_size):
            points = [
                PointStruct(
                    id=str(container.vector_id),
                    vector=container.vector.tolist(),
                    payload={'conceptId': container.concept_id},
                )
                for container in containers
            ]
            await retry(
                'Qdrant embedding batch',
                lambda points=points: qdrant.upsert(
                    collection_name=collection_name,
                    points=points,
                    wait=True,
                ),
                timeout=timeout,
                attempts=attempts,
            )
            mappings = [
                UpdateOne(
                    {'conceptId': container.concept_id},
                    {'$set': {'vectorId': str(container.vector_id)}},
                )
                for container in containers
            ]
            await retry(
                'MongoDB vector mapping batch',
                lambda mappings=mappings: collection.bulk_write(mappings, ordered=True),
                timeout=timeout,
                attempts=attempts,
            )
            completed += len(containers)
            progress('embeddings', completed)
    finally:
        await retry(
            'enable Qdrant HNSW indexing',
            lambda: qdrant.update_collection(
                collection_name=collection_name,
                hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
            ),
            timeout=timeout,
            attempts=attempts,
        )


def parse_similarity_filename(path: Path, prefix: ConceptPrefix) -> tuple[SimilarityMethod, ConceptPrefix | None]:
    suffix = '.similarity.dump'
    stem = path.name[:-len(suffix)]
    start = f'{prefix.value}-'
    if not stem.startswith(start):
        raise ValueError(f'Unexpected similarity filename: {path.name}')
    remainder = stem[len(start):]
    for method in sorted(SimilarityMethod, key=lambda value: len(value.value), reverse=True):
        if remainder == method.value:
            return method, None
        method_prefix = f'{method.value}-'
        if remainder.startswith(method_prefix):
            corpus_value = remainder[len(method_prefix):]
            try:
                return method, ConceptPrefix(corpus_value)
            except ValueError as exc:
                raise ValueError(f'Unknown corpus prefix in {path.name}') from exc
    raise ValueError(f'Unknown similarity method in {path.name}')


async def load_similarities(
    neo4j,
    prefix: ConceptPrefix,
    path: Path,
    *,
    batch_size: int,
    timeout: float,
    attempts: int,
) -> None:
    method, corpus = parse_similarity_filename(path, prefix)
    property_name = f'{method.value}:{corpus.value}' if corpus else method.value
    print(f'Loading Neo4j similarities from {path.name} as {property_name!r}')
    total = count_lines(path)
    completed = 0
    async with neo4j.session(database=CONFIG.neo4j_db_name) as session:
        for rows in batched(read_csv_rows(path), batch_size):
            scores = []
            for row in rows:
                if len(row) < 3:
                    raise ValueError(f'Malformed similarity row in {path}: {row!r}')
                score = float(row[2])
                if not math.isfinite(score):
                    raise ValueError(f'Non-finite similarity score in {path}: {row!r}')
                scores.append({'source': row[0], 'target': row[1], 'score': score})

            async def save_scores(scores=scores):
                result = await session.run(
                    """
                    UNWIND $scores AS sim
                    MATCH (source:Concept {id: sim.source, prefix: $prefix})
                    MATCH (target:Concept {id: sim.target, prefix: $prefix})
                    CALL apoc.merge.relationship(source, 'similar_to', {}, {}, target) YIELD rel
                    SET rel[$property] = sim.score
                    RETURN count(rel) AS upserted
                    """,
                    scores=scores,
                    prefix=prefix.value,
                    property=property_name,
                )
                record = await result.single()
                if record is None or record['upserted'] != len(scores):
                    raise RuntimeError(
                        'Neo4j did not upsert every similarity; vocabulary nodes may be missing'
                    )

            await retry('Neo4j similarity batch', save_scores, timeout=timeout, attempts=attempts)
            completed += len(scores)
            progress('similarities', completed, total)


async def finalize_mongo_indexes(
    mongo: AsyncMongoClient,
    prefix: ConceptPrefix,
    *,
    timeout: float,
    attempts: int,
) -> None:
    print('Creating/verifying MongoDB indexes')
    collection = mongo[CONFIG.mongodb_db_name][prefix.value]
    for field, unique in (('label', False), ('nGrams', False)):
        await retry(
            f'MongoDB {field} index',
            lambda field=field, unique=unique: ensure_mongo_index(
                collection, field, unique=unique,
            ),
            timeout=timeout,
            attempts=attempts,
        )


async def run(args: argparse.Namespace) -> None:
    try:
        prefix = ConceptPrefix(args.prefix.lower())
    except ValueError as exc:
        choices = ', '.join(value.value for value in ConceptPrefix)
        raise SystemExit(f'Unknown vocabulary prefix {args.prefix!r}; choose one of: {choices}') from exc

    offline_dir = args.offline_dir.resolve()
    doc_path = offline_dir / f'{prefix.value}.doc.dump'
    node_path = offline_dir / f'{prefix.value}.node_ids.dump'
    graph_path = offline_dir / f'{prefix.value}.graph.dump'
    embed_path = offline_dir / f'{prefix.value}.embed.dump'
    similarity_paths = sorted(offline_dir.glob(f'{prefix.value}-*.similarity.dump'))

    required = [doc_path, node_path, graph_path]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise SystemExit(f'Missing required offline file(s): {", ".join(missing)}')

    print(f'Importing vocabulary {prefix.value!r} from {offline_dir}')
    print(f'Batch size={args.batch_size}, timeout={args.timeout}s, attempts={args.retries}')
    print('Annotation dumps are not loaded.')

    mongo = AsyncMongoClient(
        host=CONFIG.mongodb_host,
        port=CONFIG.mongodb_port,
        username=CONFIG.mongodb_username,
        password=CONFIG.mongodb_password,
        authSource=CONFIG.mongodb_auth_source,
        connectTimeoutMS=int(args.timeout * 1000),
        serverSelectionTimeoutMS=int(args.timeout * 1000),
    )
    neo4j = AsyncGraphDatabase.driver(
        CONFIG.neo4j_uri,
        auth=(CONFIG.neo4j_username, CONFIG.neo4j_password),
        connection_timeout=args.timeout,
        connection_acquisition_timeout=args.timeout,
    )
    qdrant = AsyncQdrantClient(location=CONFIG.qdrant_location, timeout=args.timeout)

    try:
        await retry(
            'MongoDB connection',
            lambda: mongo.admin.command('ping'),
            timeout=args.timeout,
            attempts=args.retries,
        )
        await retry(
            'Neo4j connection',
            neo4j.verify_connectivity,
            timeout=args.timeout,
            attempts=args.retries,
        )

        print('Creating/verifying MongoDB conceptId index before upserts')
        await retry(
            'MongoDB conceptId index',
            lambda: ensure_mongo_index(
                mongo[CONFIG.mongodb_db_name][prefix.value], 'conceptId', unique=True,
            ),
            timeout=args.timeout,
            attempts=args.retries,
        )
        await load_documents(
            mongo, prefix, doc_path,
            batch_size=args.batch_size, timeout=args.timeout, attempts=args.retries,
        )
        await ensure_neo4j_indexes(neo4j, args.timeout, args.retries)
        await load_graph(
            neo4j, prefix, node_path, graph_path,
            batch_size=args.batch_size, timeout=args.timeout, attempts=args.retries,
        )
        if embed_path.is_file():
            await load_embeddings(
                qdrant, mongo, prefix, embed_path,
                batch_size=args.batch_size, timeout=args.timeout, attempts=args.retries,
            )
        else:
            print(f'No {embed_path.name}; skipping Qdrant')
        for similarity_path in similarity_paths:
            await load_similarities(
                neo4j, prefix, similarity_path,
                batch_size=args.batch_size, timeout=args.timeout, attempts=args.retries,
            )
        await finalize_mongo_indexes(
            mongo, prefix, timeout=args.timeout, attempts=args.retries,
        )
    finally:
        await qdrant.close()
        await neo4j.close()
        await mongo.close()

    print(f'Finished importing {prefix.value!r}.')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Database environment variables:
  BTS_DATA_DIR
  BTS_MONGODB_HOST, BTS_MONGODB_PORT, BTS_MONGODB_DB_NAME,
  BTS_MONGODB_USERNAME, BTS_MONGODB_PASSWORD, BTS_MONGODB_AUTH_SOURCE
  BTS_NEO4J_URI, BTS_NEO4J_DB_NAME, BTS_NEO4J_USERNAME, BTS_NEO4J_PASSWORD
  BTS_QDRANT_LOCATION

Import tuning variables:
  BTS_IMPORT_BATCH_SIZE, BTS_IMPORT_TIMEOUT_SECONDS, BTS_IMPORT_RETRIES
""",
    )
    parser.add_argument('prefix', help='Vocabulary prefix, for example: hpo')
    parser.add_argument(
        '--offline-dir',
        type=Path,
        default=Path(CONFIG.data_dir) / 'offline',
        help='Directory containing offline dumps (default: BTS_DATA_DIR/offline)',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=_env_int('BTS_IMPORT_BATCH_SIZE', DEFAULT_BATCH_SIZE),
        help=f'Rows per database request (env BTS_IMPORT_BATCH_SIZE; default {DEFAULT_BATCH_SIZE})',
    )
    parser.add_argument(
        '--timeout',
        type=float,
        default=_env_float('BTS_IMPORT_TIMEOUT_SECONDS', DEFAULT_TIMEOUT_SECONDS),
        help='Timeout per database request in seconds (env BTS_IMPORT_TIMEOUT_SECONDS; default 60)',
    )
    parser.add_argument(
        '--retries',
        type=int,
        default=_env_int('BTS_IMPORT_RETRIES', DEFAULT_RETRIES),
        help=f'Attempts per idempotent request (env BTS_IMPORT_RETRIES; default {DEFAULT_RETRIES})',
    )
    args = parser.parse_args()
    if args.batch_size < 1 or args.timeout <= 0 or args.retries < 1:
        parser.error('--batch-size and --retries must be >= 1; --timeout must be > 0')
    return args


if __name__ == '__main__':
    asyncio.run(run(parse_args()))
