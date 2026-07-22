#!/usr/bin/env python3
"""Load one offline annotation dump into Neo4j.

Connection settings use BTS_NEO4J_URI, BTS_NEO4J_DB_NAME,
BTS_NEO4J_USERNAME, and BTS_NEO4J_PASSWORD. Imports are sequential,
idempotent, timeout-bounded, and retried with exponential backoff.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import sys
from collections.abc import Awaitable, Callable, Iterable, Iterator
from pathlib import Path
from typing import TypeVar

from neo4j import AsyncGraphDatabase
from neo4j.exceptions import ClientError

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.vocabulary.utils import parse_annotation_curie


T = TypeVar('T')
DEFAULT_BATCH_SIZE = 250
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_RETRIES = 5


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
    """Run an idempotent operation with a timeout and exponential backoff."""
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


def infer_prefixes(path: Path) -> tuple[str | None, str | None]:
    """Infer zero, one, or two fallback prefixes from a dump filename."""
    suffix = '.annotation.dump'
    if not path.name.endswith(suffix):
        raise ValueError(f'Annotation dump must end in {suffix}: {path}')
    stem = path.name[:-len(suffix)]
    if not stem:
        return None, None
    parts = stem.split('-')
    if len(parts) == 1:
        return parts[0], None
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(
        f'Cannot infer prefixes from {path.name!r}; use --source-prefix and --target-prefix.'
    )


def canonical_prefix(value: str | ConceptPrefix | None) -> str | ConceptPrefix | None:
    if value is None or not str(value).strip():
        return None
    value = value.value if isinstance(value, ConceptPrefix) else str(value).strip()
    try:
        return ConceptPrefix(value.lower())
    except ValueError:
        # External vocabularies such as mesh may not be in ConceptPrefix.
        return value.lower()


def split_curie(curie: str) -> tuple[str, str]:
    prefix, concept_id = curie.split(':', 1)
    return prefix, concept_id


def parse_annotation_row(
    row: list[str],
    *,
    source_fallback: str | ConceptPrefix | None = None,
    target_fallback: str | ConceptPrefix | None = None,
    location: str = 'row',
) -> dict:
    """Normalize a legacy or current six-column annotation row."""
    if len(row) < 6:
        raise ValueError(f'{location} has {len(row)} columns; expected at least 6')
    source_prefix, source_id, target_prefix, target_id, annotation_type, properties_text = row[:6]
    source_curie = parse_annotation_curie(
        canonical_prefix(source_prefix), source_id, canonical_prefix(source_fallback),
    )
    target_curie = parse_annotation_curie(
        canonical_prefix(target_prefix), target_id, canonical_prefix(target_fallback),
    )
    normalized_source_prefix, normalized_source_id = split_curie(source_curie)
    normalized_target_prefix, normalized_target_id = split_curie(target_curie)
    try:
        properties = json.loads(properties_text) if properties_text.strip() else {}
    except json.JSONDecodeError as exc:
        raise ValueError(f'{location} contains invalid properties JSON') from exc
    if not isinstance(properties, dict):
        raise ValueError(f'{location} properties must be a JSON object')
    try:
        normalized_type = AnnotationType(annotation_type or AnnotationType.ANNOTATED_WITH.value).value
    except ValueError as exc:
        raise ValueError(f'{location} has unknown annotation type {annotation_type!r}') from exc

    return {
        'prefixFrom': normalized_source_prefix,
        'conceptIdFrom': normalized_source_id,
        'prefixTo': normalized_target_prefix,
        'conceptIdTo': normalized_target_id,
        'annotationType': normalized_type,
        'properties': properties,
    }


def read_annotations(
    path: Path,
    *,
    source_fallback: str | ConceptPrefix | None,
    target_fallback: str | ConceptPrefix | None,
) -> Iterator[dict]:
    with path.open(encoding='utf-8', newline='') as file:
        for line_number, row in enumerate(csv.reader(file), 1):
            if not row or not any(value.strip() for value in row):
                continue
            yield parse_annotation_row(
                row,
                source_fallback=source_fallback,
                target_fallback=target_fallback,
                location=f'{path}:{line_number}',
            )


def count_rows(path: Path) -> int:
    with path.open(encoding='utf-8', newline='') as file:
        return sum(1 for row in csv.reader(file) if row and any(value.strip() for value in row))


async def ensure_indexes(neo4j, *, timeout: float, attempts: int) -> None:
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


async def load_annotations(
    neo4j,
    path: Path,
    *,
    source_fallback: str | ConceptPrefix | None,
    target_fallback: str | ConceptPrefix | None,
    batch_size: int,
    timeout: float,
    attempts: int,
) -> None:
    total = count_rows(path)
    completed = 0
    annotations = read_annotations(
        path,
        source_fallback=source_fallback,
        target_fallback=target_fallback,
    )
    async with neo4j.session(database=CONFIG.neo4j_db_name) as session:
        for batch_number, annotation_batch in enumerate(batched(annotations, batch_size), 1):
            async def save_batch(annotation_batch=annotation_batch):
                result = await session.run(
                    """
                    UNWIND $annotations AS annotation
                    MERGE (source:Concept {
                        id: annotation.conceptIdFrom,
                        prefix: annotation.prefixFrom
                    })
                    MERGE (target:Concept {
                        id: annotation.conceptIdTo,
                        prefix: annotation.prefixTo
                    })
                    WITH source, target, annotation,
                        coalesce(annotation.annotationType, 'annotated_with') AS rel_type
                    CALL apoc.merge.relationship(source, rel_type, {}, {}, target) YIELD rel
                    SET rel += coalesce(annotation.properties, {})
                    RETURN count(rel) AS upserted
                    """,
                    annotations=annotation_batch,
                )
                record = await result.single()
                if record is None or record['upserted'] != len(annotation_batch):
                    raise RuntimeError('Neo4j did not upsert every annotation in the batch')

            await retry(
                f'Neo4j annotation batch {batch_number}',
                save_batch,
                timeout=timeout,
                attempts=attempts,
            )
            completed += len(annotation_batch)
            print(f'  annotations: {completed}/{total}', flush=True)


async def run(args: argparse.Namespace) -> None:
    path = args.annotation_dump.resolve()
    if not path.is_file():
        raise SystemExit(f'Annotation dump not found: {path}')
    inferred_source, inferred_target = infer_prefixes(path)
    source_fallback = args.source_prefix or inferred_source
    target_fallback = args.target_prefix or inferred_target

    print(f'Loading annotations from {path}')
    print(f'Fallback prefixes: source={source_fallback!r}, target={target_fallback!r}')
    print(f'Batch size={args.batch_size}, timeout={args.timeout}s, attempts={args.retries}')

    neo4j = AsyncGraphDatabase.driver(
        CONFIG.neo4j_uri,
        auth=(CONFIG.neo4j_username, CONFIG.neo4j_password),
        connection_timeout=args.timeout,
        connection_acquisition_timeout=args.timeout,
    )
    try:
        await retry(
            'Neo4j connection', neo4j.verify_connectivity,
            timeout=args.timeout, attempts=args.retries,
        )
        await ensure_indexes(neo4j, timeout=args.timeout, attempts=args.retries)
        await load_annotations(
            neo4j,
            path,
            source_fallback=source_fallback,
            target_fallback=target_fallback,
            batch_size=args.batch_size,
            timeout=args.timeout,
            attempts=args.retries,
        )
    finally:
        await neo4j.close()
    print('Annotation import complete.')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Environment variables:
  BTS_NEO4J_URI, BTS_NEO4J_DB_NAME, BTS_NEO4J_USERNAME, BTS_NEO4J_PASSWORD
  BTS_IMPORT_BATCH_SIZE, BTS_IMPORT_TIMEOUT_SECONDS, BTS_IMPORT_RETRIES

Prefix columns and CURIEs in the file take precedence. --source-prefix and
--target-prefix are fallbacks for legacy rows where prefix information is absent.
""",
    )
    parser.add_argument('annotation_dump', type=Path, help='Path to an .annotation.dump file')
    parser.add_argument('--source-prefix', help='Fallback source prefix when absent from rows')
    parser.add_argument('--target-prefix', help='Fallback target prefix when absent from rows')
    parser.add_argument(
        '--batch-size', type=int,
        default=_env_int('BTS_IMPORT_BATCH_SIZE', DEFAULT_BATCH_SIZE),
        help=f'Rows per request (env BTS_IMPORT_BATCH_SIZE; default {DEFAULT_BATCH_SIZE})',
    )
    parser.add_argument(
        '--timeout', type=float,
        default=_env_float('BTS_IMPORT_TIMEOUT_SECONDS', DEFAULT_TIMEOUT_SECONDS),
        help='Request timeout in seconds (env BTS_IMPORT_TIMEOUT_SECONDS; default 60)',
    )
    parser.add_argument(
        '--retries', type=int,
        default=_env_int('BTS_IMPORT_RETRIES', DEFAULT_RETRIES),
        help=f'Attempts per request (env BTS_IMPORT_RETRIES; default {DEFAULT_RETRIES})',
    )
    args = parser.parse_args()
    if args.batch_size < 1 or args.timeout <= 0 or args.retries < 1:
        parser.error('--batch-size and --retries must be >= 1; --timeout must be > 0')
    return args


if __name__ == '__main__':
    asyncio.run(run(parse_args()))
