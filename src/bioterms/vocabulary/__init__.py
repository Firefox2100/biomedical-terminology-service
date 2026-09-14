import ast
import csv
import os
import importlib
import importlib.resources
import inspect
from datetime import datetime, timezone
from typing import Iterator, Optional
import aiofiles
import aiofiles.os
import numpy as np

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import check_files_exist, verbose_print
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, VectorDatabase, get_active_cache, \
    get_active_doc_db, get_active_graph_db, get_active_vector_db
from bioterms.model.concept import Concept, GRAPH_NODE_EXTRA_PROPERTIES
from .utils import ALL_VOCABULARIES, get_vocabulary_module, get_vocabulary_status


def get_vocabulary_config(prefix: ConceptPrefix) -> dict:
    """
    Get the vocabulary configuration for the given prefix.
    :param prefix: The prefix of the vocabulary.
    :return: The vocabulary configuration.
    """
    vocabulary_module = get_vocabulary_module(prefix)
    return {
        'name': vocabulary_module.VOCABULARY_NAME,
        'prefix': vocabulary_module.VOCABULARY_PREFIX,
        'annotations': vocabulary_module.ANNOTATIONS,
        'similarityMethods': vocabulary_module.SIMILARITY_METHODS,
        'filePaths': vocabulary_module.FILE_PATHS,
        'conceptClass': vocabulary_module.CONCEPT_CLASS,
    }


async def delete_vocabulary_files(prefix: ConceptPrefix):
    """
    Delete the vocabulary files for the given prefix.
    :param prefix: The prefix of the vocabulary.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    deletion_func = getattr(vocabulary_module, 'delete_vocabulary_files', None)

    if deletion_func is None or not callable(deletion_func):
        # Fallback to default deletion method
        for file_path in vocabulary_module.FILE_PATHS:
            try:
                await aiofiles.os.remove(file_path)
            except Exception:
                pass

        timestamp_file_path = os.path.join(CONFIG.data_dir, vocabulary_module.TIMESTAMP_FILE)
        try:
            await aiofiles.os.remove(timestamp_file_path)
        except Exception:
            pass
    else:
        result = deletion_func()
        if inspect.iscoroutine(result):
            await result


async def download_vocabulary(prefix: ConceptPrefix,
                              redownload: bool = False
                              ):
    """
    Download the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to download.
    :param redownload: Whether to redownload the files even if they exist.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if redownload:
        await delete_vocabulary_files(prefix)

    download_func = getattr(vocabulary_module, 'download_vocabulary', None)
    if download_func is None or not callable(download_func):
        raise ValueError(f'Vocabulary module for {prefix} does not have a download_vocabulary function.')

    result = download_func()
    if inspect.iscoroutine(result):
        await result

    timestamp_file_path = os.path.join(CONFIG.data_dir, vocabulary_module.TIMESTAMP_FILE)

    async with aiofiles.open(timestamp_file_path, 'w') as timestamp_file:
        current_time = datetime.now(timezone.utc).isoformat()
        await timestamp_file.write(current_time)


async def create_indexes(prefix: ConceptPrefix,
                         overwrite: bool = False,
                         doc_db: DocumentDatabase = None,
                         graph_db: GraphDatabase = None,
                         ):
    """
    Create indexes for the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary.
    :param overwrite: Whether to overwrite existing indexes.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    """
    vocabulary_module = get_vocabulary_module(prefix)
    cache = get_active_cache()

    create_index_func = getattr(vocabulary_module, 'create_indexes', None)
    if create_index_func is None or not callable(create_index_func):
        # Fallback to default index creation method
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        await doc_db.create_index(
            prefix=vocabulary_module.VOCABULARY_PREFIX,
            field='conceptId',
            unique=True,
            overwrite=overwrite,
        )
        await doc_db.create_index(
            prefix=vocabulary_module.VOCABULARY_PREFIX,
            field='label',
            overwrite=overwrite,
        )

        await graph_db.create_index()
    else:
        result = create_index_func(
            overwrite=overwrite,
            doc_db=doc_db,
            graph_db=graph_db,
        )
        if inspect.iscoroutine(result):
            await result

    await cache.rotate_dataset_version()


async def delete_vocabulary(prefix: ConceptPrefix,
                            cache: Cache = None,
                            doc_db: DocumentDatabase = None,
                            graph_db: GraphDatabase = None,
                            vector_db: VectorDatabase = None,
                            ):
    """
    Delete the vocabulary data for the given prefix from the databases.
    :param prefix: The prefix of the vocabulary.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param vector_db: The vector database instance.
    """
    vocabulary_module = get_vocabulary_module(prefix)
    cache = cache or get_active_cache()

    delete_func = getattr(vocabulary_module, 'delete_vocabulary_data', None)
    if delete_func is None or not callable(delete_func):
        # Fallback to default deletion method
        if cache is None:
            cache = get_active_cache()
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()
        if vector_db is None:
            vector_db = get_active_vector_db()

        await cache.purge()
        await doc_db.delete_all_for_label(vocabulary_module.VOCABULARY_PREFIX)
        await graph_db.delete_vocabulary_graph(prefix=vocabulary_module.VOCABULARY_PREFIX)
        await vector_db.delete_vectors_for_prefix(prefix=vocabulary_module.VOCABULARY_PREFIX)
    else:
        result = delete_func(
            doc_db=doc_db,
            graph_db=graph_db,
        )
        if inspect.iscoroutine(result):
            await result

    await cache.rotate_dataset_version()


async def load_vocabulary(prefix: ConceptPrefix,
                          drop_existing: bool = True,
                          offline: bool = False,
                          build_search_index: bool = True,
                          cache: Cache = None,
                          doc_db: DocumentDatabase = None,
                          graph_db: GraphDatabase = None,
                          ):
    """
    Load the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to load.
    :param drop_existing: Whether to drop existing data before loading.
    :param offline: Whether to operate in offline mode (write to data files instead of database).
    :param build_search_index: In offline mode, whether to precompute and write each concept's
        fallback-search `nGrams`/`searchText` fields into the `.doc.dump` (see
        `write_concepts_to_file`). Ignored when `offline` is False. Pass False when the
        eventual restore target doesn't need them (any SQL backend) to skip this work.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if not check_files_exist(vocabulary_module.FILE_PATHS):
        raise ValueError(f'Vocabulary files for {prefix} not found. Are they downloaded?')

    if not offline:
        if drop_existing:
            # Drop existing data before loading
            await delete_vocabulary(
                prefix=prefix,
                doc_db=doc_db,
                graph_db=graph_db,
            )

        # Create indexes before loading data
        await create_indexes(
            prefix=prefix,
            doc_db=doc_db,
            graph_db=graph_db,
        )

    load_func = getattr(vocabulary_module, 'load_vocabulary_from_file', None)
    if load_func is None or not callable(load_func):
        raise ValueError(f'Vocabulary module for {prefix} does not have a load_vocabulary_from_file function.')

    result = load_func(
        doc_db=doc_db,
        graph_db=graph_db,
        offline=offline,
        build_search_index=build_search_index,
    )
    if inspect.iscoroutine(result):
        await result

    if not offline:
        # Cache is only required when mutating online databases.
        if cache is None:
            cache = get_active_cache()

        await cache.purge()
        await cache.rotate_dataset_version()


async def _embed_vocabulary_online(prefix: ConceptPrefix,
                                   config: dict,
                                   drop_existing: bool,
                                   doc_db: DocumentDatabase = None,
                                   graph_db: GraphDatabase = None,
                                   vector_db: VectorDatabase = None,
                                   ):
    """
    Embed a vocabulary's concepts directly into the vector database.
    :param prefix: The prefix of the vocabulary to embed.
    :param config: The vocabulary configuration dictionary.
    :param drop_existing: Whether to drop existing embeddings before embedding.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param vector_db: The vector database instance.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if vector_db is None:
        vector_db = get_active_vector_db()

    status = await get_vocabulary_status(
        prefix=prefix,
        doc_db=doc_db,
        graph_db=graph_db,
    )

    if not status.loaded:
        raise RuntimeError(f'Vocabulary {prefix} is not loaded. Cannot embed.')

    if drop_existing:
        await vector_db.delete_vectors_for_prefix(prefix=prefix)

    concept_iter = doc_db.get_terms_iter(
        prefix=prefix,
        model_class=config['conceptClass'],
    )

    await vector_db.insert_concepts(
        concepts=concept_iter,
        prefix=prefix,
        total_concepts=status.concept_count,
    )


async def _embed_vocabulary_offline(prefix: ConceptPrefix,
                                    config: dict,
                                    ):
    """
    Embed a vocabulary's concepts from an offline concept dump into an offline embedding dump.
    If the offline concept dump does not exist yet, it is produced first by reading the
    vocabulary's concepts from the configured document database and writing them out in the
    same `.doc.dump` format `write_concepts_to_file` uses elsewhere. This lets a vocabulary
    that was loaded straight into the database (never dumped offline) still be embedded
    offline -- against a local file, immune to the live database's load/latency/disk pressure
    -- and the dump this produces is reusable for a later `restore_vocabulary_embeddings` call
    without touching the database again.
    :param prefix: The prefix of the vocabulary to embed.
    :param config: The vocabulary configuration dictionary.
    """
    from bioterms.embedding import ConceptTransformer, TextTransformer, EmbeddingContainerV2, \
        EmbeddingContainerFileV2
    from .utils import write_concepts_to_file

    offline_concept_path = os.path.join(CONFIG.data_dir, 'offline', f'{prefix.value}.doc.dump')
    if not os.path.exists(offline_concept_path):
        doc_db = await get_active_doc_db()
        status = await get_vocabulary_status(prefix=prefix, doc_db=doc_db)
        if not status.loaded:
            raise ValueError(
                f'Offline concept file for {prefix} not found at {offline_concept_path}, and '
                f'{prefix} is not loaded in the configured document database either -- nothing '
                f'to embed from.'
            )

        verbose_print(
            f'Offline concept dump for {prefix.value} not found; reading its {status.concept_count} '
            f'concepts from the configured document database instead, and writing them to '
            f'{offline_concept_path} for reuse.'
        )
        concepts = [c async for c in doc_db.get_terms_iter(prefix=prefix, model_class=config['conceptClass'])]
        # Embedding only ever reads label/synonyms/definition off each concept -- the
        # fallback-search nGrams/searchText fields this dump could otherwise carry are
        # irrelevant here regardless of the eventual restore target.
        await write_concepts_to_file(prefix=prefix, concepts=concepts, build_search_index=False)
        del concepts
        verbose_print(f'Offline concept dump for {prefix.value} written.')

    offline_embedding_path = os.path.join(CONFIG.data_dir, 'offline', f'{prefix.value}.embed.dump')

    async def concept_iter():
        async with aiofiles.open(offline_concept_path) as f:
            async for line in f:
                yield config['conceptClass'].model_validate_json(line.strip())

    transformer = ConceptTransformer()
    async def embed_iter():
        async for batch in transformer.embed_concepts(
            concepts=concept_iter(),
        ):
            for item, vector in batch:
                # Convert the vector to np array
                vector = np.array(vector, dtype=np.float32)

                yield EmbeddingContainerV2(
                    item_id=item.item_id,
                    concept_id=item.concept_id,
                    kind=item.kind,
                    text=item.text,
                    vector=vector,
                )

    embedding_file = EmbeddingContainerFileV2(offline_embedding_path, dim=TextTransformer().dimension)
    await embedding_file.write(embed_iter())


async def embed_vocabulary(prefix: ConceptPrefix,
                           drop_existing: bool = True,
                           offline: bool = False,
                           cache: Cache = None,
                           doc_db: DocumentDatabase = None,
                           graph_db: GraphDatabase = None,
                           vector_db: VectorDatabase = None,
                           ):
    """
    Embed the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to embed.
    :param drop_existing: Whether to drop existing embeddings before embedding.
    :param offline: Whether to operate in offline mode (skip writing to vector database).
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param vector_db: The vector database instance.
    """
    config = get_vocabulary_config(prefix)

    if not offline:
        await _embed_vocabulary_online(prefix, config, drop_existing, doc_db, graph_db, vector_db)
    else:
        await _embed_vocabulary_offline(prefix, config)

    if not offline:
        # Offline embedding writes files only, so cache invalidation is unnecessary.
        if cache is None:
            cache = get_active_cache()

        await cache.rotate_dataset_version()


async def restore_vocabulary_embeddings(prefix: ConceptPrefix,
                                        drop_existing: bool = True,
                                        offline_dir: str | os.PathLike | None = None,
                                        doc_db: DocumentDatabase = None,
                                        graph_db: GraphDatabase = None,
                                        vector_db: VectorDatabase = None,
                                        ):
    """
    Restore precomputed embeddings for the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to restore.
    :param drop_existing: Whether to drop existing embeddings before restoring.
    :param offline_dir: Directory containing the offline dump files (default: BTS_DATA_DIR/offline).
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param vector_db: The vector database instance.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if vector_db is None:
        vector_db = get_active_vector_db()

    cache = get_active_cache()

    status = await get_vocabulary_status(
        prefix=prefix,
        doc_db=doc_db,
        graph_db=graph_db,
    )
    if not status.loaded:
        raise RuntimeError(f'Vocabulary {prefix} is not loaded. Cannot restore embeddings.')

    if drop_existing:
        await vector_db.delete_vectors_for_prefix(prefix=prefix)

    from bioterms.embedding import EmbeddingContainerFileV2
    from bioterms.database.vector_db import EmbeddingItemVector

    offline_dir = str(offline_dir) if offline_dir is not None else os.path.join(CONFIG.data_dir, 'offline')
    offline_embedding_path = os.path.join(offline_dir, f'{prefix.value}.embed.dump')
    # dim is a placeholder here -- reading a file overwrites it with the value from the file
    # header, which is what `EmbeddingContainerFileV2.read()` actually uses.
    embedding_file = EmbeddingContainerFileV2(offline_embedding_path, dim=1)

    async def item_iter():
        async for container in embedding_file.read():
            yield EmbeddingItemVector(
                item_id=container.item_id,
                concept_id=container.concept_id,
                kind=container.kind,
                text=container.text,
                vector=container.vector.tolist(),
            )

    await vector_db.load_embedding_items(
        prefix=prefix,
        items=item_iter(),
    )

    await cache.rotate_dataset_version()


async def _restore_documents(prefix: ConceptPrefix,
                             doc_path: str,
                             concept_class: type[Concept],
                             doc_db: DocumentDatabase,
                             batch_size: int,
                             no_upsert: bool,
                             ) -> int:
    """
    Stream the offline document dump (JSON lines, produced by `write_concepts_to_file`) into
    the document database in `batch_size`-sized chunks via `doc_db.save_terms`, so this
    automatically goes through whichever concrete document database driver is configured --
    including the native-vs-fallback auto-complete search indexing chosen per backend.

    Concepts are written batch-by-batch and never accumulated beyond that -- only a running
    count is kept, so this stays memory-bounded regardless of how large the dump file is.
    Graph node properties come from `_iter_offline_node_ids` instead (see there), not from
    this function's output, so a masked/partial restore with an empty `.doc.dump` still
    restores the graph correctly.
    :param prefix: The vocabulary prefix being restored.
    :param doc_path: Path to the `<prefix>.doc.dump` file.
    :param concept_class: The vocabulary's Concept subclass, for typed deserialisation.
    :param doc_db: The document database instance to write to.
    :param batch_size: Number of concepts written per `save_terms` call.
    :param no_upsert: Passed through to `save_terms` -- True is faster but requires the
        destination to already be free of this vocabulary's data (see `overwrite`).
    :return: The number of concepts parsed and written from the dump file.
    """
    concept_count = 0
    batch: list[Concept] = []

    async with aiofiles.open(doc_path, encoding='utf-8') as f:
        async for line in f:
            line = line.strip()
            if not line:
                continue
            concept = concept_class.model_validate_json(line)
            if concept.prefix != prefix:
                raise ValueError(
                    f'{doc_path} contains a document for prefix {concept.prefix!r}, expected {prefix!r}'
                )
            concept_count += 1
            batch.append(concept)
            if len(batch) >= batch_size:
                await doc_db.save_terms(batch, no_upsert=no_upsert)
                batch = []

    if batch:
        await doc_db.save_terms(batch, no_upsert=no_upsert)

    return concept_count


def _iter_offline_graph_edges(graph_path: str,
                              ) -> Iterator[tuple[str, str, Optional[str], Optional[str]]]:
    """
    Stream the vocabulary's internal relationships from its offline `<prefix>.graph.dump` file
    (CSV rows of `source_id,target_id,relationship_type,relationship_key`, written by
    `write_graph_to_file`/`edge_iter`) as `(source_id, target_id, relationship_type,
    relationship_key)` tuples, one CSV row at a time, for use with
    `GraphDatabase.save_vocabulary_graph` -- without ever materialising the full edge set as an
    in-memory `nx.Graph`, which for a large vocabulary's edge dump can be sizeable.
    Node properties are not part of this file -- see `_iter_offline_node_ids`.
    :param graph_path: Path to the `<prefix>.graph.dump` file.
    :return: A generator of edge tuples, in the same shape `bioterms.etc.utils.edge_iter` yields.
    """
    with open(graph_path, encoding='utf-8', newline='') as f:
        for row in csv.reader(f):
            if not row or not any(value.strip() for value in row):
                continue
            if len(row) < 2:
                raise ValueError(f'Malformed edge row in {graph_path}: {row!r}')
            source, target = row[0], row[1]
            rel_type = row[2] if len(row) > 2 else ''
            rel_key = row[3] if len(row) > 3 and row[3] else None
            label = ConceptRelationshipType(rel_type) if rel_type else None
            yield source, target, label.value if label else None, rel_key


def _iter_offline_node_ids(node_id_path: str,
                           prefix: ConceptPrefix,
                           concept_class: type[Concept],
                           ) -> Iterator[Concept]:
    """
    Stream the vocabulary's graph node list from its offline `<prefix>.node_ids.dump` file
    (CSV rows of `concept_id,concept_types,*extra_properties`, written by
    `write_graph_to_file`), one CSV row at a time, for use with
    `GraphDatabase.save_vocabulary_graph`.

    This is read independently of `<prefix>.doc.dump`: `save_vocabulary_graph` needs a
    non-empty concepts iterable to know which prefix/properties to write graph nodes under, and
    an empty `.doc.dump` (e.g. a masked/partial restore that only needs to rebuild the graph
    half) must not silently drop the graph nodes (as the PostgreSQL driver does when handed an
    empty iterable) or mis-prefix them (as the Neo4j driver does by falling back to an
    empty-string prefix). Node dump rows carry no `prefix` column of their own -- it is
    supplied by the caller, since a single dump file only ever covers one vocabulary.
    :param node_id_path: Path to the `<prefix>.node_ids.dump` file.
    :param prefix: The vocabulary prefix being restored.
    :param concept_class: The vocabulary's Concept subclass, for typed deserialisation of the
        extra properties columns.
    :return: A generator of one concept instance per graph node, carrying only the fields the
        graph database stores (id, types, extra properties).
    """
    with open(node_id_path, encoding='utf-8', newline='') as f:
        for row in csv.reader(f):
            if not row or not any(value.strip() for value in row):
                continue
            if len(row) < 2:
                raise ValueError(f'Malformed node row in {node_id_path}: {row!r}')

            concept_id, types_repr = row[0], row[1]
            concept_types = ast.literal_eval(types_repr) if types_repr else []

            payload = {'prefix': prefix, 'conceptId': concept_id, 'conceptTypes': concept_types}
            for i, prop in enumerate(GRAPH_NODE_EXTRA_PROPERTIES):
                if len(row) > 2 + i and row[2 + i] != '':
                    payload[prop] = row[2 + i]

            yield concept_class.model_validate(payload)


async def restore_vocabulary(prefix: ConceptPrefix,
                             overwrite: bool = False,
                             batch_size: int = 5000,
                             offline_dir: str | os.PathLike | None = None,
                             restore_embeddings: bool = True,
                             cache: Cache = None,
                             doc_db: DocumentDatabase = None,
                             graph_db: GraphDatabase = None,
                             vector_db: VectorDatabase = None,
                             ) -> dict:
    """
    Restore a vocabulary from offline dump files (produced by `load_vocabulary(prefix,
    offline=True)`) into the live databases.

    Unlike the standalone `scripts/load_offline_vocabulary.py` script this replaces, restoring
    goes through the same `DocumentDatabase`/`GraphDatabase`/`VectorDatabase` interfaces as a
    normal (non-offline) `load_vocabulary` call, so it automatically adapts to whichever
    concrete drivers are configured (MongoDB or SQL for documents, Neo4j or PostgreSQL for the
    graph, Qdrant/MongoDB/PostgreSQL for vectors) instead of assuming MongoDB+Neo4j+Qdrant --
    including the native-vs-fallback auto-complete search indexing chosen per document database
    backend (see `DocumentDatabase.create_index`), which this picks up for free by reusing
    `create_indexes`/`save_terms` rather than hand-rolling an "nGrams" index.

    Similarity dumps are restored separately, via `similarity.restore_similarity` -- similarity
    is not part of a vocabulary's core data (it may not exist yet, may be recomputed with a
    different method later, and is keyed by target vocabulary rather than owned by it the way
    documents/graph edges are), so it gets its own CLI command rather than a flag here.

    Restoring proceeds in three independent steps -- documents (`.doc.dump`), then graph nodes
    (`.node_ids.dump`), then graph edges (`.graph.dump`) -- deliberately not short-circuited by
    one another, so a masked/partial restore where one dump is empty (e.g. rebuilding only the
    graph half, with an empty `.doc.dump`) still restores whichever dumps do have content
    instead of silently dropping or mis-prefixing the graph nodes.
    :param prefix: The prefix of the vocabulary to restore.
    :param overwrite: Whether to drop any existing data for this vocabulary before restoring.
        When False (default), documents/graph edges/embeddings are safely upserted into
        whatever is already there instead -- e.g. to resume a partially completed restore.
    :param batch_size: Number of concepts written to the database per `save_terms` request
        during this restore. Graph edge writes are batched by `save_vocabulary_graph` itself
        instead, matching the batch size a normal (non-offline) load would use.
    :param offline_dir: Directory containing the offline dump files (default: BTS_DATA_DIR/offline).
    :param restore_embeddings: Whether to also restore the `<prefix>.embed.dump` file, if present.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param vector_db: The vector database instance.
    :return: A summary dict with `conceptCount`, `edgeCount`, and `embeddingsRestored`.
    """
    config = get_vocabulary_config(prefix)
    offline_dir = str(offline_dir) if offline_dir is not None else os.path.join(CONFIG.data_dir, 'offline')

    doc_path = os.path.join(offline_dir, f'{prefix.value}.doc.dump')
    node_id_path = os.path.join(offline_dir, f'{prefix.value}.node_ids.dump')
    graph_path = os.path.join(offline_dir, f'{prefix.value}.graph.dump')
    embed_path = os.path.join(offline_dir, f'{prefix.value}.embed.dump')

    missing = [path for path in (doc_path, node_id_path, graph_path) if not os.path.isfile(path)]
    if missing:
        raise ValueError(f'Missing required offline dump file(s): {", ".join(missing)}')

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    if overwrite:
        await delete_vocabulary(prefix=prefix, doc_db=doc_db, graph_db=graph_db)

    # Ensures the native-vs-fallback auto-complete search index (and the other standard
    # indexes) exist before documents are written, same as a normal load_vocabulary() call.
    await create_indexes(prefix=prefix, doc_db=doc_db, graph_db=graph_db)

    concept_count = await _restore_documents(
        prefix=prefix,
        doc_path=doc_path,
        concept_class=config['conceptClass'],
        doc_db=doc_db,
        batch_size=batch_size,
        no_upsert=overwrite,
    )

    # Streamed independently of the documents above: graph nodes are sourced from
    # `.node_ids.dump` rather than the just-restored documents, so an empty `.doc.dump` (a
    # masked restore that only needs to rebuild the graph) doesn't hand `save_vocabulary_graph`
    # an empty concepts iterable -- which the PostgreSQL driver silently no-ops on, and the
    # Neo4j driver falls back to writing under a blank prefix for. Both `nodes` and the edges
    # passed below are generators reading their dump file row-by-row, so `save_vocabulary_graph`
    # only ever holds one batch in memory instead of the whole vocabulary's graph.
    nodes = _iter_offline_node_ids(
        node_id_path=node_id_path,
        prefix=prefix,
        concept_class=config['conceptClass'],
    )

    edge_count = 0

    def _counted_edges() -> Iterator[tuple[str, str, Optional[str], Optional[str]]]:
        nonlocal edge_count
        for edge in _iter_offline_graph_edges(graph_path):
            edge_count += 1
            yield edge

    await graph_db.save_vocabulary_graph(nodes, _counted_edges(), consume_concepts=True)

    embeddings_restored = False
    if restore_embeddings and os.path.isfile(embed_path):
        if vector_db is None:
            vector_db = get_active_vector_db()
        await restore_vocabulary_embeddings(
            prefix=prefix,
            drop_existing=overwrite,
            offline_dir=offline_dir,
            doc_db=doc_db,
            graph_db=graph_db,
            vector_db=vector_db,
        )
        embeddings_restored = True

    if cache is None:
        cache = get_active_cache()

    await cache.purge()
    await cache.rotate_dataset_version()

    return {
        'conceptCount': concept_count,
        'edgeCount': edge_count,
        'embeddingsRestored': embeddings_restored,
    }


def get_vocabulary_license(prefix: ConceptPrefix) -> str | None:
    """
    Get the licence information for the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary.
    :return: The licence information as a string, or None if not available.
    """
    file_name = ALL_VOCABULARIES.get(prefix)
    if not file_name:
        raise ValueError(f'Vocabulary with prefix {prefix} not found.')

    file_name += '.md'

    try:
        file_path = importlib.resources.files('bioterms.data.licenses') / file_name
        with importlib.resources.as_file(file_path) as license_file:
            return license_file.read_text()
    except FileNotFoundError:
        return None
