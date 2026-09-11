import csv
import os
import importlib
import importlib.resources
import inspect
from datetime import datetime, timezone
import aiofiles
import aiofiles.os
import networkx as nx
import numpy as np

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import check_files_exist
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, VectorDatabase, get_active_cache, \
    get_active_doc_db, get_active_graph_db, get_active_vector_db
from bioterms.model.concept import Concept
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
                          cache: Cache = None,
                          doc_db: DocumentDatabase = None,
                          graph_db: GraphDatabase = None,
                          ):
    """
    Load the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to load.
    :param drop_existing: Whether to drop existing data before loading.
    :param offline: Whether to operate in offline mode (write to data files instead of database).
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
    :param prefix: The prefix of the vocabulary to embed.
    :param config: The vocabulary configuration dictionary.
    """
    from bioterms.embedding import ConceptTransformer, TextTransformer, EmbeddingContainerV2, \
        EmbeddingContainerFileV2

    offline_concept_path = os.path.join(CONFIG.data_dir, 'offline', f'{prefix.value}.doc.dump')
    if not os.path.exists(offline_concept_path):
        raise ValueError(f'Offline concept file for {prefix} not found at {offline_concept_path}.')
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
                             ) -> list[Concept]:
    """
    Stream the offline document dump (JSON lines, produced by `write_concepts_to_file`) into
    the document database in `batch_size`-sized chunks via `doc_db.save_terms`, so this
    automatically goes through whichever concrete document database driver is configured --
    including the native-vs-fallback auto-complete search indexing chosen per backend.

    Every parsed concept is also collected and returned, since `GraphDatabase.save_vocabulary_graph`
    reads node properties from the concepts list rather than from a separate node dump file.
    :param prefix: The vocabulary prefix being restored.
    :param doc_path: Path to the `<prefix>.doc.dump` file.
    :param concept_class: The vocabulary's Concept subclass, for typed deserialisation.
    :param doc_db: The document database instance to write to.
    :param batch_size: Number of concepts written per `save_terms` call.
    :param no_upsert: Passed through to `save_terms` -- True is faster but requires the
        destination to already be free of this vocabulary's data (see `overwrite`).
    :return: Every concept parsed from the dump file.
    """
    concepts: list[Concept] = []
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
            concepts.append(concept)
            batch.append(concept)
            if len(batch) >= batch_size:
                await doc_db.save_terms(batch, no_upsert=no_upsert)
                batch = []

    if batch:
        await doc_db.save_terms(batch, no_upsert=no_upsert)

    return concepts


def _read_offline_graph(graph_path: str) -> nx.MultiDiGraph:
    """
    Rebuild the vocabulary's internal relationship graph from its offline `<prefix>.graph.dump`
    file (CSV rows of `source_id,target_id,relationship_type,relationship_key`, written by
    `write_graph_to_file`/`edge_iter`), for use with `GraphDatabase.save_vocabulary_graph`.
    Node properties are not part of this file -- see `_restore_documents`.
    :param graph_path: Path to the `<prefix>.graph.dump` file.
    :return: The reconstructed graph.
    """
    graph = nx.MultiDiGraph()

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
            graph.add_edge(source, target, key=rel_key, label=label)

    return graph


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
    graph_path = os.path.join(offline_dir, f'{prefix.value}.graph.dump')
    embed_path = os.path.join(offline_dir, f'{prefix.value}.embed.dump')

    missing = [path for path in (doc_path, graph_path) if not os.path.isfile(path)]
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

    concepts = await _restore_documents(
        prefix=prefix,
        doc_path=doc_path,
        concept_class=config['conceptClass'],
        doc_db=doc_db,
        batch_size=batch_size,
        no_upsert=overwrite,
    )
    concept_count = len(concepts)

    graph = _read_offline_graph(graph_path)
    edge_count = graph.number_of_edges()
    await graph_db.save_vocabulary_graph(concepts, graph, consume_concepts=True)

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
