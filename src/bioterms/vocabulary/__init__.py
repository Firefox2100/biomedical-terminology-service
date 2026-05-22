import os
import importlib
import importlib.resources
import inspect
from datetime import datetime, timezone
import aiofiles
import aiofiles.os
import numpy as np

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, VectorDatabase, get_active_cache, \
    get_active_doc_db, get_active_graph_db, get_active_vector_db
from bioterms.embedding import ConceptTransformer, EmbeddingContainerV1, EmbeddingContainerFileV1
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
        'conceptTypes': vocabulary_module.CONCEPT_TYPES,
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
    cache = cache or get_active_cache()

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
        # Purge cache after loading
        if cache is None:
            cache = get_active_cache()
        await cache.purge()

    await cache.rotate_dataset_version()


async def embed_vocabulary(prefix: ConceptPrefix,
                           drop_existing: bool = True,
                           offline: bool = False,
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
    cache = get_active_cache()

    if not offline:
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

        id_map = await vector_db.insert_concepts(
            concepts=concept_iter,
            prefix=prefix,
            total_concepts=status.concept_count,
        )

        await doc_db.update_vector_mapping(
            prefix=prefix,
            mapping=id_map,
        )
    else:
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
                for concept_id, vector in batch:
                    # Convert the vector to np array
                    vector = np.array(vector, dtype=np.float32)

                    yield EmbeddingContainerV1(
                        concept_id=concept_id,
                        vector=vector,
                    )

        embedding_file = EmbeddingContainerFileV1(offline_embedding_path)
        await embedding_file.write(embed_iter())

    await cache.rotate_dataset_version()


async def restore_vocabulary_embeddings(prefix: ConceptPrefix,
                                        drop_existing: bool = True,
                                        doc_db: DocumentDatabase = None,
                                        graph_db: GraphDatabase = None,
                                        vector_db: VectorDatabase = None,
                                        ):
    """
    Restore precomputed embeddings for the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to restore.
    :param drop_existing: Whether to drop existing embeddings before restoring.
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

    offline_embedding_path = os.path.join(CONFIG.data_dir, 'offline', f'{prefix.value}.embed.dump')
    embedding_file = EmbeddingContainerFileV1(offline_embedding_path)

    async def embed_iter():
        async for container in embedding_file.read():
            yield container.concept_id, str(container.vector_id), container.vector.tolist()

    id_map = await vector_db.load_embeddings(
        prefix=prefix,
        embeddings=embed_iter(),
    )

    await doc_db.update_vector_mapping(
        prefix=prefix,
        mapping=id_map,
    )

    await cache.rotate_dataset_version()


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
