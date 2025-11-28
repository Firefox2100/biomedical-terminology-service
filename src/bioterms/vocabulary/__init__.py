import importlib
import importlib.resources
import inspect
import aiofiles.os

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
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


async def delete_vocabulary(prefix: ConceptPrefix,
                            doc_db: DocumentDatabase = None,
                            graph_db: GraphDatabase = None,
                            ):
    """
    Delete the vocabulary data for the given prefix from the databases.
    :param prefix: The prefix of the vocabulary.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    delete_func = getattr(vocabulary_module, 'delete_vocabulary_data', None)
    if delete_func is None or not callable(delete_func):
        # Fallback to default deletion method
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        await doc_db.delete_all_for_label(vocabulary_module.VOCABULARY_PREFIX)
        await graph_db.delete_vocabulary_graph(prefix=vocabulary_module.VOCABULARY_PREFIX)
    else:
        result = delete_func(
            doc_db=doc_db,
            graph_db=graph_db,
        )
        if inspect.iscoroutine(result):
            await result


async def load_vocabulary(prefix: ConceptPrefix,
                          drop_existing: bool = True,
                          doc_db: DocumentDatabase = None,
                          graph_db: GraphDatabase = None,
                          ):
    """
    Load the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to load.
    :param drop_existing: Whether to drop existing data before loading.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if not check_files_exist(vocabulary_module.FILE_PATHS):
        raise ValueError(f'Vocabulary files for {prefix} not found. Are they downloaded?')

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
    )
    if inspect.iscoroutine(result):
        await result


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
