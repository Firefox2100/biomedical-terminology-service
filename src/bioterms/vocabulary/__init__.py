import importlib

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db


ALL_VOCABULARIES = {
    ConceptPrefix.HPO: 'hpo',
}


def get_vocabulary_module(prefix: ConceptPrefix):
    """
    Get the vocabulary module for the given prefix.
    :param prefix: The prefix of the vocabulary.
    :return: The vocabulary module.
    """
    vocabulary_module_name = ALL_VOCABULARIES.get(prefix)
    if not vocabulary_module_name:
        raise ValueError(f'Vocabulary with prefix {prefix} not found.')

    vocabulary_module = importlib.import_module(f'bioterms.vocabulary.{vocabulary_module_name}')

    return vocabulary_module


def get_vocabulary_config(prefix: ConceptPrefix) -> dict:
    """
    Get the vocabulary configuration for the given prefix.
    :param prefix: The prefix of the vocabulary.
    :return: The vocabulary configuration.
    """
    vocabulary_module = get_vocabulary_module(prefix)
    return {
        'prefix': vocabulary_module.VOCABULARY_PREFIX,
        'annotations': vocabulary_module.ANNOTATIONS,
        'filePaths': vocabulary_module.FILE_PATHS,
        'conceptClass': vocabulary_module.CONCEPT_CLASS,
    }


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
        vocabulary_module.delete_vocabulary_files()

    await vocabulary_module.download_vocabulary()


async def load_vocabulary(prefix: ConceptPrefix,
                          drop_existing: bool = True
                          ):
    """
    Load the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to load.
    :param drop_existing: Whether to drop existing data before loading.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if not check_files_exist(vocabulary_module.FILE_PATHS):
        raise ValueError(f'Vocabulary files for {prefix} not found. Are they downloaded?')

    if drop_existing:
        # Drop existing data before loading
        await vocabulary_module.delete_vocabulary_data()

    # Create indexes before loading data
    await vocabulary_module.create_indexes()
    await vocabulary_module.load_vocabulary_from_file()


async def delete_vocabulary(prefix: ConceptPrefix,
                            doc_db: DocumentDatabase | None = None,
                            graph_db: GraphDatabase | None = None,
                            ):
    """
    Delete the vocabulary specified by the prefix from the databases.
    :param prefix: The prefix of the vocabulary to delete.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.delete_all_for_label(prefix)
    await graph_db.delete_vocabulary_graph(prefix)
