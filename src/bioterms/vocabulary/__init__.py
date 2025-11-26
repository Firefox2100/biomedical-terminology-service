import importlib
import importlib.resources

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.vocabulary_status import VocabularyStatus


ALL_VOCABULARIES = {
    ConceptPrefix.HPO: 'hpo',
    ConceptPrefix.NCIT: 'ncit',
    ConceptPrefix.OMIM: 'omim',
    ConceptPrefix.ORDO: 'ordo',
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
        'name': vocabulary_module.VOCABULARY_NAME,
        'prefix': vocabulary_module.VOCABULARY_PREFIX,
        'annotations': vocabulary_module.ANNOTATIONS,
        'similarityMethods': vocabulary_module.SIMILARITY_METHODS,
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
        await vocabulary_module.delete_vocabulary_data(
            doc_db=doc_db,
            graph_db=graph_db,
        )

    # Create indexes before loading data
    await vocabulary_module.create_indexes(
        doc_db=doc_db,
        graph_db=graph_db,
    )
    await vocabulary_module.load_vocabulary_from_file(
        doc_db=doc_db,
        graph_db=graph_db,
    )


async def delete_vocabulary(prefix: ConceptPrefix,
                            doc_db: DocumentDatabase = None,
                            graph_db: GraphDatabase = None,
                            ):
    """
    Delete the vocabulary specified by the prefix from the databases.
    :param prefix: The prefix of the vocabulary to delete.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.delete_all_for_label(prefix)
    await graph_db.delete_vocabulary_graph(prefix)


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


async def get_vocabulary_status(prefix: ConceptPrefix,
                                doc_db: DocumentDatabase = None,
                                graph_db: GraphDatabase = None,
                                ) -> VocabularyStatus:
    """
    Get the status of the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :return: The vocabulary status.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if doc_db is None:
        doc_db = await get_active_doc_db()

    if graph_db is None:
        graph_db = get_active_graph_db()

    concept_count = await doc_db.count_terms(prefix)
    relationship_count = await graph_db.count_internal_relationships(prefix)
    annotations = vocabulary_module.ANNOTATIONS

    return VocabularyStatus(
        prefix=prefix,
        name=vocabulary_module.VOCABULARY_NAME,
        loaded=concept_count > 0,
        conceptCount=concept_count,
        relationshipCount=relationship_count,
        annotations=annotations,
        similarityMethods=vocabulary_module.SIMILARITY_METHODS,
    )
