from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import VocabularyNotLoaded, FilesNotFound
from bioterms.etc.utils import check_files_exist, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db


async def assert_vocabulary_loaded(prefix_1: ConceptPrefix,
                                   prefix_2: ConceptPrefix,
                                   graph_db: GraphDatabase = None):
    """
    Check if the annotation vocabulary graph is loaded in the primary graph database.
    :param prefix_1: The first vocabulary prefix to check.
    :param prefix_2: The second vocabulary prefix to check.
    :param graph_db: Optional GraphDatabase instance to use.
    :raises VocabularyNotLoaded: If any of the vocabularies is not loaded.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    prefix_1_count = await graph_db.count_terms(prefix_1)
    if prefix_1_count == 0:
        raise VocabularyNotLoaded(f'Vocabulary with prefix {prefix_1} is not loaded in the graph database.')

    prefix_2_count = await graph_db.count_terms(prefix_2)
    if prefix_2_count == 0:
        raise VocabularyNotLoaded(f'Vocabulary with prefix {prefix_2} is not loaded in the graph database.')


async def assert_pre_requisite(annotation_name: str,
                               prefix_1: ConceptPrefix,
                               prefix_2: ConceptPrefix,
                               file_paths: list[str],
                               graph_db: GraphDatabase = None,
                               ):
    verbose_print(f'Loading annotation: {annotation_name}...')

    await assert_vocabulary_loaded(
        prefix_1=prefix_1,
        prefix_2=prefix_2,
        graph_db=graph_db,
    )

    verbose_print(f'Confirmed {prefix_1} and {prefix_2} vocabularies are loaded.')

    annotation_count = await graph_db.count_annotations(
        prefix_1=prefix_1,
        prefix_2=prefix_2,
    )

    if annotation_count > 0:
        verbose_print(f'Annotation {annotation_name} already loaded with {annotation_count} entries, skipping.')
        return  # Annotations already exist, skip loading

    if not check_files_exist(file_paths):
        raise FilesNotFound(f'{annotation_name} file not found. Please download it first.')
