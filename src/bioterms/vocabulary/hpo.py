import os
import httpx
import networkx as nx
from owlready2 import get_ontology, ThingClass

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, iter_progress, \
    verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_NAME = 'Human Phenotype Ontology'
VOCABULARY_PREFIX = ConceptPrefix.HPO
ANNOTATIONS = [ConceptPrefix.ORDO, ConceptPrefix.HGNC_SYMBOL]
SIMILARITY_METHODS = [SimilarityMethod.RELEVANCE]
FILE_PATHS = ['hpo/hp.owl']
TIMESTAMP_FILE = 'hpo/.timestamp'
CONCEPT_CLASS = Concept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the HPO vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    owl_url = 'https://github.com/obophenotype/human-phenotype-ontology/releases/latest/download/hp.owl'

    await download_file(
        url=owl_url,
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


def _construct_hpo_concept(hpo_class: ThingClass) -> CONCEPT_CLASS:
    """
    Construct a Concept instance from an HPO class.
    :param hpo_class: The HPO class to convert.
    :return: A Concept instance.
    """
    concept = CONCEPT_CLASS(
        prefix=VOCABULARY_PREFIX,
        conceptTypes=[],
        conceptId=hpo_class.name.split('_')[-1],
        label=hpo_class.label[0]
        if hasattr(hpo_class, 'label') and hpo_class.label
        else None,
        definition=hpo_class.IAO_0000115[0]
        if hasattr(hpo_class, 'IAO_0000115') and hpo_class.IAO_0000115
        else None,
        comment=hpo_class.comment[0]
        if hasattr(hpo_class, 'comment') and hpo_class.comment
        else None,
        status=ConceptStatus.DEPRECATED
        if hasattr(hpo_class, 'deprecated') and bool(hpo_class.deprecated)
        else ConceptStatus.ACTIVE,
        synonyms=[],
    )

    return concept


def _process_hpo_class(hpo_class: ThingClass,
                       ) -> tuple[CONCEPT_CLASS, list[tuple[str, str, ConceptRelationshipType]]]:
    """
    Process an HPO class and extract the corresponding Concept and relationships.
    :param hpo_class: The HPO class to process.
    :return: A tuple containing the Concept and a list of relationships.
    """
    concept = _construct_hpo_concept(hpo_class)
    relationships: list[tuple[str, str, ConceptRelationshipType]] = []

    if hasattr(hpo_class, 'subclasses'):
        for child in hpo_class.subclasses():
            relationships.append((
                child.name.split('_')[-1],
                concept.concept_id,
                ConceptRelationshipType.IS_A
            ))

    if hasattr(hpo_class, 'hasAlternativeId'):
        for replaced_classes in hpo_class.hasAlternativeId:
            relationships.append((
                replaced_classes.split(':')[-1],
                concept.concept_id,
                ConceptRelationshipType.REPLACED_BY
            ))

    if hasattr(hpo_class, 'consider'):
        for replaced_classes in hpo_class.consider:
            relationships.append((
                replaced_classes.split(':')[-1],
                concept.concept_id,
                ConceptRelationshipType.REPLACED_BY
            ))

    return concept, relationships


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the HPO vocabulary from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('HPO owl file not found')

    full_ontology_path = os.path.join(CONFIG.data_dir, FILE_PATHS[0])
    verbose_print(f'Loading HPO ontology from {full_ontology_path}')

    owl_file_path = f'file://{full_ontology_path}'

    hpo_ontology = get_ontology(owl_file_path).load()
    hpo_classes = list(hpo_ontology.classes())
    verbose_print('HPO ontology read from file')

    hpo_graph = nx.DiGraph()
    concepts = []

    for hpo_class in iter_progress(hpo_classes, description='Processing HPO classes', total=len(hpo_classes)):
        if hpo_class.name.startswith('HP_'):
            concept, relationships = _process_hpo_class(hpo_class)
            concepts.append(concept)
            hpo_graph.add_node(concept.concept_id)

            for source_id, target_id, rel_type in relationships:
                hpo_graph.add_edge(
                    source_id,
                    target_id,
                    label=rel_type
                )

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    verbose_print('Saving HPO concepts and graph to databases')

    await doc_db.save_terms(
        terms=concepts
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=hpo_graph,
    )
