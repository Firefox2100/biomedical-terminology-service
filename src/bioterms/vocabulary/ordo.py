import os
import httpx
import networkx as nx
from owlready2 import get_ontology, ThingClass, PropertyClass, Restriction

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_NAME = 'Orphanet Rare Disease Ontology'
VOCABULARY_PREFIX = ConceptPrefix.ORDO
ANNOTATIONS = [ConceptPrefix.HPO]
SIMILARITY_METHODS = [SimilarityMethod.RELEVANCE]
FILE_PATHS = ['ordo/ordo_orphanet.owl']
CONCEPT_CLASS = Concept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the ORDO vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    owl_url = 'https://data.bioontology.org/ontologies/ORDO/download'

    if not CONFIG.bioportal_api_key:
        raise ValueError('BioPortal API key is required to download ORDO ontology.')

    await download_file(
        url=owl_url,
        file_path=FILE_PATHS[0],
        headers={'Authorization': f'apikey token={CONFIG.bioportal_api_key}'},
        download_client=download_client,
    )


def delete_vocabulary_files():
    """
    Delete the ORDO vocabulary files.
    """
    try:
        os.remove(os.path.join(CONFIG.data_dir, FILE_PATHS[0]))
    except Exception:
        pass


def _construct_ordo_concept(ordo_class: ThingClass) -> Concept:
    """
    Construct a Concept instance from an ORDO class.
    :param ordo_class: The ORDO class to convert.
    :return: A Concept instance.
    """
    concept = CONCEPT_CLASS(
        prefix=ConceptPrefix.ORDO,
        conceptTypes=[],
        conceptId=ordo_class.name.split('_')[-1],
        label=ordo_class.label[0]
        if hasattr(ordo_class, 'label') and ordo_class.label
        else None,
        definition=str(ordo_class.definition[0])
        if hasattr(ordo_class, 'definition') and ordo_class.definition
        else None,
        synonyms=[synonym for synonym in ordo_class.alternative_term]
        if hasattr(ordo_class, 'alternative_term') and ordo_class.alternative_term
        else None,
    )

    return concept


def _process_ordo_class(ordo_class: ThingClass,
                        part_of_prop: PropertyClass,
                        ) -> tuple[Concept, list[tuple[str, str, ConceptRelationshipType]]]:
    """
    Process an ORDO class into a Concept and its relationships.
    :param ordo_class: The ORDO class to process.
    :param part_of_prop: The 'part of' property from the ontology.
    :return: A tuple of Concept and list of relationships.
    """
    concept = _construct_ordo_concept(ordo_class)
    relationships: list[tuple[str, str, ConceptRelationshipType]] = []

    if hasattr(ordo_class, 'is_a'):
        for parent in ordo_class.is_a:
            if isinstance(parent, ThingClass):
                # Regular parent class
                if parent.name == 'Thing':
                    # Root node
                    pass
                elif parent.name.startswith('Orphanet_'):
                    relationships.append((
                        concept.concept_id,
                        parent.name.split('_')[-1],
                        ConceptRelationshipType.IS_A
                    ))

            elif isinstance(parent, Restriction):
                # Constrains model
                if parent.property.name == 'Orphanet_C056':
                    # moved_to, points to replacing term
                    replacing_term = parent.value
                    concept.status = ConceptStatus.DEPRECATED
                    if isinstance(replacing_term, ThingClass) and replacing_term.name.startswith('Orphanet_'):
                        relationships.append((
                            concept.concept_id,
                            replacing_term.name.split('_')[-1],
                            ConceptRelationshipType.REPLACED_BY
                        ))

    if part_of_prop[ordo_class]:
        # Has part_of property, link as is_a
        for parent in part_of_prop[ordo_class]:
            if isinstance(parent, ThingClass) and parent.name.startswith('Orphanet_'):
                relationships.append((
                    concept.concept_id,
                    parent.name.split('_')[-1],
                    ConceptRelationshipType.IS_A
                ))

    return concept, relationships

async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the ORDO vocabulary from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('ORDO owl file not found')

    owl_file_path = f'file://{os.path.join(CONFIG.data_dir, FILE_PATHS[0])}'

    ordo_ontology = get_ontology(owl_file_path).load()

    ordo_graph = nx.DiGraph()
    concepts = []
    part_of_prop = [p for p in ordo_ontology.object_properties() if p.name.startswith('BFO_0000050')][0]

    for ordo_class in ordo_ontology.classes():
        if ordo_class.name.startswith('Orphanet_'):
            concept, relationships = _process_ordo_class(ordo_class, part_of_prop)
            concepts.append(concept)
            ordo_graph.add_node(concept.concept_id)

            for source_id, target_id, rel_type in relationships:
                ordo_graph.add_edge(
                    source_id,
                    target_id,
                    label=rel_type
                )

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.save_terms(
        terms=concepts
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=ordo_graph,
    )


async def create_indexes(overwrite: bool = False,
                         doc_db: DocumentDatabase = None,
                         graph_db: GraphDatabase = None,
                         ):
    """
    Create indexes for the ORDO vocabulary in the primary databases.
    :param overwrite: Whether to overwrite existing indexes.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.create_index(
        prefix=ConceptPrefix.ORDO,
        field='conceptId',
        unique=True,
        overwrite=overwrite,
    )
    await doc_db.create_index(
        prefix=ConceptPrefix.ORDO,
        field='label',
        overwrite=overwrite,
    )

    await graph_db.create_index()


async def delete_vocabulary_data(doc_db: DocumentDatabase = None,
                                 graph_db: GraphDatabase = None,
                                 ):
    """
    Delete all ORDO vocabulary data from the primary databases.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.delete_all_for_label(ConceptPrefix.ORDO)
    await graph_db.delete_vocabulary_graph(prefix=ConceptPrefix.ORDO)
