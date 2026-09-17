"""Uberon vocabulary download and loading support."""

import httpx
import networkx as nx
from owlready2 import Restriction, ThingClass

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus, \
    SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_obo_owl_release, iter_progress, \
    load_obo_owl_classes, obo_class_metadata, obo_entity_local_id, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, \
    get_active_graph_db
from bioterms.model.concept import Concept
from .utils import write_concepts_to_file, write_graph_to_file


VOCABULARY_NAME = 'Uberon Multi-Species Anatomy Ontology'
VOCABULARY_PREFIX = ConceptPrefix.UBERON
ANNOTATIONS = [
    ConceptPrefix.NCIT,
    ConceptPrefix.SNOMED,
]
SIMILARITY_METHODS = [
    SimilarityMethod.RELEVANCE,
    SimilarityMethod.CO_ANNOTATION,
    SimilarityMethod.WEIGHED_RELEVANCE,
]
FILE_PATHS = ['uberon/uberon.owl']
TIMESTAMP_FILE = 'uberon/.timestamp'
CONCEPT_CLASS = Concept

_OBO_ID_PREFIX = 'UBERON'


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """Download the canonical Uberon OWL release product."""
    await download_obo_owl_release(
        release_url='https://github.com/obophenotype/uberon/releases/latest/download/uberon.owl',
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


def _construct_uberon_concept(uberon_class: ThingClass) -> CONCEPT_CLASS:
    """Construct a concept from one Uberon OWL class."""
    metadata = obo_class_metadata(uberon_class)
    return CONCEPT_CLASS(
        prefix=VOCABULARY_PREFIX,
        conceptTypes=[],
        conceptId=obo_entity_local_id(uberon_class, _OBO_ID_PREFIX),
        label=metadata['label'],
        definition=metadata['definition'],
        comment=metadata['comment'],
        synonyms=metadata['synonyms'],
        status=ConceptStatus.DEPRECATED if metadata['deprecated'] else ConceptStatus.ACTIVE,
    )


def _process_uberon_class(
    uberon_class: ThingClass,
) -> tuple[CONCEPT_CLASS, list[tuple[str, str, ConceptRelationshipType]]]:
    """Build one Uberon concept and its asserted within-Uberon relationships."""
    concept = _construct_uberon_concept(uberon_class)
    relationships: list[tuple[str, str, ConceptRelationshipType]] = []

    for parent in getattr(uberon_class, 'is_a', []):
        if isinstance(parent, ThingClass):
            parent_id = obo_entity_local_id(parent, _OBO_ID_PREFIX)
            if parent_id is not None:
                relationships.append((
                    concept.concept_id,
                    parent_id,
                    ConceptRelationshipType.IS_A,
                ))
        elif isinstance(parent, Restriction) and parent.property.name == 'BFO_0000050':
            parent_id = obo_entity_local_id(parent.value, _OBO_ID_PREFIX)
            if parent_id is not None:
                relationships.append((
                    concept.concept_id,
                    parent_id,
                    ConceptRelationshipType.PART_OF,
                ))

    # OBO alternate IDs are retired identifiers redirected to the current class.
    for alternative_id in getattr(uberon_class, 'hasAlternativeId', []):
        local_id = obo_entity_local_id(alternative_id, _OBO_ID_PREFIX)
        if local_id is not None:
            relationships.append((
                local_id,
                concept.concept_id,
                ConceptRelationshipType.REPLACED_BY,
            ))

    # IAO:0100001 is OBO's term-replaced-by annotation. The full Uberon product also
    # contains imported replacement targets; only retain edges inside the Uberon namespace.
    for replacement in getattr(uberon_class, 'IAO_0100001', []):
        replacement_id = obo_entity_local_id(replacement, _OBO_ID_PREFIX)
        if replacement_id is not None:
            relationships.append((
                concept.concept_id,
                replacement_id,
                ConceptRelationshipType.REPLACED_BY,
            ))

    for considered in getattr(uberon_class, 'consider', []):
        considered_id = obo_entity_local_id(considered, _OBO_ID_PREFIX)
        if considered_id is not None:
            relationships.append((
                concept.concept_id,
                considered_id,
                ConceptRelationshipType.CONSIDER,
            ))

    return concept, relationships


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True,
                                    ):
    """Load Uberon concepts and asserted relationships from its canonical OWL product."""
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Uberon OWL file not found')

    verbose_print('Loading Uberon OWL release...')
    _, uberon_classes = load_obo_owl_classes(
        file_path=FILE_PATHS[0],
        class_name_prefix='UBERON_',
    )
    verbose_print(f'Uberon OWL release loaded with {len(uberon_classes)} classes.')

    concepts = []
    uberon_graph = nx.MultiDiGraph()
    for uberon_class in iter_progress(
        uberon_classes,
        description='Processing Uberon classes',
        total=len(uberon_classes),
    ):
        concept, relationships = _process_uberon_class(uberon_class)
        concepts.append(concept)
        uberon_graph.add_node(concept.concept_id)
        for source_id, target_id, relationship_type in relationships:
            uberon_graph.add_edge(
                source_id,
                target_id,
                key=relationship_type.value,
                label=relationship_type,
            )

    if not offline:
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        await doc_db.save_terms(terms=concepts, no_upsert=True)
        await graph_db.save_vocabulary_graph(concepts=concepts, graph=uberon_graph)
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            build_search_index=build_search_index,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=uberon_graph,
        )
