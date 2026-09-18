"""Gene Ontology vocabulary download and loading support."""

import httpx
import networkx as nx
from owlready2 import Restriction, ThingClass

from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, \
    get_active_graph_db
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus, \
    SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_obo_owl_release, iter_progress, \
    load_obo_owl_classes, obo_class_metadata, obo_entity_local_id, verbose_print
from bioterms.model.concept import Concept
from .utils import write_concepts_to_file, write_graph_to_file


VOCABULARY_NAME = 'Gene Ontology'
VOCABULARY_PREFIX = ConceptPrefix.GO
ANNOTATIONS = [
    ConceptPrefix.REACTOME,
    ConceptPrefix.UBERON,
    ConceptPrefix.UNIPROT,
]
SIMILARITY_METHODS = [
    SimilarityMethod.RELEVANCE,
    SimilarityMethod.CO_ANNOTATION,
    SimilarityMethod.WEIGHED_RELEVANCE,
]
FILE_PATHS = ['go/go-basic.owl']
TIMESTAMP_FILE = 'go/.timestamp'
CONCEPT_CLASS = Concept

_OBO_ID_PREFIX = 'GO'
_RESTRICTION_RELATIONSHIPS = {
    'BFO_0000050': ConceptRelationshipType.PART_OF,
    'RO_0002211': ConceptRelationshipType.REGULATES,
    'RO_0002212': ConceptRelationshipType.NEGATIVELY_REGULATES,
    'RO_0002213': ConceptRelationshipType.POSITIVELY_REGULATES,
}


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """Download GO's hierarchy-safe basic OWL production release."""
    await download_obo_owl_release(
        release_url='https://purl.obolibrary.org/obo/go/go-basic.owl',
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


def _construct_go_concept(go_class: ThingClass) -> CONCEPT_CLASS:
    """Construct a concept from one GO OWL class."""
    metadata = obo_class_metadata(go_class)
    return CONCEPT_CLASS(
        prefix=VOCABULARY_PREFIX,
        conceptTypes=[],
        conceptId=obo_entity_local_id(go_class, _OBO_ID_PREFIX),
        label=metadata['label'],
        definition=metadata['definition'],
        comment=metadata['comment'],
        synonyms=metadata['synonyms'],
        status=ConceptStatus.DEPRECATED if metadata['deprecated'] else ConceptStatus.ACTIVE,
    )


def _process_go_class(
    go_class: ThingClass,
) -> tuple[CONCEPT_CLASS, list[tuple[str, str, ConceptRelationshipType]]]:
    """Build one GO concept and its hierarchy-safe asserted relationships."""
    concept = _construct_go_concept(go_class)
    relationships: list[tuple[str, str, ConceptRelationshipType]] = []

    for parent in getattr(go_class, 'is_a', []):
        relationship_type = None
        target = None
        if isinstance(parent, ThingClass):
            relationship_type = ConceptRelationshipType.IS_A
            target = parent
        elif isinstance(parent, Restriction):
            relationship_type = _RESTRICTION_RELATIONSHIPS.get(parent.property.name)
            target = parent.value

        target_id = obo_entity_local_id(target, _OBO_ID_PREFIX)
        if relationship_type is not None and target_id is not None:
            relationships.append((concept.concept_id, target_id, relationship_type))

    for alternative_id in getattr(go_class, 'hasAlternativeId', []):
        local_id = obo_entity_local_id(alternative_id, _OBO_ID_PREFIX)
        if local_id is not None:
            relationships.append((
                local_id, concept.concept_id, ConceptRelationshipType.REPLACED_BY,
            ))

    for replacement in getattr(go_class, 'IAO_0100001', []):
        replacement_id = obo_entity_local_id(replacement, _OBO_ID_PREFIX)
        if replacement_id is not None:
            relationships.append((
                concept.concept_id, replacement_id, ConceptRelationshipType.REPLACED_BY,
            ))

    for considered in getattr(go_class, 'consider', []):
        considered_id = obo_entity_local_id(considered, _OBO_ID_PREFIX)
        if considered_id is not None:
            relationships.append((
                concept.concept_id, considered_id, ConceptRelationshipType.CONSIDER,
            ))

    return concept, relationships


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True,
                                    ):
    """Load GO concepts and hierarchy-safe relationships from ``go-basic.owl``."""
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('GO basic OWL file not found')

    verbose_print('Loading GO basic OWL release...')
    _, go_classes = load_obo_owl_classes(FILE_PATHS[0], 'GO_')
    verbose_print(f'GO basic OWL release loaded with {len(go_classes)} classes.')

    concepts = []
    go_graph = nx.MultiDiGraph()
    for go_class in iter_progress(
        go_classes, description='Processing GO classes', total=len(go_classes),
    ):
        concept, relationships = _process_go_class(go_class)
        concepts.append(concept)
        go_graph.add_node(concept.concept_id)
        for source_id, target_id, relationship_type in relationships:
            go_graph.add_edge(
                source_id, target_id, key=relationship_type.value, label=relationship_type,
            )

    if not offline:
        doc_db = doc_db or await get_active_doc_db()
        graph_db = graph_db or get_active_graph_db()
        await doc_db.save_terms(terms=concepts, no_upsert=True)
        await graph_db.save_vocabulary_graph(concepts=concepts, graph=go_graph)
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            build_search_index=build_search_index,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=go_graph,
        )
