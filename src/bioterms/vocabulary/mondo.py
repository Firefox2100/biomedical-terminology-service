import os
import httpx
import networkx as nx
from owlready2 import get_ontology, ThingClass
from urllib.parse import unquote

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod, \
    AnnotationType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, iter_progress, \
    verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept
from bioterms.model.annotation import Annotation
from .utils import write_concepts_to_file, write_graph_to_file, write_annotations_to_file


VOCABULARY_NAME = 'Mondo Disease Ontology'
VOCABULARY_PREFIX = ConceptPrefix.MONDO
ANNOTATIONS = [
    ConceptPrefix.HGNC,
    ConceptPrefix.HPO,
    ConceptPrefix.NCIT,
    ConceptPrefix.OMIM,
    ConceptPrefix.ORDO,
    ConceptPrefix.SNOMED,
]
SIMILARITY_METHODS = [
]
FILE_PATHS = ['mondo/mondo.owl']
TIMESTAMP_FILE = 'mondo/.timestamp'
CONCEPT_CLASS = Concept
CONCEPT_TYPES = []


def map_vocabulary_prefix(vocabulary_id: str) -> ConceptPrefix | str:
    """
    Given a vocabulary prefix in Mondo, map to the corresponding ConceptPrefix in this system.
    If the vocabulary is not fully supported, it will convert it into a string that may
    be used in the future as the basis of a new ConceptPrefix.
    :param vocabulary_id: The vocabulary ID.
    :return: The mapped ConceptPrefix or string.
    """
    mapping = {
        'HGNC': ConceptPrefix.HGNC,
        'HP': ConceptPrefix.HPO,
        'NCIT': ConceptPrefix.NCIT,
        'OMIM': ConceptPrefix.OMIM,
        'Orphanet': ConceptPrefix.ORDO,
        'SCTID': ConceptPrefix.SNOMED,
    }

    return mapping.get(vocabulary_id, vocabulary_id.lower())


def map_xref_url(url: str) -> str | None:
    """
    Given a Xref URL, convert to curie styled ID used in Mondo
    :param url: The URL to convert
    :return: The converted ID
    """
    mapping = {
        'http://id.who.int/icd/entity/': 'icd11.foundation',
        'http://identifiers.org/meddra/': 'MedDRA',
        'http://identifiers.org/medgen/': 'MEDGEN',
        'http://identifiers.org/mesh/': 'MESH',
        'http://identifiers.org/snomedct/': 'SCTID',
        'http://linkedlifedata.com/resource/umls/id/': 'UMLS',
        'http://purl.bioontology.org/ontology/ICD10CM/': 'ICD10CM',
        'http://purl.obolibrary.org/obo/DOID_': 'DOID',
        'http://purl.obolibrary.org/obo/NCIT_': 'NCIT',
        'http://www.ebi.ac.uk/efo/': 'EFO',
        'http://www.orpha.net/ORDO/Orphanet_': 'Orphanet',
        'https://icd.who.int/browse10/2019/en#/': 'ICD10WHO',
        'https://omim.org/entry/': 'OMIM',
        'https://omim.org/phenotypicSeries/': 'OMIMPS',
    }

    url = unquote(str(url).strip())

    for url_prefix, curie_prefix in sorted(
        mapping.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if url.startswith(url_prefix):
            local_id = url[len(url_prefix):]
            if local_id:
                return f'{curie_prefix}:{local_id}'

    return None


_XREF_MATCH_ATTRIBUTES = (
    ('exactMatch', AnnotationType.EXACT),
    ('broadMatch', AnnotationType.BROAD),
    ('narrowMatch', AnnotationType.NARROW),
    ('relatedMatch', AnnotationType.RELATED),
)


def _build_mondo_concept(mondo_class: ThingClass) -> Concept:
    """
    Build a Concept instance from a Mondo ontology class.
    :param mondo_class: The owlready2 class representing the Mondo concept.
    :return: The built Concept instance.
    """
    return CONCEPT_CLASS(
        prefix=VOCABULARY_PREFIX,
        conceptTypes=[],
        conceptId=mondo_class.name.split('_')[-1],
        label=mondo_class.label[0]
            if hasattr(mondo_class, 'label') and mondo_class.label
            else None,
        definition=mondo_class.IAO_0000115[0]
            if hasattr(mondo_class, 'IAO_0000115') and mondo_class.IAO_0000115
            else None,
        comment=mondo_class.comment[0]
            if hasattr(mondo_class, 'comment') and mondo_class.comment
            else None,
        status=ConceptStatus.DEPRECATED
            if hasattr(mondo_class, 'deprecated') and bool(mondo_class.deprecated)
            else ConceptStatus.ACTIVE,
        synonyms=mondo_class.hasExactSynonym
            if hasattr(mondo_class, 'hasExactSynonym') and mondo_class.hasExactSynonym
            else None
    )


def _add_mondo_is_a_edges(mondo_graph: nx.DiGraph,
                          mondo_class: ThingClass,
                          concept_id: str,
                          ):
    """
    Add is-a edges to the Mondo graph for the parents of a given Mondo class.
    :param mondo_graph: The Mondo graph to add edges to.
    :param mondo_class: The owlready2 class to read parents from.
    :param concept_id: The concept ID of the Mondo class.
    """
    if not hasattr(mondo_class, 'is_a'):
        return

    for parent in mondo_class.is_a:
        if isinstance(parent, ThingClass) and parent.name.startswith('MONDO_'):
            mondo_graph.add_edge(
                concept_id,
                parent.name.split('_')[-1],
                label=ConceptRelationshipType.IS_A
            )


def _build_mondo_xref_annotations(mondo_class: ThingClass,
                                  concept_id: str,
                                  ) -> list[Annotation]:
    """
    Build the cross-vocabulary annotations for a Mondo class from its exact/broad/narrow/related
    match xrefs, falling back to hasDbXref entries not already covered by those matches.
    :param mondo_class: The owlready2 class to extract xrefs from.
    :param concept_id: The Mondo concept ID the annotations originate from.
    :return: The list of built Annotation instances.
    """
    annotations = []
    cross_references = set()

    for attribute_name, annotation_type in _XREF_MATCH_ATTRIBUTES:
        for m in getattr(mondo_class, attribute_name, []):
            curie_id = map_xref_url(m)
            if not curie_id:
                # Unknown mapping, skip it
                continue

            cross_references.add(curie_id)

            vocabulary_prefix = map_vocabulary_prefix(curie_id.split(':', 1)[0])
            target_id = curie_id.split(':', 1)[1]
            annotations.append(Annotation(
                prefixFrom=VOCABULARY_PREFIX,
                prefixTo=vocabulary_prefix,
                conceptIdFrom=concept_id,
                conceptIdTo=target_id,
                annotationType=annotation_type,
            ))

    for xref in getattr(mondo_class, 'hasDbXref', []):
        if xref in cross_references:
            continue

        # Other type of matches, default to ANNOTATED_WITH
        if ':' not in xref:
            continue

        xref_prefix, target_id = xref.split(':', 1)
        vocabulary_prefix = map_vocabulary_prefix(xref_prefix)
        annotations.append(Annotation(
            prefixFrom=VOCABULARY_PREFIX,
            prefixTo=vocabulary_prefix,
            conceptIdFrom=concept_id,
            conceptIdTo=target_id,
            annotationType=AnnotationType.ANNOTATED_WITH,
        ))

    return annotations


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the Mondo vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    owl_url = 'https://github.com/monarch-initiative/mondo/releases/latest/download/mondo.owl'

    await download_file(
        url=owl_url,
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    ):
    """
    Load the Mondo vocabulary from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Mondo owl file not found')

    full_ontology_path = os.path.join(CONFIG.data_dir, FILE_PATHS[0])
    verbose_print(f'Loading Mondo ontology from {full_ontology_path}')

    owl_file_path = f'file://{full_ontology_path}'

    mondo_ontology = get_ontology(owl_file_path).load()
    mondo_classes = list(mondo_ontology.classes())
    verbose_print('Mondo ontology read from file')

    mondo_graph = nx.DiGraph()
    concepts = []
    annotations = []

    for mondo_class in iter_progress(mondo_classes, description='Processing Mondo classes', total=len(mondo_classes)):
        if not mondo_class.name.startswith('MONDO_'):
            continue

        concept = _build_mondo_concept(mondo_class)

        concepts.append(concept)
        mondo_graph.add_node(concept.concept_id)

        _add_mondo_is_a_edges(mondo_graph, mondo_class, concept.concept_id)
        annotations.extend(_build_mondo_xref_annotations(mondo_class, concept.concept_id))

    if not offline:
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        verbose_print('Saving Mondo concepts and graph to databases')

        await doc_db.save_terms(
            terms=concepts,
            no_upsert=True,
        )

        await graph_db.save_vocabulary_graph(
            concepts=concepts,
            graph=mondo_graph,
        )

        verbose_print(f'Saving {len(annotations)} OHDSI annotations to the database...')
        await graph_db.save_annotations(annotations)
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=mondo_graph,
        )
        await write_annotations_to_file(
            prefix_from=VOCABULARY_PREFIX,
            annotations=annotations,
        )
