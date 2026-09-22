from functools import lru_cache

import httpx
from owlready2 import default_world, ThingClass
from urllib.parse import unquote

from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod, \
    AnnotationType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_obo_owl_release, iter_progress, \
    load_obo_owl_classes, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept
from bioterms.model.edge_buffer import EdgeBuffer
from bioterms.model.annotation import Annotation
from bioterms.annotation.utils import AnnotationSource, is_gene_annotation_prefix
from .utils import write_concepts_to_file, write_graph_to_file


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
    SimilarityMethod.CO_ANNOTATION,
    SimilarityMethod.RELEVANCE,
    SimilarityMethod.WEIGHED_RELEVANCE,
]
FILE_PATHS = ['mondo/mondo.owl']
TIMESTAMP_FILE = 'mondo/.timestamp'
CONCEPT_CLASS = Concept


@lru_cache
def _mondo_annotation_source(target_prefix: ConceptPrefix | str) -> AnnotationSource:
    return AnnotationSource('Mondo', VOCABULARY_PREFIX, target_prefix)


def _create_mondo_annotation(target_prefix: ConceptPrefix | str,
                             concept_id: str,
                             target_id: str,
                             annotation_type: AnnotationType,
                             properties: dict[str, str] | None,
                             ) -> Annotation:
    # HGNC is a gene-vocabulary link and intentionally retains the legacy, provenance-free form.
    if is_gene_annotation_prefix(target_prefix):
        return Annotation(
            prefixFrom=VOCABULARY_PREFIX,
            prefixTo=target_prefix,
            conceptIdFrom=concept_id,
            conceptIdTo=target_id,
            annotationType=annotation_type,
            properties=properties,
        )
    return _mondo_annotation_source(target_prefix).create(
        publisher_concept_id=concept_id,
        other_concept_id=target_id,
        annotation_type=annotation_type,
        properties=properties,
    )


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
        'http://www.ebi.ac.uk/efo/EFO_': 'EFO',
        'http://www.orpha.net/ORDO/Orphanet_': 'Orphanet',
        'https://icd.who.int/browse10/2019/en#/': 'ICD10WHO',
        'https://omim.org/entry/': 'OMIM',
        'https://omim.org/phenotypicSeries/PS': 'OMIMPS',
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

# Mondo stores per-xref provenance on reified hasDbXref axioms. Match attributes reuse
# provenance from the corresponding (concept, xref) pair when available.
_XREF_SOURCE_QUERY = """
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
SELECT ?src ?target ?source WHERE {
  ?ax a owl:Axiom .
  ?ax owl:annotatedSource ?src .
  ?ax owl:annotatedProperty oboInOwl:hasDbXref .
  ?ax owl:annotatedTarget ?target .
  ?ax oboInOwl:source ?source .
}
"""


def _build_xref_source_lookup(ontology_world=None) -> dict[tuple[str, str], str]:
    """Map Mondo concept/xref pairs to their optional provenance tags."""
    world = ontology_world if ontology_world is not None else default_world
    lookup: dict[tuple[str, str], str] = {}

    # Undefined query entities are expected in small or synthetic ontologies.
    for src, target, source in world.sparql(_XREF_SOURCE_QUERY, error_on_undefined_entities=False):
        if not hasattr(src, 'name') or not src.name.startswith('MONDO_'):
            continue
        concept_id = src.name.split('_')[-1]
        lookup[(concept_id, str(target))] = str(source)

    return lookup


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


def _add_mondo_is_a_edges(mondo_graph: EdgeBuffer,
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
                                  xref_source_lookup: dict[tuple[str, str], str] = None,
                                  ) -> list[Annotation]:
    """
    Build the cross-vocabulary annotations for a Mondo class from its exact/broad/narrow/related
    match xrefs, falling back to hasDbXref entries not already covered by those matches.
    :param mondo_class: The owlready2 class to extract xrefs from.
    :param concept_id: The Mondo concept ID the annotations originate from.
    :param xref_source_lookup: Optional (concept_id, xref curie) -> oboInOwl:source tag lookup
        (see _build_xref_source_lookup). When a pair is found, the tag is attached as the
        annotation's 'mappingSource' property; a missing pair is left untagged rather than
        treated as an error, since Mondo does not annotate every xref this way.
    :return: The list of built Annotation instances.
    """
    annotations = []
    cross_references = set()
    xref_source_lookup = xref_source_lookup or {}

    def _source_properties(curie_id: str) -> dict[str, str] | None:
        source = xref_source_lookup.get((concept_id, curie_id))
        return {'mappingSource': source} if source else None

    for attribute_name, annotation_type in _XREF_MATCH_ATTRIBUTES:
        for m in getattr(mondo_class, attribute_name, []):
            curie_id = map_xref_url(m)
            if not curie_id:
                # Unknown mapping, skip it
                continue

            cross_references.add(curie_id)

            vocabulary_prefix = map_vocabulary_prefix(curie_id.split(':', 1)[0])
            target_id = curie_id.split(':', 1)[1]
            annotations.append(_create_mondo_annotation(
                vocabulary_prefix, concept_id, target_id, annotation_type,
                _source_properties(curie_id),
            ))

    for xref in getattr(mondo_class, 'hasDbXref', []):
        if xref in cross_references:
            continue

        # Other type of matches, default to ANNOTATED_WITH
        if ':' not in xref:
            continue

        xref_prefix, target_id = xref.split(':', 1)
        vocabulary_prefix = map_vocabulary_prefix(xref_prefix)
        annotations.append(_create_mondo_annotation(
            vocabulary_prefix, concept_id, target_id, AnnotationType.ANNOTATED_WITH,
            _source_properties(xref),
        ))

    return annotations


async def load_mondo_annotations_from_file(target_prefix: ConceptPrefix,
                                           graph_db: GraphDatabase = None,
                                           ) -> int:
    """Load one Mondo cross-reference namespace independently of the vocabulary.

    This is the annotation-only counterpart of ``load_vocabulary_from_file``. It is
    intentionally usable after a Mondo import performed with ``--no-annotation`` so a
    database build can pipeline concept loading and cross-vocabulary mapping loading.

    :param target_prefix: The supported vocabulary namespace to retain from Mondo xrefs.
    :param graph_db: Optional graph database instance.
    :return: Number of annotations saved.
    """
    if target_prefix not in ANNOTATIONS:
        raise ValueError(f'Mondo does not declare annotations to {target_prefix.value}.')
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Mondo owl file not found')
    if graph_db is None:
        graph_db = get_active_graph_db()

    verbose_print(f'Loading Mondo annotations to {target_prefix.value}')
    mondo_ontology, mondo_classes = load_obo_owl_classes(FILE_PATHS[0], 'MONDO_')
    xref_source_lookup = _build_xref_source_lookup(mondo_ontology.world)
    annotations = []

    for mondo_class in iter_progress(
        mondo_classes,
        description=f'Processing Mondo to {target_prefix.value} annotations',
        total=len(mondo_classes),
    ):
        if not mondo_class.name.startswith('MONDO_'):
            continue
        concept_id = mondo_class.name.split('_')[-1]
        annotations.extend(
            annotation
            for annotation in _build_mondo_xref_annotations(
                mondo_class,
                concept_id,
                xref_source_lookup,
            )
            if annotation.prefix_to == target_prefix
        )

    verbose_print(
        f'Saving {len(annotations)} Mondo to {target_prefix.value} annotations to the database...'
    )
    await graph_db.save_annotations(annotations)
    return len(annotations)


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the Mondo vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    await download_obo_owl_release(
        release_url='https://github.com/monarch-initiative/mondo/releases/latest/download/mondo.owl',
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True,
                                    ):
    """
    Load the Mondo vocabulary from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Mondo owl file not found')

    verbose_print('Loading Mondo ontology')
    _, mondo_classes = load_obo_owl_classes(FILE_PATHS[0], 'MONDO_')
    verbose_print('Mondo ontology read from file')

    mondo_graph = EdgeBuffer()
    concepts = []

    for mondo_class in iter_progress(mondo_classes, description='Processing Mondo classes', total=len(mondo_classes)):
        if not mondo_class.name.startswith('MONDO_'):
            continue

        concept = _build_mondo_concept(mondo_class)

        concepts.append(concept)
        mondo_graph.add_node(concept.concept_id)

        _add_mondo_is_a_edges(mondo_graph, mondo_class, concept.concept_id)

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
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            build_search_index=build_search_index,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=mondo_graph,
        )
