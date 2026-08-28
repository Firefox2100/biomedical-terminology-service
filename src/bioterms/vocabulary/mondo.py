import os
import httpx
import networkx as nx
from owlready2 import get_ontology, default_world, ThingClass
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
    SimilarityMethod.CO_ANNOTATION,
    SimilarityMethod.RELEVANCE,
    SimilarityMethod.WEIGHED_RELEVANCE,
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

# MONDO reifies most hasDbXref statements as an owl:Axiom carrying an oboInOwl:source
# annotation -- a curator ORCID, a GitHub issue URL, or a generic marker like
# 'MONDO:equivalentTo' (bulk import) or 'MONDO:exact-label-match' (algorithmic, weaker
# than a curated match). Confirmed live against data/mondo/mondo.owl: covers ~147k of the
# ontology's ~416k hasDbXref statements (the rest carry no axiom annotation at all).
# exactMatch/broadMatch/narrowMatch/relatedMatch carry essentially none of their own --
# provenance is looked up by (concept_id, xref target curie) so a match-attribute-derived
# annotation picks up the tag from the corresponding raw hasDbXref triple when one exists.
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
    """
    Build a (concept_id, xref target curie) -> oboInOwl:source tag lookup from Mondo's
    reified owl:Axiom blocks on hasDbXref triples. Pairs with no axiom annotation are
    simply absent from the returned dict -- callers must treat a missing key as
    "no per-xref provenance available", not as an error.
    :param ontology_world: The owlready2 World to query (defaults to owlready2's default_world).
    :return: A dict mapping (concept_id, xref_target_curie) to the source tag string.
    """
    world = ontology_world if ontology_world is not None else default_world
    lookup: dict[tuple[str, str], str] = {}

    # error_on_undefined_entities=False: owlready2 otherwise raises if the world's triple
    # store never references one of the queried IRIs at all (e.g. a small/synthetic
    # ontology in tests) -- that case must behave like "no matches", not an error.
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
            annotations.append(Annotation(
                prefixFrom=VOCABULARY_PREFIX,
                prefixTo=vocabulary_prefix,
                conceptIdFrom=concept_id,
                conceptIdTo=target_id,
                annotationType=annotation_type,
                properties=_source_properties(curie_id),
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
            properties=_source_properties(xref),
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

    xref_source_lookup = _build_xref_source_lookup(mondo_ontology.world)
    verbose_print(f'Built per-xref provenance lookup for {len(xref_source_lookup)} hasDbXref statements')

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
        annotations.extend(_build_mondo_xref_annotations(mondo_class, concept.concept_id, xref_source_lookup))

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
