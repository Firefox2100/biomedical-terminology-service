import httpx

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import verbose_print
from bioterms.vocabulary.uberon import download_vocabulary
from .utils import assert_pre_requisite, load_obo_xref_annotations


ANNOTATION_NAME = 'Uberon Mapping to Gene Ontology'
VOCABULARY_PREFIX_1 = ConceptPrefix.UBERON
VOCABULARY_PREFIX_2 = ConceptPrefix.GO
FILE_PATHS = ['uberon/uberon.owl']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """Reuse the Uberon release containing GO cross-references."""
    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Load GO cross-references published in Uberon."""
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    annotations = load_obo_xref_annotations(
        FILE_PATHS[0], ConceptPrefix.UBERON, 'UBERON', 'GO', ConceptPrefix.GO,
        'Uberon hasDbXref',
    )
    verbose_print(f'Processed {len(annotations)} Uberon to GO annotations. Saving...')
    await graph_db.save_annotations(annotations)
