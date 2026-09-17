import httpx

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import verbose_print
from bioterms.vocabulary.hgnc import download_vocabulary
from .utils import assert_pre_requisite, load_hgnc_mapping


ANNOTATION_NAME = 'HGNC Mapping to Ensembl Gene'
VOCABULARY_PREFIX_1 = ConceptPrefix.HGNC
VOCABULARY_PREFIX_2 = ConceptPrefix.ENSEMBL
FILE_PATHS = ['hgnc/symbol.txt']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """Reuse the complete HGNC release containing its Ensembl gene cross-references."""
    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Load HGNC-curated Ensembl gene cross-references."""
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    annotations = load_hgnc_mapping(
        'ensembl_gene_id', ConceptPrefix.ENSEMBL, AnnotationType.EXACT,
    )
    verbose_print(f'Processed {len(annotations)} HGNC to Ensembl annotations. Saving...')
    await graph_db.save_annotations(annotations)
