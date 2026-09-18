import httpx

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import check_files_exist, verbose_print
from bioterms.vocabulary.hgnc import download_vocabulary
from bioterms.vocabulary.uniprot import FILE_PATHS as UNIPROT_FILE_PATHS, iter_hgnc_annotations
from .utils import assert_pre_requisite, load_hgnc_mapping, save_annotation_stream


ANNOTATION_NAME = 'HGNC Mapping to UniProtKB'
VOCABULARY_PREFIX_1 = ConceptPrefix.HGNC
VOCABULARY_PREFIX_2 = ConceptPrefix.UNIPROT
FILE_PATHS = ['hgnc/symbol.txt']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """Reuse the complete HGNC release containing UniProt-provided cross-references."""
    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Load the UniProt cross-references published in the HGNC release."""
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    annotations = load_hgnc_mapping(
        'uniprot_ids', ConceptPrefix.UNIPROT, AnnotationType.ANNOTATED_WITH,
    )
    verbose_print(f'Processed {len(annotations)} HGNC to UniProt annotations. Saving...')
    await graph_db.save_annotations(annotations)
    if check_files_exist(UNIPROT_FILE_PATHS):
        count = await save_annotation_stream(graph_db, iter_hgnc_annotations())
        verbose_print(f'Loaded {count} UniProtKB to HGNC annotations.')
