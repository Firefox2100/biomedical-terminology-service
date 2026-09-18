import httpx

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from .utils import assert_pre_requisite, download_hpoa, hpoa_file_path, load_hpoa_annotations


ANNOTATION_NAME = 'HPO phenotype annotations to OMIM'
VOCABULARY_PREFIX_1 = ConceptPrefix.HPO
VOCABULARY_PREFIX_2 = ConceptPrefix.OMIM
FILE_PATHS = [hpoa_file_path()]


async def download_annotation(download_client: httpx.AsyncClient = None):
    """Download ``phenotype.hpoa`` from the current official HPO release."""
    await download_hpoa(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Load the OMIM subset of HPO's disease annotations in publisher-first direction."""
    if graph_db is None:
        graph_db = get_active_graph_db()

    await assert_pre_requisite(
        annotation_name=ANNOTATION_NAME,
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
        file_paths=FILE_PATHS,
        graph_db=graph_db,
    )

    annotations = load_hpoa_annotations('OMIM', ConceptPrefix.OMIM)
    verbose_print(f'Processed {len(annotations)} HPO to OMIM annotations. Saving to database...')
    await graph_db.save_annotations(annotations)
