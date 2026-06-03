import httpx

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import GraphDatabase, get_active_graph_db
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'Mondo Mapping to HGNC'
VOCABULARY_PREFIX_1 = ConceptPrefix.MONDO
VOCABULARY_PREFIX_2 = ConceptPrefix.HGNC
FILE_PATHS = ['mondo/mondo.owl']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the Mondo release file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    from bioterms.vocabulary.mondo import download_vocabulary

    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the Mondo to HGNC mapping from a file into the primary databases.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    await assert_pre_requisite(
        annotation_name=ANNOTATION_NAME,
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
        file_paths=FILE_PATHS,
        graph_db=graph_db,
    )

    raise NotImplementedError(
        'Mondo to HGNC mapping is part of the Mondo release, and should have been loaded during the Mondo import'
    )
