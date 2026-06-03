import httpx

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist
from bioterms.database import GraphDatabase, get_active_graph_db
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'Ohdsi Mapping to NCIT'
VOCABULARY_PREFIX_1 = ConceptPrefix.OHDSI
VOCABULARY_PREFIX_2 = ConceptPrefix.NCIT
FILE_PATHS = [
    'ohdsi/CONCEPT.csv',
]


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the Ohdsi release file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    raise FilesNotFound(
        message='Ohdsi to NCIT mapping file is part of the Ohdsi release, and cannot be downloaded separately',
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the Ohdsi to NCIT mapping from a file into the primary databases.
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
        'Ohdsi to NCIT mapping is part of the Ohdsi release, and should have been loaded during the Ohdsi import'
    )
