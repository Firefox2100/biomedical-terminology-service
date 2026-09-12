import httpx

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist
from bioterms.database import GraphDatabase, get_active_graph_db
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'UniProt Mapping to HGNC Gene Symbol'
VOCABULARY_PREFIX_1 = ConceptPrefix.UNIPROT
VOCABULARY_PREFIX_2 = ConceptPrefix.HGNC_SYMBOL
FILE_PATHS = [
    'uniprot/uniprot_sprot.dat.gz',
    'uniprot/uniprot_trembl.dat.gz',
]


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the UniProt release files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    raise FilesNotFound(
        message='UniProt to HGNC Gene Symbol mapping is part of the UniProt release, and cannot be '
                'downloaded separately',
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the UniProt to HGNC Gene Symbol mapping from a file into the primary databases.
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
        'UniProt to HGNC Gene Symbol mapping is part of the UniProt release, and should have been '
        'loaded during the UniProt import'
    )
