import httpx

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist
from bioterms.database import GraphDatabase, get_active_graph_db
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'Reactome Mapping to UniProt'
VOCABULARY_PREFIX_1 = ConceptPrefix.REACTOME
VOCABULARY_PREFIX_2 = ConceptPrefix.UNIPROT
FILE_PATHS = [
    'reactome/gene_mapping.csv',
]


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the Reactome release file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    raise FilesNotFound(
        message='Reactome to UniProt mapping file is part of the Reactome release, and cannot be '
                'downloaded separately',
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the Reactome to UniProt mapping from a file into the primary databases.
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
        'Reactome to UniProt mapping is part of the Reactome release, and should have been loaded '
        'during the Reactome import'
    )
