import os
import httpx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_vocabulary_loaded


ANNOTATION_NAME = 'NCIT Mapping to HGNC Gene Symbol'
VOCABULARY_PREFIX_1 = ConceptPrefix.NCIT
VOCABULARY_PREFIX_2 = ConceptPrefix.HGNC_SYMBOL
FILE_PATHS = ['ncit/gene_mapping.txt']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the NCIT gene mapping file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    annotation_url = 'https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Mappings/NCIt-HGNC_Mapping.txt'

    await download_file(
        url=annotation_url,
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the NCIT to HGNC symbol mapping from a file into the primary databases.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    await assert_vocabulary_loaded(
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
        graph_db=graph_db,
    )

    annotation_count = await graph_db.count_annotations(
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
    )

    if annotation_count > 0:
        return  # Annotations already exist, skip loading

    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('NCIT-HGNC symbol mapping file not found. Please download it first.')

    mapping_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        sep='\t',
        header=None,
        names=['ncit_id', 'hgnc_id'],
    )

    annotations = []

    for _, row in mapping_df.iterrows():
        annotations.append(Annotation(
            prefixFrom=VOCABULARY_PREFIX_1,
            prefixTo=VOCABULARY_PREFIX_2,
            conceptIdFrom=row['ncit_id'],
            conceptIdTo=row['hgnc_id'].split(':')[-1],
        ))

    await graph_db.save_annotations(
        annotations=annotations
    )
