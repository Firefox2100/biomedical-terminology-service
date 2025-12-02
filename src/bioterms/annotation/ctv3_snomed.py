import os
import httpx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_rf2, get_trud_release_url, \
    rf2_dataframe_deduplicate
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_vocabulary_loaded


ANNOTATION_NAME = 'SNOMED Mapping to CTV3'
VOCABULARY_PREFIX_1 = ConceptPrefix.SNOMED
VOCABULARY_PREFIX_2 = ConceptPrefix.CTV3
FILE_PATHS = ['snomed/international/ctv3_snomed_map.txt']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the SNOMED mapping file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    if not CONFIG.nhs_trud_api_key:
        raise ValueError('NHS TRUD API key is required to download SNOMED mapping.')
    api_key = CONFIG.nhs_trud_api_key

    international_url = await get_trud_release_url(
        f'https://isd.digital.nhs.uk/trud/api/v1/keys/{api_key}/items/4/releases?latest'
    )

    await download_rf2(
        release_url=international_url,
        file_mapping=[
            ('SnomedCT_InternationalRF2*/Full/Refset/Map/der2_sRefset_SimpleMapFull_INT*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
        ],
        download_client=download_client,
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the SNOMED to CTV3 mapping from a file into the primary databases.
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
        raise FilesNotFound('CTV3 to SNOMED mapping file not found')

    mapping_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        sep='\t',
    )
    mapping_df = rf2_dataframe_deduplicate(mapping_df)

    annotations = []

    for _, row in mapping_df.iterrows():
        annotations.append(Annotation(
            prefixFrom=ConceptPrefix.SNOMED,
            conceptIdFrom=row['referencedComponentId'],
            prefixTo=ConceptPrefix.CTV3,
            conceptIdTo=row['mapTarget'],
        ))

    await graph_db.save_annotations(
        annotations=annotations
    )
