import os
import httpx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_rf2, rf2_dataframe_deduplicate, \
    iter_progress, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'SNOMED CT Orphanet Map package'
VOCABULARY_PREFIX_1 = ConceptPrefix.SNOMED
VOCABULARY_PREFIX_2 = ConceptPrefix.ORDO
FILE_PATHS = ['snomed/orphanet_map/mapping.txt']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the SNOMED Orphanet Map package.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    if not CONFIG.nih_umls_api_key:
        raise ValueError('NIH UMLS API key is required to download SNOMED CT Orphanet Map package.')
    api_key = CONFIG.nih_umls_api_key

    download_url = ('https://download.nlm.nih.gov/umls/kss/IHTSDO2025/IHTSDO20250701/'
                    'SnomedCT_SNOMEDOrphanetMapPackage_PRODUCTION_20250930T120000Z.zip')
    release_url = f'https://uts-ws.nlm.nih.gov/download?url={download_url}&apiKey={api_key}'

    await download_rf2(
        release_url=release_url,
        file_mapping=[
            ('SnomedCT_SNOMEDOrphanetMapPackage*/Full/Refset/Map/der2_sRefset_OrphanetSimpleMapFull*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
        ],
        download_client=download_client,
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the SNOMED ORDO mapping from a file into the primary databases.
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

    mapping_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        sep='\t',
    )
    mapping_df = rf2_dataframe_deduplicate(mapping_df)
    mapping_df.drop(mapping_df[mapping_df['active'] == 0].index, inplace=True)

    verbose_print('SNOMED CT Orphanet Map package loaded from disk. Processing annotations...')

    annotations = []

    for _, row in iter_progress(
        mapping_df.iterrows(),
        desc='Processing SNOMED-ORDO annotations',
        total=len(mapping_df),
    ):
        annotations.append(Annotation(
            prefixFrom=VOCABULARY_PREFIX_1,
            prefixTo=VOCABULARY_PREFIX_2,
            conceptIdFrom=str(row['referencedComponentId']),
            conceptIdTo=str(row['mapTarget']),
        ))

    verbose_print(f'Inserting {len(annotations)} SNOMED-ORDO annotations into the database...')

    await graph_db.save_annotations(
        annotations=annotations
    )
