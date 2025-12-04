import os
import httpx
import aiofiles.os
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, extract_file_from_gzip, \
    iter_progress, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'OMIM Mapping to HGNC Gene Symbol'
VOCABULARY_PREFIX_1 = ConceptPrefix.OMIM
VOCABULARY_PREFIX_2 = ConceptPrefix.HGNC_SYMBOL
FILE_PATHS = ['omim/omim.csv']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the OMIM release file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    annotation_url = 'https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Mappings/NCIt-HGNC_Mapping.txt'
    gzip_path = os.path.join(CONFIG.data_dir, 'omim/omim.gz')

    if not CONFIG.bioportal_api_key:
        raise ValueError('BioPortal API key is required to download OMIM ontology.')

    try:
        await download_file(
            url=annotation_url,
            file_path='omim/omim.gz',
            headers={'Authorization': f'apikey token={CONFIG.bioportal_api_key}'},
            download_client=download_client,
        )

        await extract_file_from_gzip(
            gzip_path=gzip_path,
            output_path=os.path.join(CONFIG.data_dir, FILE_PATHS[0])
        )
    finally:
        try:
            await aiofiles.os.remove(gzip_path)
        except Exception:
            pass


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the OMIM to HGNC symbol mapping from a file into the primary databases.
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
    )

    verbose_print('OMIM gene mapping file loaded from disk, processing annotations...')

    annotations = []

    for _, row in iter_progress(
        mapping_df.iterrows(),
        description='Processing OMIM to HGNC annotations',
        total=len(mapping_df)
    ):
        if pd.isna(row['Gene Symbol']):
            continue

        gene_symbols = row['Gene Symbol'].split('|')
        omim_id = row['Class ID'].split('/')[-1]

        for gene_symbol in gene_symbols:
            annotations.append(Annotation(
                prefixFrom=VOCABULARY_PREFIX_1,
                prefixTo=VOCABULARY_PREFIX_2,
                conceptIdFrom=omim_id,
                conceptIdTo=gene_symbol,
            ))

    verbose_print(f'Saving {len(annotations)} OMIM to HGNC annotations to the graph database...')

    await graph_db.save_annotations(
        annotations=annotations
    )
