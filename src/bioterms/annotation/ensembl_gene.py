import os

import httpx
import pandas as pd

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import check_files_exist, download_file, iter_progress
from bioterms.model.annotation import Annotation
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'Ensembl Gene Mapping to HGNC Gene Symbol'
VOCABULARY_PREFIX_1 = ConceptPrefix.ENSEMBL
VOCABULARY_PREFIX_2 = ConceptPrefix.HGNC_SYMBOL
FILE_PATHS = ['ensembl/mapping/hgnc.tsv']


async def download_annotation(download_client: httpx.AsyncClient = None):
    if check_files_exist(FILE_PATHS):
        return
    await download_file(
        url='https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt',
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    frame = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]), sep='\t', dtype=str,
        usecols=['ensembl_gene_id', 'symbol'],
    ).dropna()
    frame['ensembl_gene_id'] = frame['ensembl_gene_id'].str.split('|')
    frame = frame.explode('ensembl_gene_id').drop_duplicates()
    annotations = [
        Annotation(
            prefixFrom=VOCABULARY_PREFIX_1, prefixTo=VOCABULARY_PREFIX_2,
            conceptIdFrom=row['ensembl_gene_id'], conceptIdTo=row['symbol'],
            annotationType=AnnotationType.HAS_SYMBOL,
        )
        for _, row in iter_progress(frame.iterrows(), total=len(frame),
                                    description='Processing Ensembl-HGNC mappings')
    ]
    await graph_db.save_annotations(annotations)
