import os

import httpx
import pandas as pd

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import check_files_exist, iter_progress
from .utils import AnnotationSource, assert_pre_requisite, download_biomart_tsv


ANNOTATION_NAME = 'Ensembl Gene Mapping to OMIM'
VOCABULARY_PREFIX_1 = ConceptPrefix.ENSEMBL
VOCABULARY_PREFIX_2 = ConceptPrefix.OMIM
FILE_PATHS = ['ensembl/mapping/omim.tsv']
_ENSEMBL_BIOMART = AnnotationSource('Ensembl BioMart', ConceptPrefix.ENSEMBL, ConceptPrefix.OMIM)


async def download_annotation(download_client: httpx.AsyncClient = None):
    if not check_files_exist(FILE_PATHS):
        await download_biomart_tsv(
            ['ensembl_gene_id', 'mim_gene_accession', 'mim_morbid_accession'],
            FILE_PATHS[0], download_client,
        )


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    frame = pd.read_csv(os.path.join(CONFIG.data_dir, FILE_PATHS[0]), sep='\t', dtype=str)
    annotations = []
    seen = set()
    for _, row in iter_progress(frame.iterrows(), total=len(frame),
                                description='Processing Ensembl-OMIM mappings'):
        for column, annotation_type in (
            ('mim_gene_accession', AnnotationType.EXACT),
            ('mim_morbid_accession', AnnotationType.ANNOTATED_WITH),
        ):
            if pd.isna(row[column]):
                continue
            key = (row['ensembl_gene_id'], row[column], column)
            if key in seen:
                continue
            seen.add(key)
            annotations.append(_ENSEMBL_BIOMART.create(
                publisher_concept_id=row['ensembl_gene_id'], other_concept_id=row[column],
                annotation_type=annotation_type, properties={'mapping': column},
            ))
    await graph_db.save_annotations(annotations)
