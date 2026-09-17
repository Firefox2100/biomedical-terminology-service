import os

import httpx
import pandas as pd

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist, download_file, iter_progress
from .utils import AnnotationSource, assert_pre_requisite


ANNOTATION_NAME = 'Ensembl Mapping to Reactome Pathways'
VOCABULARY_PREFIX_1 = ConceptPrefix.ENSEMBL
VOCABULARY_PREFIX_2 = ConceptPrefix.REACTOME
FILE_PATHS = ['ensembl/mapping/reactome.tsv']
_REACTOME = AnnotationSource('Reactome Ensembl2Reactome', ConceptPrefix.REACTOME, ConceptPrefix.ENSEMBL)


async def download_annotation(download_client: httpx.AsyncClient = None):
    if not check_files_exist(FILE_PATHS):
        await download_file(
            url='https://reactome.org/download/current/Ensembl2Reactome.txt',
            file_path=FILE_PATHS[0], download_client=download_client,
        )


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    seen = set()
    for frame in pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]), sep='\t', header=None, dtype=str,
        names=['ensembl_id', 'reactome_id', 'url', 'name', 'evidence', 'species'],
        chunksize=100000,
    ):
        frame = frame[
            (frame['species'] == 'Homo sapiens')
            & frame['ensembl_id'].str.match(r'^ENS[EGTP]')
        ].drop_duplicates(['ensembl_id', 'reactome_id'])
        annotations = []
        for _, row in iter_progress(frame.iterrows(), total=len(frame),
                                    description='Processing Ensembl-Reactome mappings'):
            key = (row['ensembl_id'], row['reactome_id'])
            if key in seen:
                continue
            seen.add(key)
            annotations.append(_REACTOME.create(
                publisher_concept_id=row['reactome_id'], other_concept_id=row['ensembl_id'],
                properties={'evidence': row['evidence'], 'name': row['name']},
            ))
        if annotations:
            await graph_db.save_annotations(annotations)
