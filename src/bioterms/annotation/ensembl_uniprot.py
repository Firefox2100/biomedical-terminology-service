import os

import httpx
import pandas as pd

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import check_files_exist, iter_progress
from .utils import AnnotationSource, assert_pre_requisite, download_current_ensembl_tsv


ANNOTATION_NAME = 'Ensembl Protein Mapping to UniProtKB'
VOCABULARY_PREFIX_1 = ConceptPrefix.ENSEMBL
VOCABULARY_PREFIX_2 = ConceptPrefix.UNIPROT
FILE_PATHS = ['ensembl/mapping/uniprot.tsv']
_ENSEMBL_UNIPROT = AnnotationSource('Ensembl UniProt TSV', ConceptPrefix.ENSEMBL, ConceptPrefix.UNIPROT)


async def download_annotation(download_client: httpx.AsyncClient = None):
    if not check_files_exist(FILE_PATHS):
        await download_current_ensembl_tsv('uniprot', FILE_PATHS[0], download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    frame = pd.read_csv(os.path.join(CONFIG.data_dir, FILE_PATHS[0]), sep='\t', dtype=str)
    frame = frame[
        frame['db_name'].isin(['Uniprot/SWISSPROT', 'Uniprot/SPTREMBL'])
        & frame['protein_stable_id'].notna() & frame['xref'].notna()
    ].drop_duplicates(['protein_stable_id', 'xref'])
    annotations = []
    for _, row in iter_progress(frame.iterrows(), total=len(frame),
                                description='Processing Ensembl-UniProt mappings'):
        properties = {'externalDatabase': row['db_name'], 'method': row['info_type']}
        for field in ('source_identity', 'xref_identity', 'linkage_type'):
            if pd.notna(row[field]) and row[field] != '-':
                properties[field] = row[field]
        annotations.append(_ENSEMBL_UNIPROT.create(
            publisher_concept_id=row['protein_stable_id'], other_concept_id=row['xref'],
            annotation_type=AnnotationType.EXACT, properties=properties,
        ))
    await graph_db.save_annotations(annotations)
