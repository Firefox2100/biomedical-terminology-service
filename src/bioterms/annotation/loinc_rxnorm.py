"""LOINC-published Part mappings to RxNorm."""

import os
import httpx
import pandas as pd
from bioterms.annotation.utils import AnnotationSource, assert_pre_requisite
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.vocabulary.loinc import download_vocabulary

ANNOTATION_NAME = 'LOINC Part Mapping to RxNorm'
VOCABULARY_PREFIX_1 = ConceptPrefix.LOINC
VOCABULARY_PREFIX_2 = ConceptPrefix.RXNORM
FILE_PATHS = ['loinc/PartRelatedCodeMapping.csv']
_SOURCE = AnnotationSource('LOINC PartRelatedCodeMapping', ConceptPrefix.LOINC, ConceptPrefix.RXNORM)

async def download_annotation(download_client: httpx.AsyncClient = None):
    await download_vocabulary(download_client)

async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db)
    frame = pd.read_csv(os.path.join(CONFIG.data_dir, FILE_PATHS[0]), dtype=str, keep_default_na=False)
    frame = frame[frame['ExtCodeSystem'] == 'http://www.nlm.nih.gov/research/umls/rxnorm']
    annotations = [
        _SOURCE.create(part, code, AnnotationType.EXACT, {'equivalence': equivalence})
        for part, code, equivalence in frame[['PartNumber', 'ExtCodeId', 'Equivalence']]
        .drop_duplicates().itertuples(index=False, name=None)
        if part and code
    ]
    if annotations:
        await graph_db.save_annotations(annotations)
