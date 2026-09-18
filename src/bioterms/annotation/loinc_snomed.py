"""LOINC-published mappings from constituent Parts to SNOMED CT concepts."""

import os

import httpx
import pandas as pd

from bioterms.annotation.utils import AnnotationSource, assert_pre_requisite
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import verbose_print
from bioterms.vocabulary.loinc import download_vocabulary


ANNOTATION_NAME = 'LOINC Part Mapping to SNOMED CT'
VOCABULARY_PREFIX_1 = ConceptPrefix.LOINC
VOCABULARY_PREFIX_2 = ConceptPrefix.SNOMED
FILE_PATHS = ['loinc/PartRelatedCodeMapping.csv']
_SOURCE = AnnotationSource(
    'LOINC PartRelatedCodeMapping', ConceptPrefix.LOINC, ConceptPrefix.SNOMED,
)
async def download_annotation(download_client: httpx.AsyncClient = None):
    """Reuse the official authenticated LOINC release containing the mapping."""
    await download_vocabulary(download_client=download_client)


def _annotation_type(map_type: str) -> AnnotationType:
    """Interpret explicit publisher labels and retain unrecognised labels as related."""
    value = map_type.strip().lower()
    if value in {'exact', 'equivalent'}:
        return AnnotationType.EXACT
    if value in {'broader', 'wider'}:
        return AnnotationType.BROAD
    if value == 'narrower':
        return AnnotationType.NARROW
    return AnnotationType.RELATED


def _is_snomed_system(value: str) -> bool:
    value = value.strip()
    return value.lower().startswith(('http://snomed.info/sct', 'https://snomed.info/sct')) \
        or value.upper() in {'SNOMEDCT', 'SNOMED CT'}


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Load the SNOMED subset of LOINC's official external Part mappings."""
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    frame = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]), dtype=str, keep_default_na=False,
    )
    frame = frame[frame['ExtCodeSystem'].map(_is_snomed_system)]
    annotations = []
    seen = set()
    for _, row in frame.iterrows():
        part_id = row['PartNumber'].strip()
        snomed_id = row['ExtCodeId'].strip()
        map_type = row.get('Equivalence', '').strip()
        key = (part_id, snomed_id, map_type)
        if not part_id or not snomed_id or key in seen:
            continue
        seen.add(key)
        properties = {
            key: value.strip()
            for key, value in {
                'mapType': map_type,
                'contentOrigin': row.get('ContentOrigin', ''),
                'systemVersion': row.get('ExtCodeSystemVersion', ''),
            }.items()
            if value.strip()
        }
        annotations.append(_SOURCE.create(
            publisher_concept_id=part_id,
            other_concept_id=snomed_id,
            annotation_type=_annotation_type(map_type),
            properties=properties,
        ))
    if annotations:
        await graph_db.save_annotations(annotations)
    verbose_print(f'Loaded {len(annotations)} LOINC-published SNOMED CT Part mappings.')
