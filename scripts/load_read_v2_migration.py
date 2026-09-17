#!/usr/bin/env python3
"""Load NHS Read v2 migration mappings as a transient graph overlay.

The overlay links existing OHDSI Read, CTV3, and SNOMED nodes in four directions and tags
each annotation with its NHS source metadata. It is not a registered vocabulary or part of
the normal vocabulary load flow. Only the latest active revision of each mapping is loaded.

Usage:
    python scripts/load_read_v2_migration.py [--skip-download] [--download-only]
"""

from __future__ import annotations

import argparse
import asyncio
import os

import httpx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_rf2, get_trud_release_url, verbose_print
from bioterms.database import get_active_graph_db
from bioterms.model.annotation import Annotation


OVERLAY_SOURCE_TAG = 'nhs_read_v2_migration_29.0.0'

DATA_DIR = 'read_v2_migration'
FILE_PATHS = {
    'v2_to_v3': f'{DATA_DIR}/rctctv3map_uk.txt',
    'v3_to_v2': f'{DATA_DIR}/ctv3rctmap_uk.txt',
    'v2_to_snomed': f'{DATA_DIR}/rcsctmap2_uk.txt',
    'v3_to_snomed': f'{DATA_DIR}/ctv3sctmap2_uk.txt',
}
OHDSI_CONCEPT_FILE = 'ohdsi/CONCEPT.csv'

TRUD_ITEM_ID = 9


async def download_migration_package(download_client: httpx.AsyncClient = None):
    """Download and extract the required NHS migration tables."""
    if check_files_exist(list(FILE_PATHS.values())):
        verbose_print('Read v2 migration files already present, skipping download.')
        return

    if not CONFIG.nhs_trud_api_key:
        raise ValueError('NHS TRUD API key is required to download the data migration package.')

    release_url = await get_trud_release_url(
        f'https://isd.digital.nhs.uk/trud/api/v1/keys/{CONFIG.nhs_trud_api_key}/items/'
        f'{TRUD_ITEM_ID}/releases?latest'
    )

    verbose_print('Downloading NHS Read v2 data migration package...')

    await download_rf2(
        release_url=release_url,
        file_mapping=[
            ('Mapping Tables/Updated/Clinically Assured/rctctv3map_uk_*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS['v2_to_v3'])),
            ('Mapping Tables/Updated/Clinically Assured/ctv3rctmap_uk_*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS['v3_to_v2'])),
            ('Mapping Tables/Updated/Clinically Assured/rcsctmap2_uk_*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS['v2_to_snomed'])),
            ('Mapping Tables/Updated/Clinically Assured/ctv3sctmap2_uk_*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS['v3_to_snomed'])),
        ],
        download_client=download_client,
    )


def _load_current_rows(file_path: str,
                       id_column: str,
                       effective_date_column: str,
                       status_column: str,
                       ) -> pd.DataFrame:
    """Return the latest active revision of each mapping."""
    df = pd.read_csv(file_path, sep='\t', dtype=str)
    df = df.sort_values(by=[id_column, effective_date_column], ascending=[True, False])
    df = df.drop_duplicates(subset=[id_column], keep='first')
    df = df[df[status_column] == '1']
    return df


def _read_v2_full_code(concept_code, term_code) -> str:
    """Build the seven-character Read code used by OHDSI."""
    if pd.isna(term_code):
        term_code = ''
    return f'{concept_code}{term_code}'


def _load_ohdsi_read_code_lookup() -> dict[str, str]:
    """Map Read v2 codes to OHDSI concept IDs."""
    concept_path = os.path.join(CONFIG.data_dir, OHDSI_CONCEPT_FILE)
    if not os.path.exists(concept_path):
        raise FilesNotFound(
            'OHDSI CONCEPT.csv not found; required to map Read v2 codes onto the existing '
            'OHDSI read-sub-vocabulary node IDs.'
        )

    lookup: dict[str, str] = {}
    chunks = pd.read_csv(
        concept_path,
        sep='\t',
        dtype={'concept_id': str, 'vocabulary_id': str, 'concept_code': str},
        usecols=['concept_id', 'vocabulary_id', 'concept_code'],
        chunksize=200000,
    )
    for chunk in chunks:
        read_rows = chunk[chunk['vocabulary_id'] == 'Read']
        for _, row in read_rows.iterrows():
            lookup[row['concept_code']] = row['concept_id']

    return lookup


def _build_annotation(prefix_from: ConceptPrefix,
                      concept_id_from: str,
                      prefix_to: ConceptPrefix,
                      concept_id_to: str,
                      direction: str,
                      map_type: str | None,
                      is_assured: str | None,
                      ) -> Annotation:
    """Build an overlay annotation without asserting identity preservation."""
    return Annotation(
        prefixFrom=prefix_from,
        conceptIdFrom=concept_id_from,
        prefixTo=prefix_to,
        conceptIdTo=concept_id_to,
        annotationType=AnnotationType.ANNOTATED_WITH,
        properties={
            'source': OVERLAY_SOURCE_TAG,
            'direction': direction,
            'mapType': '' if map_type is None else str(map_type),
            'isAssured': '' if is_assured is None else str(is_assured),
        },
    )


def build_overlay_annotations() -> list[Annotation]:
    """Build annotations from all four migration mapping tables."""
    ohdsi_read_lookup = _load_ohdsi_read_code_lookup()
    verbose_print(f'Loaded {len(ohdsi_read_lookup)} OHDSI Read-vocabulary concept codes.')

    annotations: list[Annotation] = []

    # v2 -> v3
    unresolved = 0
    df = _load_current_rows(
        os.path.join(CONFIG.data_dir, FILE_PATHS['v2_to_v3']),
        id_column='MAPID', effective_date_column='EFFECTIVEDATE', status_column='MAPSTATUS',
    )
    for _, row in df.iterrows():
        ohdsi_id = ohdsi_read_lookup.get(_read_v2_full_code(row['V2_CONCEPTID'], row.get('V2_TERMID')))
        if ohdsi_id is None:
            unresolved += 1
            continue
        annotations.append(_build_annotation(
            ConceptPrefix.OHDSI, ohdsi_id, ConceptPrefix.CTV3, row['CTV3_CONCEPTID'],
            'v2_to_v3', row.get('MAPTYP'), row.get('IS_ASSURED'),
        ))
    verbose_print(f'v2->v3: {len(df) - unresolved} resolved, {unresolved} had no matching OHDSI read code.')

    # v3 -> v2 (backward)
    unresolved = 0
    df = _load_current_rows(
        os.path.join(CONFIG.data_dir, FILE_PATHS['v3_to_v2']),
        id_column='MAPID', effective_date_column='EFFECTIVEDATE', status_column='MAPSTATUS',
    )
    df = df[df['V2_CONCEPTID'] != '_NONE']
    for _, row in df.iterrows():
        ohdsi_id = ohdsi_read_lookup.get(_read_v2_full_code(row['V2_CONCEPTID'], row.get('V2_TERMID')))
        if ohdsi_id is None:
            unresolved += 1
            continue
        annotations.append(_build_annotation(
            ConceptPrefix.CTV3, row['CTV3_CONCEPTID'], ConceptPrefix.OHDSI, ohdsi_id,
            'v3_to_v2', row.get('MAPTYP'), row.get('ISASSURED'),
        ))
    verbose_print(f'v3->v2: {len(df) - unresolved} resolved, {unresolved} had no matching OHDSI read code.')

    # v2 -> SNOMED
    unresolved = 0
    df = _load_current_rows(
        os.path.join(CONFIG.data_dir, FILE_PATHS['v2_to_snomed']),
        id_column='MapId', effective_date_column='EffectiveDate', status_column='MapStatus',
    )
    for _, row in df.iterrows():
        ohdsi_id = ohdsi_read_lookup.get(_read_v2_full_code(row['ReadCode'], row.get('TermCode')))
        if ohdsi_id is None:
            unresolved += 1
            continue
        annotations.append(_build_annotation(
            ConceptPrefix.OHDSI, ohdsi_id, ConceptPrefix.SNOMED, row['ConceptId'],
            'v2_to_snomed', None, row.get('IS_ASSURED'),
        ))
    verbose_print(f'v2->snomed: {len(df) - unresolved} resolved, {unresolved} had no matching OHDSI read code.')

    # v3 -> SNOMED (redundant with native ctv3<->snomed edges by design -- see module docstring)
    df = _load_current_rows(
        os.path.join(CONFIG.data_dir, FILE_PATHS['v3_to_snomed']),
        id_column='MAPID', effective_date_column='EFFECTIVEDATE', status_column='MAPSTATUS',
    )
    for _, row in df.iterrows():
        annotations.append(_build_annotation(
            ConceptPrefix.CTV3, row['CTV3_CONCEPTID'], ConceptPrefix.SNOMED, row['SCT_CONCEPTID'],
            'v3_to_snomed_migration', None, row.get('IS_ASSURED'),
        ))
    verbose_print(f'v3->snomed: {len(df)} rows loaded (compare against native ctv3<->snomed edges).')

    return annotations


async def load_migration_overlay():
    """
    Build and save the Read v2 migration overlay annotations into the active graph database.
    """
    if not check_files_exist(list(FILE_PATHS.values())):
        raise FilesNotFound('Read v2 migration files not found; run the download step first.')

    annotations = build_overlay_annotations()
    verbose_print(f'Saving {len(annotations)} Read v2 migration overlay annotations...')

    graph_db = get_active_graph_db()
    await graph_db.save_annotations(annotations)

    verbose_print('Read v2 migration overlay loaded.')


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--skip-download', action='store_true',
                        help='Skip the download step (files must already be present).')
    parser.add_argument('--download-only', action='store_true',
                        help='Only download the migration package, do not load it.')
    args = parser.parse_args()

    if not args.skip_download:
        await download_migration_package()

    if not args.download_only:
        await load_migration_overlay()


if __name__ == '__main__':
    asyncio.run(main())
