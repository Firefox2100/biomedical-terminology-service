#!/usr/bin/env python3
"""Download and load the NHS Read v2 data migration package as a transient overlay.

Read v2 itself is a retired vocabulary this project cannot obtain or license directly
(see docs/CLAUDE.md's provenance notes) -- OHDSI's own 'read' sub-vocabulary is the only
Read v2 content already in this graph, and its nodes have no path at all to CTV3 or SNOMED
(degree exactly 1, connected only to their own OHDSI standard-concept mapping). NHS's data
migration package (TRUD item 9) is still downloadable, though, and contains "Clinically
Assured" mapping tables between Read v2, CTV3, and SNOMED CT that were produced specifically
to support migrating legacy Read v2 systems onto SNOMED CT.

This script loads four of those mapping tables as ANNOTATED_WITH annotations directly onto
the existing OHDSI 'read'/CTV3/SNOMED nodes, each tagged with
properties={'source': 'nhs_read_v2_migration_29.0.0', ...raw NHS map metadata...} so they
are trivially identifiable and strippable later. This is NOT a canonical bts vocabulary:
it is not registered in bioterms.vocabulary.utils.ALL_VOCABULARIES, does not get its own
ConceptPrefix, and is not part of the normal `bioterms-cli vocabulary` load flow -- run this
script directly instead. It exists to give the research project
(biomedical-graph-research) a structural signal for the previously-undetectable
OHDSI-read/CTV3/SNOMED shared-lineage risk; production bts does not depend on it.

Four mapping directions are loaded:
  - v2 -> v3   (rctctv3map_uk_*.txt)
  - v3 -> v2   (ctv3rctmap_uk_*.txt, backward mapping)
  - v2 -> SNOMED (rcsctmap2_uk_*.txt)
  - v3 -> SNOMED (ctv3sctmap2_uk_*.txt) -- redundant with the *native* ctv3<->snomed
    annotated_with edges snomed.py already loads from SNOMED's own RF2 release, kept
    deliberately: comparing the two is itself a research signal for whether they share
    origin, which is the open question this overlay exists to make inspectable.

Read v2 mapping-file rows are versioned like RF2 (a MAPID/MapId repeats across historical
revisions, distinguished by EFFECTIVEDATE/EffectiveDate); only the latest revision per id,
with MAPSTATUS/MapStatus == '1' (currently active), is loaded -- confirmed against real
release data that an inactivated row is not always the same as the first one encountered
for a given id.

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
    """
    Download the NHS data migration package (TRUD item 9) and extract the four mapping
    tables this overlay needs, under data/read_v2_migration/.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
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
    """
    Read an NHS mapping table and keep only the latest revision (by effective date) of each
    mapping id, filtered to currently-active rows (status == '1'). Mirrors
    bioterms.etc.utils.rf2_dataframe_deduplicate's convention, applied to these
    differently-named/differently-cased NHS columns rather than RF2's own.
    :param file_path: Path to the mapping table file.
    :param id_column: Name of the column identifying one logical mapping across revisions.
    :param effective_date_column: Name of the column giving each revision's effective date.
    :param status_column: Name of the column whose '1' value means "currently active".
    :return: The deduplicated, active-only dataframe.
    """
    df = pd.read_csv(file_path, sep='\t', dtype=str)
    df = df.sort_values(by=[id_column, effective_date_column], ascending=[True, False])
    df = df.drop_duplicates(subset=[id_column], keep='first')
    df = df[df[status_column] == '1']
    return df


def _read_v2_full_code(concept_code, term_code) -> str:
    """
    Build the full 7-character Read v2 term code (5-char concept code + 2-char term code)
    OHDSI uses as its 'Read' vocabulary_id concept_code. Confirmed against real data: NHS's
    migration tables carry the Read v2 concept code and term code as SEPARATE columns
    (e.g. V2_CONCEPTID='0....', V2_TERMID='00'), but OHDSI's CONCEPT.csv concatenates them
    into one 7-char code ('0....00') -- joining on V2_CONCEPTID alone silently matches
    almost nothing (empirically: 1 of ~160k rows in the real release).
    :param concept_code: The 5-character Read v2 concept code column value.
    :param term_code: The 2-character Read v2 term code column value (may be NaN/None).
    :return: The concatenated 7-character code.
    """
    if pd.isna(term_code):
        term_code = ''
    return f'{concept_code}{term_code}'


def _load_ohdsi_read_code_lookup() -> dict[str, str]:
    """
    Build a Read v2 code -> OHDSI internal concept_id lookup, restricted to OHDSI's own
    'Read' vocabulary_id rows (its 'read' sub-vocabulary). This is the join needed to attach
    the migration package's raw Read v2 codes onto the actual OHDSI nodes already in the
    graph -- bts's OhdsiConcept nodes are keyed by OHDSI's own concept_id, not by the Read
    code itself.
    :return: A dict mapping Read v2 code (OHDSI's concept_code) to OHDSI's concept_id.
    :raises FilesNotFound: If OHDSI's CONCEPT.csv is not present.
    """
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
    """
    Build one overlay Annotation, tagged with the raw NHS map metadata rather than any
    identity-preservation judgment -- whether a given direction/map_type combination should
    be treated as identity-preserving is a relations.yaml decision for the consuming
    project, not something asserted here.
    """
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
    """
    Read all four mapping tables and the OHDSI Read v2 code lookup, and build the full list
    of overlay Annotation instances to save.
    :return: The list of built Annotation instances.
    """
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
