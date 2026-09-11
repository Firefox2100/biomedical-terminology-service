import runpy
from pathlib import Path

import pandas as pd
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix


SCRIPT = runpy.run_path(
    str(Path(__file__).parents[2] / 'scripts' / 'load_read_v2_migration.py'),
    run_name='load_read_v2_migration_script',
)


def test_load_current_rows_keeps_latest_active_revision(tmp_path):
    # Same MAPID appears twice: an older active row and a newer inactive one -- the newer
    # (inactive) row must win the dedup, and then get filtered out by the status check.
    # This mirrors what was actually observed in the real ctv3sctmap2 release file.
    path = tmp_path / 'mapping.txt'
    path.write_text(
        'MAPID\tEFFECTIVEDATE\tMAPSTATUS\tVAL\n'
        'm1\t20160323\t1\told\n'
        'm1\t20170328\t0\tnew\n'
        'm2\t20200101\t1\tkeep\n'
    )

    df = SCRIPT['_load_current_rows'](str(path), 'MAPID', 'EFFECTIVEDATE', 'MAPSTATUS')

    assert list(df['MAPID']) == ['m2']
    assert list(df['VAL']) == ['keep']


def test_load_ohdsi_read_code_lookup_filters_to_read_vocabulary(monkeypatch, tmp_path):
    ohdsi_dir = tmp_path / 'ohdsi'
    ohdsi_dir.mkdir()
    (ohdsi_dir / 'CONCEPT.csv').write_text(
        'concept_id\tvocabulary_id\tconcept_code\n'
        '1\tRead\t0....00\n'
        '2\tSNOMED\t14679004\n'
        '3\tRead\t0....11\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    lookup = SCRIPT['_load_ohdsi_read_code_lookup']()

    assert lookup == {'0....00': '1', '0....11': '3'}


def test_read_v2_full_code_concatenates_concept_and_term_code():
    assert SCRIPT['_read_v2_full_code']('0....', '00') == '0....00'


def test_read_v2_full_code_handles_missing_term_code():
    import math
    assert SCRIPT['_read_v2_full_code']('0....', math.nan) == '0....'


def test_load_ohdsi_read_code_lookup_raises_when_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    with pytest.raises(SCRIPT['FilesNotFound']):
        SCRIPT['_load_ohdsi_read_code_lookup']()


def test_build_annotation_tags_raw_metadata_not_a_trust_judgment():
    annotation = SCRIPT['_build_annotation'](
        ConceptPrefix.OHDSI, '123', ConceptPrefix.CTV3, 'X30wJ',
        'v2_to_v3', 'aN1', '1',
    )

    assert annotation.annotation_type == AnnotationType.ANNOTATED_WITH
    assert annotation.properties == {
        'source': 'nhs_read_v2_migration_29.0.0',
        'direction': 'v2_to_v3',
        'mapType': 'aN1',
        'isAssured': '1',
    }


def test_build_annotation_handles_missing_optional_metadata():
    annotation = SCRIPT['_build_annotation'](
        ConceptPrefix.CTV3, 'X30wJ', ConceptPrefix.SNOMED, '138875005',
        'v3_to_snomed_migration', None, None,
    )

    assert annotation.properties['mapType'] == ''
    assert annotation.properties['isAssured'] == ''


def test_build_overlay_annotations_end_to_end_on_small_fixtures(monkeypatch, tmp_path):
    ohdsi_dir = tmp_path / 'ohdsi'
    ohdsi_dir.mkdir()
    (ohdsi_dir / 'CONCEPT.csv').write_text(
        'concept_id\tvocabulary_id\tconcept_code\n'
        '100\tRead\tv2code100\n'
    )

    migration_dir = tmp_path / 'read_v2_migration'
    migration_dir.mkdir()

    # OHDSI's concept_code concatenates the Read v2 concept code + term code:
    # 'v2code1' + '00' == 'v2code100', matching the CONCEPT.csv row above.
    (migration_dir / 'rctctv3map_uk.txt').write_text(
        'MAPID\tV2_CONCEPTID\tV2_TERMID\tCTV3_CONCEPTID\tMAPTYP\tMAPSTATUS\tEFFECTIVEDATE\tIS_ASSURED\n'
        'm1\tv2code1\t00\tctv3codeA\taN1\t1\t20200101\t1\n'
        'm2\tv2code_unmatched\t00\tctv3codeB\taN1\t1\t20200101\t1\n'
    )
    (migration_dir / 'ctv3rctmap_uk.txt').write_text(
        'MAPID\tCTV3_CONCEPTID\tV2_CONCEPTID\tV2_TERMID\tMAPTYP\tMAPSTATUS\tEFFECTIVEDATE\tISASSURED\n'
        'm3\tctv3codeA\tv2code1\t00\tA\t1\t20200101\t1\n'
        'm4\tctv3codeC\t_NONE\t\tN\t1\t20200101\t0\n'
    )
    (migration_dir / 'rcsctmap2_uk.txt').write_text(
        'MapId\tReadCode\tTermCode\tConceptId\tIS_ASSURED\tEffectiveDate\tMapStatus\n'
        'm5\tv2code1\t00\t14679004\t1\t20200101\t1\n'
    )
    (migration_dir / 'ctv3sctmap2_uk.txt').write_text(
        'MAPID\tCTV3_CONCEPTID\tSCT_CONCEPTID\tMAPSTATUS\tEFFECTIVEDATE\tIS_ASSURED\n'
        'm6\tctv3codeA\t138875005\t1\t20200101\t0\n'
    )

    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    annotations = SCRIPT['build_overlay_annotations']()

    by_direction = {a.properties['direction']: a for a in annotations}
    assert len(annotations) == 4  # the _unmatched and _NONE rows must not produce edges

    v2_to_v3 = by_direction['v2_to_v3']
    assert v2_to_v3.prefix_from == ConceptPrefix.OHDSI
    assert v2_to_v3.concept_id_from == '100'
    assert v2_to_v3.prefix_to == ConceptPrefix.CTV3
    assert v2_to_v3.concept_id_to == 'ctv3codeA'

    v3_to_v2 = by_direction['v3_to_v2']
    assert v3_to_v2.prefix_from == ConceptPrefix.CTV3
    assert v3_to_v2.prefix_to == ConceptPrefix.OHDSI
    assert v3_to_v2.concept_id_to == '100'

    v2_to_snomed = by_direction['v2_to_snomed']
    assert v2_to_snomed.prefix_from == ConceptPrefix.OHDSI
    assert v2_to_snomed.concept_id_to == '14679004'

    v3_to_snomed = by_direction['v3_to_snomed_migration']
    assert v3_to_snomed.prefix_from == ConceptPrefix.CTV3
    assert v3_to_snomed.concept_id_to == '138875005'
