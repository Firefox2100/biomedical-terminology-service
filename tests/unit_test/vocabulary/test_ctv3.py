import pandas as pd
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptRelationshipType, ConceptStatus
import bioterms.vocabulary.ctv3 as ctv3


def test_term_selection_status_and_stub_concept():
    assert ctv3._parse_term_label('short', 'medium', 'long') == 'long'
    assert ctv3._parse_term_label('short', 'medium', pd.NA) == 'medium'
    assert ctv3._parse_term_label('short', pd.NA, pd.NA) == 'short'
    assert ctv3._status_from_ctv3_code('C') == ConceptStatus.ACTIVE
    assert ctv3._status_from_ctv3_code('O') == ConceptStatus.ACTIVE
    assert ctv3._status_from_ctv3_code('D') == ConceptStatus.DEPRECATED

    concept = ctv3._make_ctv3_stub_concept({'concept_id': 'X1', 'status': 'D'})
    assert concept.concept_id == 'X1'
    assert concept.label is None
    assert concept.status == ConceptStatus.DEPRECATED


def test_extract_label_and_synonyms_prefers_preferred_term_and_longest_field():
    group = pd.DataFrame([
        {'type': 'P', 'term_30': 'preferred short', 'term_60': 'preferred medium',
         'term_198': 'preferred long'},
        {'type': 'S', 'term_30': 'synonym short', 'term_60': pd.NA, 'term_198': pd.NA},
    ])

    assert ctv3._extract_ctv3_label_and_synonyms(group) == (
        'preferred long', ['synonym short'],
    )


def test_extract_label_falls_back_to_first_synonym_and_rejects_unlabelled_group():
    synonym_only = pd.DataFrame([
        {'type': 'S', 'term_30': 'fallback', 'term_60': pd.NA, 'term_198': pd.NA},
    ])
    unlabelled = pd.DataFrame([
        {'type': 'X', 'term_30': 'ignored', 'term_60': pd.NA, 'term_198': pd.NA},
    ])

    assert ctv3._extract_ctv3_label_and_synonyms(synonym_only) == ('fallback', ['fallback'])
    assert ctv3._extract_ctv3_label_and_synonyms(unlabelled) is None


@pytest.mark.asyncio
async def test_load_ctv3_release_preserves_stubs_hierarchy_and_replacements(monkeypatch, tmp_path):
    release_dir = tmp_path / 'ctv3'
    release_dir.mkdir()
    (release_dir / 'concept.v3').write_text('A|C|\nB|D|\nC|C|\n')
    (release_dir / 'description.v3').write_text('A|T1|P|\nA|T2|S|\n')
    (release_dir / 'term.v3').write_text(
        'T1||Preferred||Preferred long|\nT2||Alias|||\n'
    )
    (release_dir / 'hierarchy.v3').write_text('A|C|\n')
    (release_dir / 'redundancy.map').write_text('A|OLD_A\n')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    captured = {}

    async def capture_concepts(prefix, concepts, **_kwargs):
        captured['concepts'] = concepts

    async def capture_graph(prefix, concepts, vocabulary_graph):
        captured['graph'] = vocabulary_graph

    monkeypatch.setattr(ctv3, 'write_concepts_to_file', capture_concepts)
    monkeypatch.setattr(ctv3, 'write_graph_to_file', capture_graph)

    await ctv3.load_vocabulary_from_file(offline=True, build_search_index=False)

    concepts = {concept.concept_id: concept for concept in captured['concepts']}
    assert concepts['A'].label == 'Preferred long'
    assert concepts['A'].synonyms == ['Alias']
    assert concepts['B'].label is None
    assert concepts['B'].status == ConceptStatus.DEPRECATED
    assert concepts['C'].label is None
    assert captured['graph'].edges['A', 'C']['label'] == ConceptRelationshipType.IS_A
    assert captured['graph'].edges['OLD_A', 'A']['label'] == ConceptRelationshipType.REPLACED_BY
