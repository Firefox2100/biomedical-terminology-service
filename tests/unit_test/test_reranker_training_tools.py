import json
import sys
from pathlib import Path


RERANKER_DIR = Path(__file__).parents[2] / 'scripts' / 'reranker'
sys.path.insert(0, str(RERANKER_DIR))

from audit_training_data import audit
from concept_rendering import RenderVariant
from build_training_data import QueryUnit
from mine_cross_vocab_positives import _direction_key, _offer_bounded_mapping, _order_and_cap_units
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.model.concept import EmbeddingItem
from train_reranker import (
    _ambiguous_query_keys,
    _build_candidate_sets,
    _deduplicate_groups,
    _normalise_query,
    _stratified_eval_sample,
)


def _group(prefix: str, query_id: str, gold: str, query: str) -> dict:
    return {
        'prefix': prefix,
        'query_id': query_id,
        'gold_concept_id': gold,
        'query': query,
        'query_kind': 'alias',
        'negatives': [{'concept_id': 'negative'}],
    }


def test_ambiguity_normalises_case_and_whitespace():
    groups = [
        _group('snomed', 'q1', 'gold-1', '  Shared   Alias '),
        _group('snomed', 'q2', 'gold-2', 'shared alias'),
        _group('hpo', 'q3', 'gold-3', 'shared alias'),
    ]

    assert _normalise_query('  Shared   Alias ') == 'shared alias'
    assert _ambiguous_query_keys(groups) == {('snomed', 'shared alias')}


def test_deduplicate_groups_preserves_distinct_golds():
    duplicate = _group('snomed', 'q1', 'gold-1', 'Alias')
    distinct_gold = _group('snomed', 'q1', 'gold-2', 'Alias')

    result, removed = _deduplicate_groups([duplicate, dict(duplicate), distinct_gold])

    assert removed == 1
    assert [group['gold_concept_id'] for group in result] == ['gold-1', 'gold-2']


def test_stratified_eval_sample_is_deterministic_and_balanced():
    groups = [
        _group(prefix, f'{prefix}-{index}', f'gold-{index}', f'query-{index}')
        for prefix in ('a', 'b', 'c')
        for index in range(10)
    ]

    first = _stratified_eval_sample(groups, target_size=12, seed=7)
    second = _stratified_eval_sample(groups, target_size=12, seed=7)

    assert [group['query_id'] for group in first] == [group['query_id'] for group in second]
    assert {prefix: sum(group['prefix'] == prefix for group in first) for prefix in ('a', 'b', 'c')} == {
        'a': 4, 'b': 4, 'c': 4,
    }


def test_audit_reports_duplicates_and_ambiguous_queries(tmp_path):
    path = tmp_path / 'groups.jsonl'
    groups = [
        _group('snomed', 'q1', 'gold-1', 'Shared Alias'),
        _group('snomed', 'q1', 'gold-1', 'Shared Alias'),
        _group('snomed', 'q2', 'gold-2', ' shared  alias '),
    ]
    path.write_text(''.join(json.dumps(group) + '\n' for group in groups), encoding='utf-8')

    report, review = audit([path])

    assert report['rows'] == 3
    assert report['exact_duplicate_rows'] == 1
    assert report['reused_query_id_keys'] == 1
    assert report['rows_beyond_first_per_query_id'] == 1
    assert report['ambiguous_query_keys'] == 1
    assert review[0]['gold_concept_ids'] == ['gold-1', 'gold-2']


def test_candidate_sets_record_only_exact_normalised_alias_matches():
    groups = [{
        **_group('snomed', 'q1', 'gold', '  Shared   Alias '),
        'negatives': [{'concept_id': 'negative-1'}, {'concept_id': 'negative-2'}],
    }]
    store = {
        ('snomed', 'gold'): {
            'label': 'Gold label', 'synonyms': ['shared alias'], 'definition': None,
        },
        ('snomed', 'negative-1'): {
            'label': 'Contains shared alias but is not exact', 'synonyms': [], 'definition': None,
        },
        ('snomed', 'negative-2'): {
            'label': 'Other', 'synonyms': ['SHARED ALIAS'], 'definition': None,
        },
    }

    examples = _build_candidate_sets(groups, store, RenderVariant.LABEL_ALIASES, 6)

    assert examples[0]['exact_alias_ids'] == ['gold', 'negative-2']


def test_relationship_units_have_stable_order_and_cap():
    def unit(concept_id: str, item_id: str) -> tuple[QueryUnit, str]:
        return (
            QueryUnit(
                prefix=ConceptPrefix.NCIT,
                concept_id=concept_id,
                item=EmbeddingItem(
                    item_id=item_id, concept_id='source', kind=EmbeddingKind.ALIAS, text=item_id,
                ),
            ),
            ConceptPrefix.MONDO.value,
        )

    forward = [unit('gold-2', 'alias-2'), unit('gold-1', 'alias-1'), unit('gold-3', 'alias-3')]
    reverse = list(reversed(forward))

    first = _order_and_cap_units(forward, ConceptPrefix.NCIT, ConceptPrefix.MONDO, 2)
    second = _order_and_cap_units(reverse, ConceptPrefix.NCIT, ConceptPrefix.MONDO, 2)

    assert [(u.concept_id, u.item.item_id) for u, _ in first] == [
        (u.concept_id, u.item.item_id) for u, _ in second
    ]
    assert len(first) == 2
    assert _direction_key(ConceptPrefix.NCIT, ConceptPrefix.MONDO) == 'mondo->ncit'


def test_bounded_relationship_mapping_sample_is_order_independent():
    mappings = [(f'source-{index}', f'gold-{index}') for index in range(20)]

    def sample(values):
        heap, seen = [], set()
        for mapping in values:
            _offer_bounded_mapping(
                heap, seen, mapping, ConceptPrefix.OHDSI, ConceptPrefix.SNOMED, 5,
            )
        return seen

    assert sample(mappings) == sample(reversed(mappings))
    assert len(sample(mappings)) == 5
