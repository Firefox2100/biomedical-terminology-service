import json
import sys
import types
from pathlib import Path
import pytest


RERANKER_DIR = Path(__file__).parents[2] / 'scripts' / 'reranker'
sys.path.insert(0, str(RERANKER_DIR))

from audit_training_data import audit
from audit_mapping_conflicts import classify_mapping_conflict
from audit_retrieval import _rrf_rank, audit as audit_retrieval
from probe_live_retrieval import _fused_gold_rank, _unique_ids
from concept_rendering import RenderVariant
from build_training_data import QueryUnit, _MiningOutput, _mine_negatives
from mine_cross_vocab_positives import (
    _direction_key, _offer_bounded_mapping, _order_and_cap_units,
    _retrieval_miss_record,
)
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.model.concept import EmbeddingItem
from train_reranker import (
    DEFAULT_OUTPUT_DIR,
    _parse_args,
    _ambiguous_query_keys,
    _build_candidate_sets,
    _deduplicate_groups,
    _filter_training_quality,
    _normalise_query,
    _load_groups,
    _resolve_train_paths,
    _run_candidate_set_evaluation,
    _stratified_eval_sample,
    _training_negatives,
    _flatten_to_rows,
)
from query_quality import contextless_query_reason
from bioterms.etc.consts import DEFAULT_RERANKER_MODEL


def test_default_training_bundle_is_the_local_service_bundle(monkeypatch):
    monkeypatch.setattr(sys, 'argv', [
        'train_reranker.py', '--train-data', 'dummy.jsonl',
        '--concept-store-dir', 'dummy-concepts',
    ])

    args = _parse_args()

    assert Path(args.output_dir) == DEFAULT_OUTPUT_DIR
    assert DEFAULT_OUTPUT_DIR / 'final' == DEFAULT_RERANKER_MODEL


def test_retrieval_audit_counts_gold_absence_and_fusion_truncation(tmp_path):
    path = tmp_path / 'cross-vocab.jsonl'
    found = {
        'prefix': 'mondo', 'query_kind': 'cross_vocab_exact',
        'query_id': 'source:1->mondo:gold', 'query': 'long disease name',
        'gold_concept_id': 'gold',
        'gold_retrieval': {'ranks': {'lexical': 10}},
        'candidate_pool': [
            {'concept_id': 'negative', 'ranks': {'lexical': 1, 'alias_embedding': 1}},
        ],
    }
    absent = {
        **found, 'query_id': 'source:2->mondo:gold', 'gold_retrieval': None,
    }
    path.write_text(json.dumps(found) + '\n' + json.dumps(absent) + '\n')

    report, review = audit_retrieval([path], arm_depth=5)

    assert _rrf_rank(found, arm_depth=5, rrf_k=60) is None
    assert _rrf_rank(found, arm_depth=10, rrf_k=60) == 2
    assert report['global']['rows'] == 2
    assert report['global']['gold_absent'] == 1
    assert report['global']['lexical@10'] == 1
    assert report['global'].get('rrf@50', 0) == 0
    assert len(review) == 2
    assert review[0]['rrf_rank'] is None


def test_retrieval_audit_samples_without_loading_other_rows(tmp_path):
    path = tmp_path / 'aliases.jsonl'
    group = {
        'prefix': 'hpo', 'query_kind': 'alias', 'query_id': 'q1', 'query': 'sepsis',
        'gold_concept_id': 'g1',
        'gold_retrieval_evidence': {'ranks': {'lexical': 1}},
        'candidate_pool': [],
    }
    path.write_text(json.dumps(group) + '\nnot valid json\n')

    report, review = audit_retrieval([path], sample_modulus=2)

    assert report['source_rows_seen'] == 2
    assert report['global']['rows'] == 1
    assert report['global']['rrf@10'] == 1
    assert review == []


def test_cross_vocabulary_retrieval_miss_keeps_gold_query_provenance():
    from types import SimpleNamespace

    unit = QueryUnit(
        prefix=ConceptPrefix.MONDO,
        concept_id='0001',
        item=SimpleNamespace(item_id='source-alias', text='disease phrase'),
    )

    assert _retrieval_miss_record(unit, ConceptPrefix.OMIM, 50) == {
        'source_prefix': 'omim',
        'prefix': 'mondo',
        'query_id': 'omim:source-alias->mondo:0001',
        'query': 'disease phrase',
        'gold_concept_id': '0001',
        'reason': 'gold_not_retrieved',
        'per_arm_depth': 50,
    }


@pytest.mark.asyncio
async def test_mining_can_retain_mapped_recall_as_separate_evidence():
    class EmptyDocDB:
        async def lexical_search(self, *_args, **_kwargs):
            return []

    class EmptyVectorDB:
        async def search_items(self, *_args, **_kwargs):
            return []

    unit = QueryUnit(
        prefix=ConceptPrefix.MONDO,
        concept_id='gold',
        item=types.SimpleNamespace(text='source alias'),
    )
    negatives, pool, _merges, _rejected, gold = await _mine_negatives(
        EmptyDocDB(), EmptyVectorDB(), object(), unit,
        negatives_per_query=1, candidate_pool=50,
        query_vector=[0.0],
        additional_ranked_hits={'exact_mapping': ['gold', 'negative']},
    )

    assert gold['sources'] == ['exact_mapping']
    assert gold['ranks'] == {'exact_mapping': 1}
    assert negatives[0]['concept_id'] == 'negative'
    assert pool[0]['sources'] == ['exact_mapping']


def test_live_probe_truncates_items_before_concept_deduplication():
    items = [('other', 'alias 1', 1.0), ('other', 'alias 2', 0.9),
             ('gold', 'gold alias', 0.8)]

    assert _unique_ids(items[:2]) == ['other']
    assert _unique_ids(items[:3]) == ['other', 'gold']
    assert _fused_gold_rank('gold', {'alias_embedding': ['other']}, 2, 60) is None
    assert _fused_gold_rank('gold', {'alias_embedding': ['other', 'gold']}, 3, 60) == 2


def test_per_vocabulary_mining_output_routes_rows(tmp_path):
    with _MiningOutput(None, str(tmp_path)) as output:
        output.write(ConceptPrefix.HPO, _group('hpo', 'h1', 'g1', 'query'))
        output.write(ConceptPrefix.SNOMED, _group('snomed', 's1', 'g2', 'query'))

    assert (tmp_path / 'aliases.hpo.jsonl').exists()
    assert (tmp_path / 'aliases.snomed.jsonl').exists()
    assert json.loads((tmp_path / 'aliases.hpo.jsonl').read_text())['prefix'] == 'hpo'


def test_training_discovers_shards_and_applies_vocabulary_masks(tmp_path):
    hpo_path = tmp_path / 'aliases.hpo.jsonl'
    snomed_path = tmp_path / 'cross-vocab.snomed.jsonl'
    hpo_path.write_text(json.dumps(_group('hpo', 'h1', 'g1', 'query')) + '\n')
    snomed_path.write_text(json.dumps(_group('snomed', 's1', 'g2', 'query')) + '\n')

    paths = _resolve_train_paths(None, [str(tmp_path)])
    groups = _load_groups(paths, include_vocabularies={'hpo', 'snomed'}, exclude_vocabularies={'snomed'})

    assert paths == [hpo_path.resolve(), snomed_path.resolve()]
    assert [group['prefix'] for group in groups] == ['hpo']


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


def test_conservative_query_quality_filters_only_training_groups(tmp_path):
    groups = [
        _group('ncit', 'q1', 'g1', 'Yes'),
        _group('ncit', 'q2', 'g2', 'B'),
        _group('hgnc', 'q3', 'g3', 'LGCR'),
        _group('hgnc', 'q4', 'g4', 'LGCR'),
        _group('gene', 'q5', 'g5', 'BRCA1'),
    ]
    review_path = tmp_path / 'quality.jsonl'
    kept, counts = _filter_training_quality(
        groups, _ambiguous_query_keys(groups), True, True, review_path,
    )

    assert [group['query'] for group in kept] == ['BRCA1']
    assert counts['total_excluded'] == 4
    assert counts['multiple_gold_concepts'] == 2
    assert counts['context_dependent_response'] == 1
    assert counts['fewer_than_three_alphanumeric_characters'] == 1
    assert len(review_path.read_text().splitlines()) == 4
    assert contextless_query_reason('CD4') is None


def test_cross_vocab_conflict_audit_flags_other_exact_candidate_without_rejecting_gold():
    group = {
        **_group('ncit', 'cross-1', 'disease', 'Dup15q'),
        'query_kind': 'cross_vocab_exact',
        'candidate_pool': [{'concept_id': 'allele'}],
    }
    concepts = {
        ('ncit', 'disease'): {
            'label': 'Chromosome 15q11-q13 Duplication Syndrome', 'synonyms': [],
        },
        ('ncit', 'allele'): {
            'label': 'GREM1 wt Allele', 'synonyms': ['DUP15q'],
        },
    }

    record = classify_mapping_conflict(group, concepts)

    assert record['reasons'] == ['other_exact_match_but_not_gold']
    assert record['gold_concept_id'] == 'disease'
    assert record['exact_candidates'][0]['concept_id'] == 'allele'


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


def test_candidate_evaluation_exports_predictions_and_exactness(monkeypatch, tmp_path):
    fake_pylate = types.ModuleType('pylate')
    fake_pylate.rank = types.SimpleNamespace(
        rerank=lambda **_kwargs: [[
            {'id': 'negative', 'score': 2.0},
            {'id': 'gold', 'score': 1.0},
        ]],
    )
    monkeypatch.setitem(sys.modules, 'pylate', fake_pylate)

    class FakeModel:
        def encode(self, values, **_kwargs):
            return values

    path = tmp_path / 'predictions.jsonl'
    result = _run_candidate_set_evaluation(FakeModel(), [{
        'prefix': 'ncit', 'query': 'query', 'query_kind': 'alias',
        'gold_concept_id': 'gold',
        'candidate_ids': ['gold', 'negative'],
        'candidate_texts': ['gold text', 'negative text'],
        'exact_alias_ids': ['gold'],
    }], batch_size=1, prediction_output=path)

    assert result['global']['accuracy_at_1'] == 0.0
    assert result['hybrid_exact_alias']['global']['accuracy_at_1'] == 1.0
    assert result['by_exactness']['unique_exact_match']['n'] == 1
    row = json.loads(path.read_text())
    assert row['gold_rank'] == 2
    assert row['top_candidates'][0]['concept_id'] == 'negative'
    assert not path.with_name(path.name + '.partial').exists()


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


def test_model_stratified_sampling_keeps_hard_middle_and_tail():
    group = _group('snomed', 'q1', 'gold', 'query')
    group['candidate_pool'] = [
        {
            'concept_id': f'n{index}', 'role': 'negative',
            'ranking_scores': {'student': float(10 - index)},
        }
        for index in range(10)
    ]

    selected = _training_negatives(group, 4, 'model_stratified', 'student', 42, 0)
    selected_ids = {candidate['concept_id'] for candidate in selected}

    assert {'n0', 'n1'} <= selected_ids
    assert any(candidate_id in selected_ids for candidate_id in {'n5', 'n6', 'n7'})
    assert any(candidate_id in selected_ids for candidate_id in {'n7', 'n8', 'n9'})


def test_distillation_rows_reuse_stored_scores_without_remining():
    group = _group('snomed', 'q1', 'gold', 'query')
    group['candidate_pool'] = [
        {'concept_id': 'gold', 'role': 'gold', 'ranking_scores': {'teacher': 9.0}},
        {'concept_id': 'negative', 'role': 'negative', 'ranking_scores': {'teacher': 2.0}},
    ]
    store = {
        ('snomed', 'gold'): {'label': 'Gold', 'synonyms': [], 'definition': None},
        ('snomed', 'negative'): {'label': 'Negative', 'synonyms': [], 'definition': None},
    }

    rows, stats = _flatten_to_rows(
        [group], store, negatives_per_query=1, seed=42, max_aliases=6,
        preferred_label_keep_probability=1.0, negative_sampling='model_stratified',
        ranking_score_key='teacher', training_objective='distillation',
        distillation_temperature=2.0,
    )

    assert stats['skipped_insufficient_resolvable_negatives'] == 0
    assert rows[0]['scores'] == [4.5, 1.0]
    assert rows[0]['documents'][0].startswith('Gold')
