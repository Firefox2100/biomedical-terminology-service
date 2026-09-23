#!/usr/bin/env python3
"""Train and evaluate a ColBERT-style reranker from mined query groups.

This standalone script depends on the ML stack but not the service or its databases.
"""
import argparse
import hashlib
import json
import os
import random
from collections import Counter, defaultdict
from pathlib import Path

from concept_rendering import ALL_VARIANTS, RenderVariant, render_concept
from query_quality import normalise_query, training_quality_reasons


# Below this fraction of loaded groups resolving a gold concept, --concept-store-dir almost
# certainly doesn't match --train-data -- fail fast rather than train on a near-empty dataset.
MIN_CONCEPT_RESOLUTION_FRACTION = 0.5
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / 'runs' / 'production'


def _normalise_query(text: str) -> str:
    """Normalise query text for ambiguity/deduplication checks (not model input)."""
    return normalise_query(text)


def _filter_training_quality(groups: list[dict],
                             ambiguous_keys: set[tuple[str, str]],
                             drop_ambiguous: bool,
                             drop_contextless: bool,
                             review_output: Path | None = None,
                             review_limit: int = 1000,
                             ) -> tuple[list[dict], dict[str, int]]:
    """Apply optional quality policy after the concept split, leaving evaluation unchanged."""
    kept = []
    reasons_count: Counter[str] = Counter()
    review = []
    for group in groups:
        reasons = training_quality_reasons(
            group, ambiguous_keys, drop_ambiguous, drop_contextless,
        )
        if not reasons:
            kept.append(group)
            continue
        reasons_count.update(reasons)
        reasons_count['total_excluded'] += 1
        reasons_count[f'excluded_{group["prefix"]}'] += 1
        if review_output is not None and len(review) < review_limit:
            review.append({
                'prefix': group['prefix'], 'query': group['query'],
                'query_kind': group.get('query_kind'),
                'gold_concept_id': group['gold_concept_id'],
                'reasons': reasons,
            })
    if review_output is not None and os.environ.get('RANK', '0') == '0':
        review_output.parent.mkdir(parents=True, exist_ok=True)
        with review_output.open('w', encoding='utf-8') as handle:
            for item in review:
                handle.write(json.dumps(item, ensure_ascii=False) + '\n')
    return kept, dict(reasons_count)


def _deduplicate_groups(groups: list[dict]) -> tuple[list[dict], int]:
    """Remove byte-logical duplicate query groups while preserving first-seen order."""
    deduplicated = []
    seen: set[tuple] = set()
    removed = 0
    for group in groups:
        key = (
            group['prefix'], group['query_id'], group['gold_concept_id'],
            _normalise_query(group['query']),
        )
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        deduplicated.append(group)
    return deduplicated, removed


def _ambiguous_query_keys(groups: list[dict]) -> set[tuple[str, str]]:
    """Return (prefix, normalised query) keys associated with more than one gold concept."""
    golds_by_query: dict[tuple[str, str], set[str]] = defaultdict(set)
    for group in groups:
        golds_by_query[(group['prefix'], _normalise_query(group['query']))].add(
            group['gold_concept_id']
        )
    return {key for key, golds in golds_by_query.items() if len(golds) > 1}


def _resolve_train_paths(explicit_paths: list[str] | None,
                         directories: list[str] | None,
                         ) -> list[Path]:
    """Resolve explicit shards plus dynamically discovered per-vocabulary shards."""
    paths = [Path(path) for path in explicit_paths or []]
    for directory in directories or []:
        paths.extend(sorted(Path(directory).glob('*.jsonl')))
    unique_paths = list(dict.fromkeys(path.resolve() for path in paths))
    if not unique_paths:
        raise SystemExit('No training JSONL files found; use --train-data and/or --train-data-dir.')
    return unique_paths


def _load_groups(paths: list[Path],
                 include_vocabularies: set[str] | None = None,
                 exclude_vocabularies: set[str] | None = None,
                 ranking_score_key: str | None = None,
                 compact_model_candidates: int | None = None,
                 eval_fraction: float = 0.02,
                 split_seed: int = 13,
                 sampling_seed: int = 42,
                 ) -> list[dict]:
    """Load the training projection of query groups, applying vocabulary masks on read.

    Scored pools can contain tens of gigabytes of retrieval provenance.  None of that evidence
    is consumed by splitting, sampling, rendering, or evaluation, so retaining the original
    dictionaries multiplies peak RAM for no semantic benefit.  Keep only the stable training
    contract and, when requested, the selected score channel.
    """
    groups: list[dict] = []
    for path in paths:
        with path.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    group = json.loads(line)
                    prefix = group['prefix']
                    if include_vocabularies is not None and prefix not in include_vocabularies:
                        continue
                    if exclude_vocabularies is not None and prefix in exclude_vocabularies:
                        continue
                    projected = {
                        'prefix': prefix,
                        'query_id': group['query_id'],
                        'query': group['query'],
                        'gold_concept_id': group['gold_concept_id'],
                        'negatives': [
                            {'concept_id': negative['concept_id']}
                            for negative in (group.get('negatives') or [])
                        ],
                    }
                    if group.get('query_kind') is not None:
                        projected['query_kind'] = group['query_kind']
                    candidate_pool = []
                    for candidate in group.get('candidate_pool') or []:
                        record = {
                            'concept_id': candidate['concept_id'],
                            'role': candidate.get('role'),
                        }
                        if ranking_score_key is not None:
                            score = (candidate.get('ranking_scores') or {}).get(ranking_score_key)
                            if score is not None:
                                record['ranking_scores'] = {ranking_score_key: score}
                        candidate_pool.append(record)
                    if compact_model_candidates and ranking_score_key is not None:
                        split_key = f'{split_seed}:{prefix}:{group["gold_concept_id"]}'
                        is_eval = _stable_unit_fraction(split_key) < eval_fraction
                        if not is_eval:
                            gold = [candidate for candidate in candidate_pool
                                    if candidate.get('role') == 'gold']
                            negatives = [candidate for candidate in candidate_pool
                                         if candidate.get('role') != 'gold'
                                         and ranking_score_key in candidate.get('ranking_scores', {})]
                            negatives.sort(key=lambda candidate: (
                                -float(candidate['ranking_scores'][ranking_score_key]),
                                candidate['concept_id'],
                            ))
                            # Materialise the same hard/mid/easy mixture used by
                            # _training_negatives while the source row is in hand. This avoids
                            # retaining ~100 retrieval candidates for every training group in
                            # every DDP worker. Repeated balanced draws intentionally reuse the
                            # selected tail item; rendering augmentation remains draw-specific.
                            count = compact_model_candidates
                            hard_count = (count + 1) // 2
                            middle_count = max(1, count // 4) if count - hard_count > 1 else 0
                            selected = list(negatives[:hard_count])
                            remaining = negatives[hard_count:]
                            if middle_count:
                                centre = len(remaining) // 2
                                lo = max(0, centre - middle_count // 2)
                                selected.extend(remaining[lo:lo + middle_count])
                            selected_ids = {candidate['concept_id'] for candidate in selected}
                            tail_start = max(hard_count, (2 * len(negatives)) // 3)
                            tail = [candidate for candidate in reversed(negatives[tail_start:])
                                    if candidate['concept_id'] not in selected_ids]
                            if tail:
                                offset = int(_stable_unit_fraction(
                                    f'{sampling_seed}:0:{group["query_id"]}:tail'
                                ) * len(tail))
                                tail = tail[offset:] + tail[:offset]
                            selected.extend(tail[:count - len(selected)])
                            if len(selected) < count:
                                selected_ids = {candidate['concept_id'] for candidate in selected}
                                selected.extend(candidate for candidate in negatives
                                                if candidate['concept_id'] not in selected_ids)
                            candidate_pool = gold + selected[:count]
                    projected['candidate_pool'] = candidate_pool
                    groups.append(projected)
    return groups


def _load_eval_groups(paths: list[Path], eval_fraction: float, split_seed: int,
                      include_vocabularies: set[str] | None = None,
                      exclude_vocabularies: set[str] | None = None) -> list[dict]:
    """Stream only the held-out projection for a restartable evaluation-only run."""
    groups = []
    seen = set()
    for path in paths:
        with path.open('r', encoding='utf-8') as handle:
            for line in handle:
                if not line.strip():
                    continue
                group = json.loads(line)
                prefix = group['prefix']
                if include_vocabularies is not None and prefix not in include_vocabularies:
                    continue
                if exclude_vocabularies is not None and prefix in exclude_vocabularies:
                    continue
                split_key = f'{split_seed}:{prefix}:{group["gold_concept_id"]}'
                if _stable_unit_fraction(split_key) >= eval_fraction:
                    continue
                key = (prefix, group['query_id'], group['gold_concept_id'],
                       _normalise_query(group['query']))
                if key in seen:
                    continue
                seen.add(key)
                groups.append({
                    'prefix': prefix,
                    'query': group['query'],
                    'query_kind': group.get('query_kind'),
                    'gold_concept_id': group['gold_concept_id'],
                    'candidate_pool': [{'concept_id': c['concept_id']}
                                       for c in group.get('candidate_pool') or []],
                    'negatives': [{'concept_id': c['concept_id']}
                                  for c in group.get('negatives') or []],
                })
    return groups


def _load_concept_store(dirs: list[Path],
                        include_vocabularies: set[str] | None = None,
                        exclude_vocabularies: set[str] | None = None,
                        ) -> dict[tuple[str, str], dict]:
    """Load every "<prefix>.concepts.jsonl" in the given directories into a (prefix, concept_id) -> fields lookup."""
    store: dict[tuple[str, str], dict] = {}
    for directory in dirs:
        for path in sorted(directory.glob('*.concepts.jsonl')):
            prefix = path.name.removesuffix('.concepts.jsonl')
            if include_vocabularies is not None and prefix not in include_vocabularies:
                continue
            if exclude_vocabularies is not None and prefix in exclude_vocabularies:
                continue
            with path.open('r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    store[(prefix, record['concept_id'])] = record
    return store


def _stable_unit_fraction(key: str) -> float:
    """Deterministic hash of a string to a float in [0, 1)."""
    digest = hashlib.sha256(key.encode('utf-8')).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _stable_choice(key: str, options: list):
    """Deterministically pick one of `options` from a string key."""
    digest = hashlib.sha256(key.encode('utf-8')).hexdigest()
    return options[int(digest[:8], 16) % len(options)]


def _split_by_concept(groups: list[dict],
                      eval_fraction: float,
                      split_seed: int,
                      ) -> tuple[list[dict], list[dict]]:
    """
    Split into train/eval by hashing (prefix, gold_concept_id) -- never by row -- so a
    concept's query units always land on the same side. Extension point for splitting by a
    cross-vocabulary equivalence component instead: swap the hash key for the component id.
    """
    train, eval_ = [], []
    for group in groups:
        key = f'{split_seed}:{group["prefix"]}:{group["gold_concept_id"]}'
        (eval_ if _stable_unit_fraction(key) < eval_fraction else train).append(group)
    return train, eval_


def _vocab_balanced_sample(groups: list[dict],
                           alpha: float,
                           target_size: int | None,
                           seed: int,
                           ) -> list[dict]:
    """Temperature-sample across vocabularies, P(vocab) ~ N_vocab^alpha, with replacement, to `target_size` (or len(groups))."""
    by_vocab: dict[str, list[dict]] = defaultdict(list)
    for group in groups:
        by_vocab[group['prefix']].append(group)

    vocabs = sorted(by_vocab.keys())
    weights = [len(by_vocab[v]) ** alpha for v in vocabs]

    rng = random.Random(seed)
    size = target_size if target_size is not None else len(groups)

    sampled = []
    for _ in range(size):
        vocab = rng.choices(vocabs, weights=weights, k=1)[0]
        sampled.append(rng.choice(by_vocab[vocab]))
    return sampled


def _stratified_eval_sample(groups: list[dict],
                            target_size: int | None,
                            seed: int,
                            ) -> list[dict]:
    """Deterministically sample held-out groups approximately equally across vocabularies."""
    if target_size is None or target_size >= len(groups):
        return groups

    by_vocab: dict[str, list[dict]] = defaultdict(list)
    for group in groups:
        by_vocab[group['prefix']].append(group)

    rng = random.Random(seed)
    vocabs = sorted(by_vocab)
    base, remainder = divmod(target_size, len(vocabs))
    sampled = []
    for index, vocab in enumerate(vocabs):
        take = min(len(by_vocab[vocab]), base + (1 if index < remainder else 0))
        sampled.extend(rng.sample(by_vocab[vocab], take))

    # Backfill when a small vocabulary could not supply its nominal share.
    if len(sampled) < target_size:
        selected_ids = {id(group) for group in sampled}
        remainder_pool = [group for group in groups if id(group) not in selected_ids]
        sampled.extend(rng.sample(remainder_pool, min(target_size - len(sampled), len(remainder_pool))))
    return sampled


def _report_sampling_diagnostics(sampled_groups: list[dict]) -> None:
    """Print per-vocabulary draw counts, unique groups/concepts, and resampling duplicates."""
    totals: dict[str, int] = defaultdict(int)
    unique_groups: dict[str, set] = defaultdict(set)
    unique_concepts: dict[str, set] = defaultdict(set)

    for group in sampled_groups:
        prefix = group['prefix']
        totals[prefix] += 1
        unique_groups[prefix].add(group['query_id'])
        unique_concepts[prefix].add(group['gold_concept_id'])

    print(f'Vocabulary-balanced sample ({len(sampled_groups)} rows):')
    for prefix in sorted(totals):
        total = totals[prefix]
        n_groups = len(unique_groups[prefix])
        n_concepts = len(unique_concepts[prefix])
        print(
            f'  {prefix}: {total} sampled, {n_groups} unique query groups '
            f'({total - n_groups} duplicate draws from resampling), {n_concepts} unique gold concepts'
        )


def _should_drop_preferred_label_query(group: dict,
                                       concept_store: dict[tuple[str, str], dict],
                                       keep_probability: float,
                                       seed: int,
                                       ) -> bool:
    """True if the query text exactly equals the gold label (case-insensitively) and this occurrence is downsampled away."""
    concept = concept_store.get((group['prefix'], group['gold_concept_id']))
    if concept is None or not concept.get('label'):
        return False
    if group['query'].strip().casefold() != concept['label'].strip().casefold():
        return False
    return _stable_unit_fraction(f'{seed}:preferred_label_keep:{group["query_id"]}') >= keep_probability


def _render_gold(group: dict,
                 concept_store: dict[tuple[str, str], dict],
                 seed: int,
                 sample_index: int,
                 max_aliases: int | None,
                 ) -> str | None:
    """Render the gold concept, excluding the query's own alias text (training-only augmentation)."""
    concept = concept_store.get((group['prefix'], group['gold_concept_id']))
    if concept is None:
        return None

    variant = _stable_choice(
        f'{seed}:{sample_index}:{group["query_id"]}:{group["gold_concept_id"]}:variant', ALL_VARIANTS,
    )
    return render_concept(
        label=concept['label'],
        synonyms=concept['synonyms'],
        definition=concept['definition'],
        variant=variant,
        exclude_aliases={group['query']},
        max_aliases=max_aliases,
        alias_selection_key=f'{group["prefix"]}:{group["gold_concept_id"]}',
    )


def _render_negative(group: dict,
                     negative: dict,
                     concept_store: dict[tuple[str, str], dict],
                     seed: int,
                     sample_index: int,
                     max_aliases: int | None,
                     ) -> str | None:
    """Render one negative concept (no alias exclusion)."""
    concept = concept_store.get((group['prefix'], negative['concept_id']))
    if concept is None:
        return None

    variant = _stable_choice(
        f'{seed}:{sample_index}:{group["query_id"]}:{negative["concept_id"]}:variant', ALL_VARIANTS,
    )
    return render_concept(
        label=concept['label'],
        synonyms=concept['synonyms'],
        definition=concept['definition'],
        variant=variant,
        max_aliases=max_aliases,
        alias_selection_key=f'{group["prefix"]}:{negative["concept_id"]}',
    )


def _training_negatives(group: dict,
                        count: int,
                        sampling: str,
                        score_key: str | None,
                        seed: int,
                        sample_index: int,
                        ) -> list[dict]:
    """Choose negatives from legacy selection or the immutable scored candidate pool.

    Model-stratified sampling deliberately mixes the top, middle, and tail of the previous
    model's score distribution.  This avoids spending every gradient on near-duplicate top
    hits while retaining the model's most informative mistakes.
    """
    if sampling == 'retrieval':
        return list(group.get('negatives') or [])[:count]
    if not score_key:
        raise ValueError('--ranking-score-key is required for model-stratified sampling')

    candidates = [
        candidate for candidate in (group.get('candidate_pool') or [])
        if candidate.get('role') != 'gold'
        and score_key in (candidate.get('ranking_scores') or {})
    ]
    candidates.sort(
        key=lambda candidate: (-float(candidate['ranking_scores'][score_key]), candidate['concept_id'])
    )
    if len(candidates) <= count:
        return candidates

    # Half hard, one quarter around the decision boundary, and the rest from the tail.
    hard_count = (count + 1) // 2
    middle_count = max(1, count // 4) if count - hard_count > 1 else 0
    selected = list(candidates[:hard_count])
    remaining = candidates[hard_count:]
    if middle_count:
        centre = len(remaining) // 2
        lo = max(0, centre - middle_count // 2)
        selected.extend(remaining[lo:lo + middle_count])
    selected_ids = {candidate['concept_id'] for candidate in selected}
    tail_start = max(hard_count, (2 * len(candidates)) // 3)
    tail = [
        candidate for candidate in reversed(candidates[tail_start:])
        if candidate['concept_id'] not in selected_ids
    ]
    # Rotate the easy tail deterministically so repeated vocabulary-balanced draws do not
    # always use the identical easy concept.
    if tail:
        offset = int(_stable_unit_fraction(
            f'{seed}:{sample_index}:{group["query_id"]}:tail'
        ) * len(tail))
        tail = tail[offset:] + tail[:offset]
    selected.extend(tail[:count - len(selected)])
    if len(selected) < count:
        selected_ids = {candidate['concept_id'] for candidate in selected}
        selected.extend(
            candidate for candidate in candidates
            if candidate['concept_id'] not in selected_ids
        )
    return selected[:count]


def _flatten_to_rows(sampled_groups: list[dict],
                     concept_store: dict[tuple[str, str], dict],
                     negatives_per_query: int,
                     seed: int,
                     max_aliases: int | None,
                     preferred_label_keep_probability: float,
                     negative_sampling: str = 'retrieval',
                     ranking_score_key: str | None = None,
                     training_objective: str = 'contrastive',
                     distillation_temperature: float = 1.0,
                     ) -> tuple[list[dict], dict[str, int]]:
    """
    Resolve/render each sampled group into one row (query, positive, negative_1..K) with K
    DISTINCT resolvable negatives -- never cycled. `sample_index` (position in the sampled
    list) seeds render-variant selection, so a resampled duplicate group can render
    differently. Drops (and counts) preferred-label-downsampled, unresolvable-gold, and
    insufficient-negative groups.
    """
    rows = []
    stats = {
        'skipped_preferred_label_downsampled': 0,
        'skipped_missing_gold': 0,
        'skipped_insufficient_resolvable_negatives': 0,
    }

    for sample_index, group in enumerate(sampled_groups):
        if _should_drop_preferred_label_query(group, concept_store, preferred_label_keep_probability, seed):
            stats['skipped_preferred_label_downsampled'] += 1
            continue

        positive = _render_gold(group, concept_store, seed, sample_index, max_aliases)
        if not positive:
            stats['skipped_missing_gold'] += 1
            continue

        negatives: list[str] = []
        negative_records: list[dict] = []
        seen_negative_ids: set[str] = set()
        for negative in _training_negatives(
            group, negatives_per_query, negative_sampling, ranking_score_key, seed, sample_index,
        ):
            concept_id = negative['concept_id']
            if concept_id in seen_negative_ids:
                continue
            rendered = _render_negative(group, negative, concept_store, seed, sample_index, max_aliases)
            if rendered:
                negatives.append(rendered)
                negative_records.append(negative)
                seen_negative_ids.add(concept_id)
            if len(negatives) >= negatives_per_query:
                break

        if len(negatives) < negatives_per_query:
            stats['skipped_insufficient_resolvable_negatives'] += 1
            continue

        if training_objective == 'distillation':
            pool_by_id = {
                candidate['concept_id']: candidate for candidate in (group.get('candidate_pool') or [])
            }
            gold = pool_by_id.get(group['gold_concept_id'])
            score_records = [gold] + negative_records
            if any(
                record is None or ranking_score_key not in (record.get('ranking_scores') or {})
                for record in score_records
            ):
                stats['skipped_insufficient_resolvable_negatives'] += 1
                continue
            row = {
                'query': group['query'],
                'documents': [positive] + negatives[:negatives_per_query],
                'scores': [
                    float(record['ranking_scores'][ranking_score_key]) / distillation_temperature
                    for record in score_records
                ],
            }
        else:
            row = {'query': group['query'], 'positive': positive}
            for i, text in enumerate(negatives[:negatives_per_query], start=1):
                row[f'negative_{i}'] = text
        rows.append(row)

    print(
        f'Flattening: {len(rows)} rows kept out of {len(sampled_groups)} sampled groups -- '
        f'dropped {stats["skipped_preferred_label_downsampled"]} (preferred-label downsampling), '
        f'{stats["skipped_missing_gold"]} (unresolvable gold concept), '
        f'{stats["skipped_insufficient_resolvable_negatives"]} '
        f'(fewer than {negatives_per_query} distinct resolvable negatives).'
    )

    return rows, stats


def _build_candidate_sets(eval_groups: list[dict],
                          concept_store: dict[tuple[str, str], dict],
                          eval_variant: RenderVariant,
                          max_aliases: int | None,
                          ) -> list[dict]:
    """
    Build candidate-set ranking examples: gold + every distinct resolvable negative, all
    rendered identically (fixed variant, no alias exclusion) to match production rendering.
    Drops groups with an unresolvable gold or no negatives at all.
    """
    examples = []

    for group in eval_groups:
        gold_concept = concept_store.get((group['prefix'], group['gold_concept_id']))
        if gold_concept is None:
            continue

        candidate_ids = [group['gold_concept_id']]
        exact_alias_ids = []
        normalised_query = _normalise_query(group['query'])
        gold_aliases = [gold_concept.get('label')] + (gold_concept.get('synonyms') or [])
        if any(value and _normalise_query(value) == normalised_query for value in gold_aliases):
            exact_alias_ids.append(group['gold_concept_id'])
        candidate_texts = [render_concept(
            label=gold_concept['label'], synonyms=gold_concept['synonyms'], definition=gold_concept['definition'],
            variant=eval_variant, max_aliases=max_aliases,
            alias_selection_key=f'{group["prefix"]}:{group["gold_concept_id"]}',
        )]
        seen_ids = {group['gold_concept_id']}

        # Schema-v2 retains the full recall pool; evaluate against it instead of the smaller
        # legacy training selection.  Legacy shards continue to use `negatives` unchanged.
        evaluation_candidates = group.get('candidate_pool') or group.get('negatives') or []
        for negative in evaluation_candidates:
            concept_id = negative['concept_id']
            if concept_id == group['gold_concept_id']:
                continue
            if concept_id in seen_ids:
                continue
            neg_concept = concept_store.get((group['prefix'], concept_id))
            if neg_concept is None:
                continue
            candidate_ids.append(concept_id)
            negative_aliases = [neg_concept.get('label')] + (neg_concept.get('synonyms') or [])
            if any(value and _normalise_query(value) == normalised_query for value in negative_aliases):
                exact_alias_ids.append(concept_id)
            candidate_texts.append(render_concept(
                label=neg_concept['label'], synonyms=neg_concept['synonyms'], definition=neg_concept['definition'],
                variant=eval_variant, max_aliases=max_aliases,
                alias_selection_key=f'{group["prefix"]}:{concept_id}',
            ))
            seen_ids.add(concept_id)

        if len(candidate_ids) < 2:
            continue

        examples.append({
            'prefix': group['prefix'],
            'gold_concept_id': group['gold_concept_id'],
            'query': group['query'],
            'query_kind': group.get('query_kind'),
            'candidate_ids': candidate_ids,
            'candidate_texts': candidate_texts,
            'exact_alias_ids': exact_alias_ids,
        })

    return examples


def _candidate_set_metrics(ranks: list[int | None]) -> dict:
    """Accuracy@1/MRR/Recall@3/Recall@5 from 1-based gold ranks (None = gold not found, shouldn't normally happen)."""
    n = len(ranks)
    if n == 0:
        return {}
    return {
        'accuracy_at_1': sum(1 for r in ranks if r == 1) / n,
        'mrr': sum((1.0 / r) if r else 0.0 for r in ranks) / n,
        'recall_at_3': sum(1 for r in ranks if r is not None and r <= 3) / n,
        'recall_at_5': sum(1 for r in ranks if r is not None and r <= 5) / n,
        'n': n,
    }


def _run_candidate_set_evaluation(model, examples: list[dict], batch_size: int,
                                  prediction_output: Path | None = None) -> dict:
    """Score every example's full candidate set with the trained model via pylate.rank.rerank; return metrics global + per vocabulary."""
    from pylate import rank

    if not examples:
        return {}

    ranks_by_vocab: dict[str, list[int | None]] = defaultdict(list)
    hybrid_ranks_by_vocab: dict[str, list[int | None]] = defaultdict(list)
    ranks_by_exactness: dict[str, list[int | None]] = defaultdict(list)
    prediction_partial = None
    prediction_handle = None
    if prediction_output is not None:
        prediction_output.parent.mkdir(parents=True, exist_ok=True)
        prediction_partial = prediction_output.with_name(prediction_output.name + '.partial')
        prediction_handle = prediction_partial.open('w', encoding='utf-8')
    # A full evaluation can contain millions of rendered candidate documents.  Encode and
    # rank bounded query-group chunks so embeddings are released after each chunk; the rank
    # metrics are exactly the same as evaluating the concatenated lists at once.
    for start in range(0, len(examples), batch_size):
        chunk = examples[start:start + batch_size]
        query_embeddings = model.encode(
            [ex['query'] for ex in chunk], is_query=True,
            batch_size=batch_size, show_progress_bar=False,
        )
        document_embeddings = model.encode(
            [ex['candidate_texts'] for ex in chunk], is_query=False,
            batch_size=batch_size, show_progress_bar=False,
        )
        reranked = rank.rerank(
            documents_ids=[ex['candidate_ids'] for ex in chunk],
            queries_embeddings=query_embeddings,
            documents_embeddings=document_embeddings,
        )
        for example, results in zip(chunk, reranked):
            ranked_ids = [r['id'] for r in results]
            try:
                gold_rank = ranked_ids.index(example['gold_concept_id']) + 1
            except ValueError:
                gold_rank = None
            ranks_by_vocab[example['prefix']].append(gold_rank)
            exact_alias_ids = example.get('exact_alias_ids') or []
            exactness = ('no_exact_match' if not exact_alias_ids else
                         'unique_exact_match' if len(exact_alias_ids) == 1 else
                         'ambiguous_exact_match')
            ranks_by_exactness[exactness].append(gold_rank)
            if prediction_handle is not None:
                prediction_handle.write(json.dumps({
                    'prefix': example['prefix'],
                    'query': example['query'],
                    'query_kind': example.get('query_kind'),
                    'gold_concept_id': example['gold_concept_id'],
                    'gold_rank': gold_rank,
                    'candidate_count': len(example['candidate_ids']),
                    'exact_alias_ids': exact_alias_ids,
                    'top_candidates': [
                        {'concept_id': str(item['id']), 'score': float(item['score'])}
                        for item in results[:5]
                    ],
                }, ensure_ascii=False) + '\n')

            # A unique exact label/synonym match is stronger evidence than semantic similarity.
            # Multiple exact matches remain ambiguous and are deliberately left to the model.
            hybrid_ids = ranked_ids
            if len(exact_alias_ids) == 1 and exact_alias_ids[0] in ranked_ids:
                exact_id = exact_alias_ids[0]
                hybrid_ids = [exact_id] + [candidate_id for candidate_id in ranked_ids if candidate_id != exact_id]
            try:
                hybrid_rank = hybrid_ids.index(example['gold_concept_id']) + 1
            except ValueError:
                hybrid_rank = None
            hybrid_ranks_by_vocab[example['prefix']].append(hybrid_rank)

    if prediction_handle is not None:
        prediction_handle.close()
        prediction_partial.replace(prediction_output)

    metrics = {'global': _candidate_set_metrics([r for ranks in ranks_by_vocab.values() for r in ranks])}
    for prefix, ranks in ranks_by_vocab.items():
        metrics[prefix] = _candidate_set_metrics(ranks)
    metrics['hybrid_exact_alias'] = {
        'global': _candidate_set_metrics([
            rank for ranks in hybrid_ranks_by_vocab.values() for rank in ranks
        ]),
        'per_vocabulary': {
            prefix: _candidate_set_metrics(ranks)
            for prefix, ranks in hybrid_ranks_by_vocab.items()
        },
    }
    metrics['by_exactness'] = {
        category: _candidate_set_metrics(ranks)
        for category, ranks in ranks_by_exactness.items()
    }
    return metrics


class CandidateSetEvaluator:
    """SentenceTransformer-compatible evaluator using the real multi-candidate ranking task."""

    greater_is_better = True
    primary_metric = 'candidate_mrr'

    def __init__(self, examples: list[dict], batch_size: int):
        self.examples = examples
        self.batch_size = batch_size

    def __call__(self, model, output_path=None, epoch=-1, steps=-1) -> dict[str, float]:
        del output_path, epoch, steps
        result = _run_candidate_set_evaluation(model, self.examples, self.batch_size)
        global_metrics = result.get('global', {})
        return {
            'candidate_accuracy_at_1': global_metrics.get('accuracy_at_1', 0.0),
            'candidate_mrr': global_metrics.get('mrr', 0.0),
            'candidate_recall_at_3': global_metrics.get('recall_at_3', 0.0),
            'candidate_recall_at_5': global_metrics.get('recall_at_5', 0.0),
        }


def _report_candidate_length_distribution(model,
                                          concept_store: dict[tuple[str, str], dict],
                                          eval_variant: RenderVariant,
                                          max_aliases: int | None,
                                          document_length: int,
                                          sample_size: int = 20_000,
                                          seed: int = 42,
                                          ) -> None:
    """Render (a sample of) the concept store with the production/eval representation, tokenize, and print the length distribution + truncation warning."""
    entries = list(concept_store.items())
    if len(entries) > sample_size:
        entries = random.Random(seed).sample(entries, sample_size)

    lengths = []
    for (prefix, concept_id), concept in entries:
        text = render_concept(
            label=concept['label'], synonyms=concept['synonyms'], definition=concept['definition'],
            variant=eval_variant, max_aliases=max_aliases,
            alias_selection_key=f'{prefix}:{concept_id}',
        )
        if not text:
            continue
        lengths.append(len(model.tokenizer(text, add_special_tokens=True)['input_ids']))

    if not lengths:
        print('Candidate token-length distribution: nothing to measure (empty concept store?).')
        return

    lengths.sort()

    def pct(p: float) -> int:
        return lengths[min(len(lengths) - 1, int(p * len(lengths)))]

    truncated = sum(1 for length in lengths if length > document_length)
    truncated_fraction = truncated / len(lengths)
    print(
        f'Candidate token-length distribution ({eval_variant.value}, n={len(lengths)}): '
        f'p50={pct(0.50)} p90={pct(0.90)} p95={pct(0.95)} p99={pct(0.99)} max={lengths[-1]} -- '
        f'{truncated}/{len(lengths)} ({truncated_fraction:.1%}) would be truncated at '
        f'--document-length={document_length}'
    )
    if truncated_fraction > 0.05:
        print(
            f'WARNING: more than 5% of rendered candidates exceed --document-length='
            f'{document_length} and will be truncated. --document-length is not changed '
            f'automatically -- raise it yourself if this looks significant.'
        )


def _build_eval_triplets(groups: list[dict],
                         concept_store: dict[tuple[str, str], dict],
                         ) -> tuple[list[str], list[str], list[str]]:
    """
    Build (anchor, positive, negative) triplets for the SECONDARY smoke-test evaluator
    (one negative per group, no alias exclusion) -- not the primary metric, see
    `_build_candidate_sets`/`_run_candidate_set_evaluation`.
    """
    anchors, positives, negatives = [], [], []

    for group in groups:
        concept = concept_store.get((group['prefix'], group['gold_concept_id']))
        if concept is None:
            continue
        positive = render_concept(
            label=concept['label'], synonyms=concept['synonyms'], definition=concept['definition'],
            variant=RenderVariant.LABEL_ALIASES_DEFINITION,
        )

        negative_text = None
        for negative in group.get('negatives') or []:
            neg_concept = concept_store.get((group['prefix'], negative['concept_id']))
            if neg_concept is not None:
                negative_text = render_concept(
                    label=neg_concept['label'], synonyms=neg_concept['synonyms'],
                    definition=neg_concept['definition'], variant=RenderVariant.LABEL_ALIASES_DEFINITION,
                )
                break
        if negative_text is None:
            continue

        anchors.append(group['query'])
        positives.append(positive)
        negatives.append(negative_text)

    return anchors, positives, negatives


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Fine-tune a ColBERT-style late-interaction concept-normalisation reranker.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--train-data', nargs='+', default=None,
        help='Mined query-group JSONL shard file(s) -- the full pool; a held-out split is carved out internally (see --eval-fraction).',
    )
    parser.add_argument(
        '--train-data-dir', nargs='+', default=None,
        help='Directory/directories dynamically scanned for per-vocabulary *.jsonl mining shards.',
    )
    parser.add_argument(
        '--include-vocabularies', nargs='*', default=None,
        help='Use only these vocabulary prefixes from discovered/explicit shards.',
    )
    parser.add_argument(
        '--exclude-vocabularies', nargs='*', default=None,
        help='Mask these vocabulary prefixes without rewriting or recombining mined shards.',
    )
    parser.add_argument(
        '--concept-store-dir', nargs='+', required=True,
        help='Directory/directories of "<prefix>.concepts.jsonl" files (from build_training_data.py --concept-store-dir).',
    )
    parser.add_argument(
        '--eval-fraction', type=float, default=0.02,
        help='Fraction of CONCEPTS (not rows) held out for eval, split by hashing (prefix, gold_concept_id).',
    )
    parser.add_argument('--split-seed', type=int, default=13, help='Seed for the train/eval concept split.')
    parser.add_argument(
        '--max-groups', type=int, default=None,
        help='Target sample size after vocabulary-balanced sampling -- the 100k/500k/1M growth knob. Defaults to the full train-split size.',
    )
    parser.add_argument(
        '--vocab-sampling-alpha', type=float, default=0.5,
        help='Temperature exponent for vocabulary-balanced sampling: P(vocab) ~ N_vocab^alpha (1.0 = proportional, 0.0 = uniform).',
    )
    parser.add_argument(
        '--negatives-per-query', type=int, default=4,
        help='Required number of DISTINCT resolvable negatives per row -- never cycled; short groups are dropped.',
    )
    parser.add_argument(
        '--negative-sampling', choices=['retrieval', 'model_stratified'], default='retrieval',
        help='Use legacy retrieval-selected negatives or a hard/mid/easy mixture from a scored candidate_pool.',
    )
    parser.add_argument(
        '--ranking-score-key', default=None,
        help='Key in candidate_pool[*].ranking_scores used by model-stratified sampling/distillation.',
    )
    parser.add_argument(
        '--training-objective', choices=['contrastive', 'distillation'], default='contrastive',
        help='Hard-label contrastive training or optional listwise KL distillation from stored ranking scores.',
    )
    parser.add_argument(
        '--distillation-temperature', type=float, default=1.0,
        help='Divide stored teacher logits by this value before listwise distillation.',
    )
    parser.add_argument(
        '--preferred-label-query-keep-probability', type=float, default=0.1,
        help='Fraction of preferred-label queries (query == gold label) to keep.',
    )
    parser.add_argument(
        '--max-aliases-rendered', type=int, default=6,
        help='Cap on synonyms rendered per candidate (non-positive disables the cap).',
    )
    parser.add_argument(
        '--eval-render-variant', default=RenderVariant.LABEL_ALIASES_DEFINITION.value,
        choices=[v.value for v in RenderVariant],
        help='Fixed rendering variant for every candidate in the primary evaluation (production-like, no augmentation).',
    )
    parser.add_argument(
        '--base-model', default='FremyCompany/BioLORD-2023',
        help='Base sentence-transformer checkpoint to initialise the ColBERT model from.',
    )
    parser.add_argument('--query-length', type=int, default=32, help='Max token length for queries.')
    parser.add_argument(
        '--document-length', type=int, default=64,
        help='Max token length for rendered candidates -- check the printed length distribution before assuming this is enough.',
    )
    parser.add_argument(
        '--output-dir', default=str(DEFAULT_OUTPUT_DIR),
        help='Run directory for checkpoints and final/ model bundle. Override for experiments; '
             'the default final/ bundle is discovered by the local service.',
    )
    parser.add_argument('--evaluate-only', action='store_true',
                        help='Evaluate an existing --output-dir/final on the full held-out split without retraining.')
    parser.add_argument('--evaluation-model', default=None,
                        help='Optional checkpoint to load in --evaluate-only mode (defaults to --output-dir/final).')
    parser.add_argument('--evaluation-result', default=None,
                        help='Optional result JSON path in --evaluate-only mode (defaults to --output-dir/final_eval_result.json).')
    parser.add_argument('--prediction-output', default=None,
                        help='Optional per-query JSONL ranking output in --evaluate-only mode for error analysis.')
    parser.add_argument('--epochs', type=float, default=1.0)
    parser.add_argument('--batch-size', type=int, default=32, help='Per-device train batch size.')
    parser.add_argument('--eval-batch-size', type=int, default=32, help='Per-device eval batch size.')
    parser.add_argument('--learning-rate', type=float, default=3e-5)
    parser.add_argument('--warmup-ratio', type=float, default=0.05)
    parser.add_argument(
        '--gradient-accumulation-steps', type=int, default=1,
        help='Must stay 1 -- contrastive training does not support ordinary gradient accumulation. Use --cached-loss for a larger effective batch.',
    )
    parser.add_argument(
        '--cached-loss', action='store_true',
        help='Use CachedContrastive (GradCache) to fit a larger --batch-size into limited memory via --cached-mini-batch-size chunks.',
    )
    parser.add_argument(
        '--cached-mini-batch-size', type=int, default=16,
        help='Chunk size CachedContrastive processes at once (only with --cached-loss).',
    )
    parser.add_argument(
        '--gather-across-devices', action='store_true',
        help='Gather document embeddings across GPUs/processes for in-batch negatives -- enable for multi-GPU training.',
    )
    parser.add_argument('--temperature', type=float, default=0.02, help='Contrastive loss temperature.')
    parser.add_argument('--bf16', action='store_true', help='Train in bf16 (Ampere/A100+; not supported on Pascal/P100).')
    parser.add_argument('--fp16', action='store_true', help='Train in fp16 (use on Pascal/P100 nodes instead of --bf16).')
    parser.add_argument('--seed', type=int, default=42, help='Seed for training and deterministic rendering/downsampling choices.')
    parser.add_argument('--logging-steps', type=int, default=50)
    parser.add_argument('--save-steps', type=int, default=1000)
    parser.add_argument('--save-total-limit', type=int, default=3)
    parser.add_argument(
        '--eval-steps', type=int, default=0,
        help='Run primary candidate-set evaluation every N steps (0 disables). The primary evaluation always runs once after training.',
    )
    parser.add_argument(
        '--periodic-eval-max-groups', type=int, default=1000,
        help='Vocabulary-stratified held-out groups used for periodic candidate-set evaluation; non-positive uses all.',
    )
    parser.add_argument(
        '--drop-ambiguous-training-queries', action='store_true',
        help='Exclude train groups whose normalised query maps to multiple gold IDs in the loaded pool; evaluation remains unchanged.',
    )
    parser.add_argument(
        '--drop-contextless-training-queries', action='store_true',
        help='Exclude one/two-character aliases and generic context-dependent responses (e.g. Yes/No) from training only.',
    )
    parser.add_argument('--quality-review-output', default=None,
                        help='Optional JSONL review queue for excluded training groups; mined/scored input remains immutable.')
    parser.add_argument('--quality-review-limit', type=int, default=1000)
    parser.add_argument('--resume-from-checkpoint', default=None, help='Path to a checkpoint directory to resume from.')
    parser.add_argument(
        '--trainer-args-json', default=None,
        help='Optional JSON object merged into SentenceTransformerTrainingArguments, e.g. \'{"dataloader_num_workers": 8}\'.',
    )

    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    """Cheap, data-independent checks, run before touching files or the ML stack."""
    errors = []
    if not 0.0 < args.eval_fraction < 1.0:
        errors.append('--eval-fraction must be strictly between 0 and 1.')
    if args.vocab_sampling_alpha < 0:
        errors.append('--vocab-sampling-alpha must be >= 0.')
    if args.negatives_per_query < 1:
        errors.append('--negatives-per-query must be >= 1.')
    if args.negative_sampling == 'model_stratified' and not args.ranking_score_key:
        errors.append('--model-stratified sampling requires --ranking-score-key.')
    if args.training_objective == 'distillation' and not args.ranking_score_key:
        errors.append('--training-objective distillation requires --ranking-score-key.')
    if args.distillation_temperature <= 0:
        errors.append('--distillation-temperature must be > 0.')
    if args.training_objective == 'distillation' and args.cached_loss:
        errors.append('--cached-loss is only supported by the contrastive objective.')
    if args.training_objective == 'distillation' and args.gather_across_devices:
        errors.append('--gather-across-devices is only supported by the contrastive objective.')
    if not 0.0 <= args.preferred_label_query_keep_probability <= 1.0:
        errors.append('--preferred-label-query-keep-probability must be within [0, 1].')
    if args.bf16 and args.fp16:
        errors.append('--bf16 and --fp16 are mutually exclusive.')
    if args.gradient_accumulation_steps != 1:
        errors.append(
            'Contrastive training should use --gradient-accumulation-steps 1. For larger '
            'contrastive batches use --cached-loss, increase --batch-size, and control memory '
            'with --cached-mini-batch-size.'
        )
    if args.eval_steps < 0:
        errors.append('--eval-steps must be >= 0.')
    if args.quality_review_limit < 0:
        errors.append('--quality-review-limit must be >= 0.')
    if args.eval_steps > 0 and args.save_steps % args.eval_steps != 0:
        errors.append('--save-steps must be a multiple of --eval-steps for best-checkpoint selection.')
    if args.include_vocabularies and args.exclude_vocabularies:
        overlap = sorted(set(args.include_vocabularies) & set(args.exclude_vocabularies))
        if overlap:
            errors.append(
                '--include-vocabularies and --exclude-vocabularies overlap: ' + ', '.join(overlap)
            )

    if errors:
        raise SystemExit('Invalid arguments:\n' + '\n'.join(f'  - {e}' for e in errors))


def main() -> None:
    args = _parse_args()
    _validate_args(args)

    max_aliases = args.max_aliases_rendered if args.max_aliases_rendered > 0 else None
    eval_variant = RenderVariant(args.eval_render_variant)

    train_paths = _resolve_train_paths(args.train_data, args.train_data_dir)
    concept_store_dirs = [Path(p) for p in args.concept_store_dir]
    include_vocabularies = set(args.include_vocabularies) if args.include_vocabularies else None
    exclude_vocabularies = set(args.exclude_vocabularies) if args.exclude_vocabularies else None

    if args.evaluate_only:
        eval_groups = _load_eval_groups(
            train_paths, args.eval_fraction, args.split_seed,
            include_vocabularies, exclude_vocabularies,
        )
        concept_store = _load_concept_store(
            concept_store_dirs, include_vocabularies, exclude_vocabularies,
        )
        eval_examples = _build_candidate_sets(
            eval_groups, concept_store, eval_variant, max_aliases,
        )
        print(f'Full held-out candidate-set evaluation examples: {len(eval_examples)}', flush=True)
        if not eval_examples:
            raise SystemExit('No held-out candidate sets could be built.')
        from pylate import models
        model_path = args.evaluation_model or str(Path(args.output_dir) / 'final')
        model = models.ColBERT(model_name_or_path=model_path)
        result = _run_candidate_set_evaluation(
            model, eval_examples, args.eval_batch_size,
            prediction_output=Path(args.prediction_output) if args.prediction_output else None,
        )
        result_path = Path(args.evaluation_result) if args.evaluation_result else Path(args.output_dir) / 'final_eval_result.json'
        with result_path.open('w', encoding='utf-8') as handle:
            json.dump(result, handle, indent=2)
        print(f'Primary candidate-set evaluation (global): {result.get("global")}', flush=True)
        print(f'Saved full per-vocabulary results to {result_path}', flush=True)
        return

    all_groups = _load_groups(
        train_paths, include_vocabularies, exclude_vocabularies, args.ranking_score_key,
        compact_model_candidates=(args.negatives_per_query
                                  if args.negative_sampling == 'model_stratified' else None),
        eval_fraction=args.eval_fraction,
        split_seed=args.split_seed,
        sampling_seed=args.seed,
    )
    all_groups, duplicate_count = _deduplicate_groups(all_groups)
    if duplicate_count:
        print(f'Deduplication: removed {duplicate_count} repeated query groups.')
    concept_store = _load_concept_store(
        concept_store_dirs,
        include_vocabularies,
        exclude_vocabularies,
    )
    print(f'Loaded {len(all_groups)} query groups and {len(concept_store)} concepts.')

    if all_groups:
        resolved = sum(1 for g in all_groups if (g['prefix'], g['gold_concept_id']) in concept_store)
        resolution_fraction = resolved / len(all_groups)
        if resolution_fraction < MIN_CONCEPT_RESOLUTION_FRACTION:
            raise SystemExit(
                f'Only {resolution_fraction:.1%} of loaded query groups resolved a gold concept '
                f'in the concept store (< {MIN_CONCEPT_RESOLUTION_FRACTION:.0%} threshold) -- '
                f'check --concept-store-dir points at the store(s) produced for this --train-data.'
            )

    train_groups, eval_groups = _split_by_concept(all_groups, args.eval_fraction, args.split_seed)
    print(f'Concept-grouped split: {len(train_groups)} train groups, {len(eval_groups)} eval groups.')

    ambiguous_keys = _ambiguous_query_keys(all_groups)
    ambiguous_train_count = sum(
        (g['prefix'], _normalise_query(g['query'])) in ambiguous_keys for g in train_groups
    )
    print(
        f'Ambiguity audit: {len(ambiguous_keys)} normalised query keys map to multiple gold '
        f'concepts ({ambiguous_train_count} train groups).'
    )
    if args.drop_ambiguous_training_queries or args.drop_contextless_training_queries:
        train_groups, quality_counts = _filter_training_quality(
            train_groups, ambiguous_keys,
            args.drop_ambiguous_training_queries,
            args.drop_contextless_training_queries,
            Path(args.quality_review_output) if args.quality_review_output else None,
            args.quality_review_limit,
        )
        print(f'Quality filtering: {len(train_groups)} train groups retained; '
              f'exclusions by reason/vocabulary: {json.dumps(quality_counts, sort_keys=True)}')

    if os.environ.get('RANK', '0') == '0':
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        with (output_dir / 'run_config.json').open('w', encoding='utf-8') as handle:
            json.dump(vars(args), handle, indent=2, sort_keys=True)

    eval_examples = _build_candidate_sets(eval_groups, concept_store, eval_variant, max_aliases)
    if not eval_examples:
        raise SystemExit(
            'No evaluation candidate sets could be built (need a resolvable gold concept plus '
            'at least one resolvable negative per eval group) -- increase --eval-fraction, mine '
            'more negatives, or check --concept-store-dir.'
        )
    print(f'Held-out candidate-set evaluation examples: {len(eval_examples)}')

    sampled_groups = _vocab_balanced_sample(
        train_groups, alpha=args.vocab_sampling_alpha, target_size=args.max_groups, seed=args.seed,
    )
    _report_sampling_diagnostics(sampled_groups)

    rows, _flatten_stats = _flatten_to_rows(
        sampled_groups, concept_store, args.negatives_per_query, seed=args.seed,
        max_aliases=max_aliases, preferred_label_keep_probability=args.preferred_label_query_keep_probability,
        negative_sampling=args.negative_sampling, ranking_score_key=args.ranking_score_key,
        training_objective=args.training_objective,
        distillation_temperature=args.distillation_temperature,
    )
    if not rows:
        raise SystemExit(
            'No trainable rows were produced -- check --train-data/--concept-store-dir point '
            'at matching mining output, that --negatives-per-query is not larger than what was '
            'mined (--min-negatives-per-query on the miner), and that --eval-fraction has not '
            'consumed the entire pool (it is a fraction of concepts, not rows).'
        )

    # Imported here, not at module load time, so `--help` and the argument-only validation
    # above work without the (heavy) ML stack installed.
    from datasets import Dataset
    from sentence_transformers import SentenceTransformerTrainer, SentenceTransformerTrainingArguments
    from pylate import evaluation, losses, models, utils
    from transformers import set_seed

    train_dataset = Dataset.from_list(rows)

    # SentenceTransformerTrainingArguments seeds training later, but the ColBERT projection is
    # created before Trainer construction. Seed explicitly so the step-zero model is reproducible.
    set_seed(args.seed)
    model = models.ColBERT(
        model_name_or_path=args.base_model,
        query_length=args.query_length,
        document_length=args.document_length,
    )
    # Not wrapped in torch.compile: SentenceTransformerTrainer iterates over the model's
    # submodules directly, which breaks once torch.compile wraps it in an OptimizedModule.

    _report_candidate_length_distribution(
        model, concept_store, eval_variant, max_aliases, document_length=args.document_length,
    )

    if args.training_objective == 'distillation':
        train_loss = losses.Distillation(model=model)
    elif args.cached_loss:
        train_loss = losses.CachedContrastive(
            model=model,
            mini_batch_size=args.cached_mini_batch_size,
            gather_across_devices=args.gather_across_devices,
            temperature=args.temperature,
        )
    else:
        train_loss = losses.Contrastive(
            model=model,
            gather_across_devices=args.gather_across_devices,
            temperature=args.temperature,
        )

    # Secondary smoke-test evaluator -- NOT the primary final evaluation metric.
    triplet_evaluator = None
    eval_dataset = None
    anchors, positives, negatives = _build_eval_triplets(eval_groups, concept_store)
    if anchors:
        triplet_evaluator = evaluation.ColBERTTripletEvaluator(
            anchors=anchors, positives=positives, negatives=negatives,
            name='held_out', batch_size=args.eval_batch_size,
        )
        eval_dataset = Dataset.from_dict({'query': anchors, 'positive': positives, 'negative_1': negatives})

    extra_args = json.loads(args.trainer_args_json) if args.trainer_args_json else {}

    periodic_examples = []
    if args.eval_steps > 0:
        periodic_groups = _stratified_eval_sample(
            eval_groups,
            target_size=(args.periodic_eval_max_groups if args.periodic_eval_max_groups > 0 else None),
            seed=args.split_seed,
        )
        periodic_examples = _build_candidate_sets(periodic_groups, concept_store, eval_variant, max_aliases)
        print(f'Periodic candidate-set evaluation examples: {len(periodic_examples)}')

    training_args = SentenceTransformerTrainingArguments(
        output_dir=args.output_dir,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.eval_batch_size,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        learning_rate=args.learning_rate,
        warmup_ratio=args.warmup_ratio,
        bf16=args.bf16,
        fp16=args.fp16,
        seed=args.seed,
        logging_steps=args.logging_steps,
        save_steps=args.save_steps,
        save_total_limit=args.save_total_limit,
        eval_strategy='steps' if periodic_examples else 'no',
        eval_steps=args.eval_steps if args.eval_steps > 0 else None,
        load_best_model_at_end=bool(periodic_examples),
        metric_for_best_model='eval_candidate_mrr' if periodic_examples else None,
        greater_is_better=True if periodic_examples else None,
        report_to=extra_args.pop('report_to', 'none'),
        **extra_args,
    )

    trainer = SentenceTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        # The held-out Dataset is only needed by the cheap final triplet smoke test. Periodic
        # selection is performed directly by CandidateSetEvaluator, avoiding a redundant loss
        # pass over all held-out triplets before every ranking evaluation.
        eval_dataset=None,
        loss=train_loss,
        # SentenceTransformerTrainer wraps supplied evaluators in SequentialEvaluator and
        # therefore expects an iterable even when there is only one primary evaluator.
        evaluator=([CandidateSetEvaluator(periodic_examples, args.eval_batch_size)]
                   if periodic_examples else None),
        data_collator=utils.ColBERTCollator(model.tokenize),
    )

    trainer.train(resume_from_checkpoint=args.resume_from_checkpoint)

    final_dir = Path(args.output_dir) / 'final'
    model.save_pretrained(str(final_dir))
    print(f'Saved final model to {final_dir}')

    if triplet_evaluator is not None:
        triplet_result = triplet_evaluator(model)
        print(f'Secondary triplet-evaluator smoke test (NOT the primary metric): {triplet_result}')
        with (Path(args.output_dir) / 'final_eval_triplet_result.json').open('w', encoding='utf-8') as f:
            json.dump(triplet_result, f, indent=2)

    candidate_set_result = _run_candidate_set_evaluation(model, eval_examples, batch_size=args.eval_batch_size)
    result_path = Path(args.output_dir) / 'final_eval_result.json'
    with result_path.open('w', encoding='utf-8') as f:
        json.dump(candidate_set_result, f, indent=2)
    print(f'Primary candidate-set evaluation (global): {candidate_set_result.get("global")}')
    print(f'(full per-vocabulary breakdown saved to {result_path} -- compare this file across '
          f'dataset-size runs to decide whether to grow the dataset further)')


if __name__ == '__main__':
    main()
