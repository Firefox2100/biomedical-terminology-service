#!/usr/bin/env python3
"""Train and evaluate a ColBERT-style reranker from mined query groups.

This standalone script depends on the ML stack but not the service or its databases.
"""
import argparse
import hashlib
import json
import random
from collections import defaultdict
from pathlib import Path

from concept_rendering import ALL_VARIANTS, RenderVariant, render_concept


# Below this fraction of loaded groups resolving a gold concept, --concept-store-dir almost
# certainly doesn't match --train-data -- fail fast rather than train on a near-empty dataset.
MIN_CONCEPT_RESOLUTION_FRACTION = 0.5


def _load_groups(paths: list[Path]) -> list[dict]:
    """Load every query-group record from the given JSONL files (not lazy: sampling/splitting need the full pool first)."""
    groups: list[dict] = []
    for path in paths:
        with path.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    groups.append(json.loads(line))
    return groups


def _load_concept_store(dirs: list[Path]) -> dict[tuple[str, str], dict]:
    """Load every "<prefix>.concepts.jsonl" in the given directories into a (prefix, concept_id) -> fields lookup."""
    store: dict[tuple[str, str], dict] = {}
    for directory in dirs:
        for path in sorted(directory.glob('*.concepts.jsonl')):
            prefix = path.name.removesuffix('.concepts.jsonl')
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


def _flatten_to_rows(sampled_groups: list[dict],
                     concept_store: dict[tuple[str, str], dict],
                     negatives_per_query: int,
                     seed: int,
                     max_aliases: int | None,
                     preferred_label_keep_probability: float,
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
        seen_negative_ids: set[str] = set()
        for negative in group.get('negatives') or []:
            concept_id = negative['concept_id']
            if concept_id in seen_negative_ids:
                continue
            rendered = _render_negative(group, negative, concept_store, seed, sample_index, max_aliases)
            if rendered:
                negatives.append(rendered)
                seen_negative_ids.add(concept_id)
            if len(negatives) >= negatives_per_query:
                break

        if len(negatives) < negatives_per_query:
            stats['skipped_insufficient_resolvable_negatives'] += 1
            continue

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
        candidate_texts = [render_concept(
            label=gold_concept['label'], synonyms=gold_concept['synonyms'], definition=gold_concept['definition'],
            variant=eval_variant, max_aliases=max_aliases,
            alias_selection_key=f'{group["prefix"]}:{group["gold_concept_id"]}',
        )]
        seen_ids = {group['gold_concept_id']}

        for negative in group.get('negatives') or []:
            concept_id = negative['concept_id']
            if concept_id in seen_ids:
                continue
            neg_concept = concept_store.get((group['prefix'], concept_id))
            if neg_concept is None:
                continue
            candidate_ids.append(concept_id)
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
            'candidate_ids': candidate_ids,
            'candidate_texts': candidate_texts,
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


def _run_candidate_set_evaluation(model, examples: list[dict], batch_size: int) -> dict:
    """Score every example's full candidate set with the trained model via pylate.rank.rerank; return metrics global + per vocabulary."""
    from pylate import rank

    if not examples:
        return {}

    queries = [ex['query'] for ex in examples]
    documents = [ex['candidate_texts'] for ex in examples]
    documents_ids = [ex['candidate_ids'] for ex in examples]

    query_embeddings = model.encode(queries, is_query=True, batch_size=batch_size, show_progress_bar=False)
    document_embeddings = model.encode(documents, is_query=False, batch_size=batch_size, show_progress_bar=False)

    reranked = rank.rerank(
        documents_ids=documents_ids, queries_embeddings=query_embeddings, documents_embeddings=document_embeddings,
    )

    ranks_by_vocab: dict[str, list[int | None]] = defaultdict(list)
    for example, results in zip(examples, reranked):
        ranked_ids = [r['id'] for r in results]
        try:
            gold_rank = ranked_ids.index(example['gold_concept_id']) + 1
        except ValueError:
            gold_rank = None
        ranks_by_vocab[example['prefix']].append(gold_rank)

    metrics = {'global': _candidate_set_metrics([r for ranks in ranks_by_vocab.values() for r in ranks])}
    for prefix, ranks in ranks_by_vocab.items():
        metrics[prefix] = _candidate_set_metrics(ranks)
    return metrics


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
        '--train-data', nargs='+', required=True,
        help='Mined query-group JSONL shard file(s) -- the full pool; a held-out split is carved out internally (see --eval-fraction).',
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
    parser.add_argument('--output-dir', required=True, help='Directory to write checkpoints and the final model to.')
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
        help='Run the secondary smoke-test evaluator every N steps (0 disables). The primary evaluation always runs once after training.',
    )
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

    if errors:
        raise SystemExit('Invalid arguments:\n' + '\n'.join(f'  - {e}' for e in errors))


def main() -> None:
    args = _parse_args()
    _validate_args(args)

    max_aliases = args.max_aliases_rendered if args.max_aliases_rendered > 0 else None
    eval_variant = RenderVariant(args.eval_render_variant)

    train_paths = [Path(p) for p in args.train_data]
    concept_store_dirs = [Path(p) for p in args.concept_store_dir]

    all_groups = _load_groups(train_paths)
    concept_store = _load_concept_store(concept_store_dirs)
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

    train_dataset = Dataset.from_list(rows)

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

    if args.cached_loss:
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
        eval_strategy='steps' if (triplet_evaluator is not None and args.eval_steps > 0) else 'no',
        eval_steps=args.eval_steps if args.eval_steps > 0 else None,
        report_to=extra_args.pop('report_to', 'none'),
        **extra_args,
    )

    trainer = SentenceTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        loss=train_loss,
        evaluator=triplet_evaluator,
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
