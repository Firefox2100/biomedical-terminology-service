#!/usr/bin/env python3
"""Audit saved mining recall and approximate production RRF without re-querying a database.

The mined pool records each arm's rank for the gold and negative concepts. This script
replays those ranks with configurable per-arm depth and RRF width. It does *not* measure
gold-absent queries discarded by the cross-vocabulary miner, exact-match pinning, or live
backend/quantization effects. Use the original shard statistics for discarded counts.
"""
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


ARMS = ('lexical', 'fuzzy_lexical', 'alias_embedding', 'definition_embedding', 'exact_mapping')
DEPTHS = (1, 5, 10, 20, 50, 100)


def _gold_ranks(group: dict) -> dict[str, int]:
    evidence = group.get('gold_retrieval') or group.get('gold_retrieval_evidence') or {}
    return evidence.get('ranks') or {}


def _rrf_rank(group: dict, arm_depth: int, rrf_k: int) -> int | None:
    """Rank the gold in the recorded union; None means no arm retrieved it."""
    gold_ranks = _gold_ranks(group)
    gold_score = sum(1.0 / (rrf_k + rank)
                     for rank in gold_ranks.values() if rank <= arm_depth)
    if not gold_score:
        return None
    ahead = 0
    for candidate in group.get('candidate_pool') or []:
        score = sum(1.0 / (rrf_k + rank)
                    for rank in (candidate.get('ranks') or {}).values()
                    if rank <= arm_depth)
        if score > gold_score:
            ahead += 1
    # Tied candidates are not counted ahead: this is an optimistic bound because the
    # backend's tie/insertion order cannot be reconstructed from candidate_pool alone.
    return ahead + 1


def _counts(group: dict, arm_depth: int, rrf_k: int) -> Counter:
    ranks = _gold_ranks(group)
    result: Counter = Counter({'rows': 1})
    result['candidate_pool_total'] = len(group.get('candidate_pool') or [])
    if not ranks:
        result['gold_absent'] = 1
    for depth in DEPTHS:
        for arm in ARMS:
            if ranks.get(arm, arm_depth + depth + 1) <= depth:
                result[f'{arm}@{depth}'] = 1
        if any(rank <= depth for rank in ranks.values()):
            result[f'union@{depth}'] = 1
    rank = _rrf_rank(group, arm_depth, rrf_k)
    if rank is not None:
        result['gold_rrf_rank_sum'] = rank
        for depth in DEPTHS:
            if rank <= depth:
                result[f'rrf@{depth}'] = 1
    return result


def audit(paths: list[Path], sample_modulus: int = 1, arm_depth: int = 50,
          rrf_k: int = 60, review_limit: int = 1000,
          ) -> tuple[dict, list[dict]]:
    if sample_modulus < 1 or arm_depth < 1 or rrf_k < 0 or review_limit < 0:
        raise ValueError('sample_modulus and arm_depth must be positive; rrf_k/review_limit nonnegative')
    global_counts: Counter = Counter()
    by_vocabulary: dict[str, Counter] = defaultdict(Counter)
    by_kind: dict[str, Counter] = defaultdict(Counter)
    review = []
    seen = 0
    for path in paths:
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                if not line.strip():
                    continue
                selected = seen % sample_modulus == 0
                seen += 1
                if not selected:
                    continue
                group = json.loads(line)
                counts = _counts(group, arm_depth, rrf_k)
                global_counts.update(counts)
                by_vocabulary[group['prefix']].update(counts)
                by_kind[group.get('query_kind', 'unknown')].update(counts)
                if len(review) < review_limit and not counts.get('rrf@50'):
                    review.append({
                        'prefix': group['prefix'],
                        'source_prefix': group.get('source_prefix'),
                        'query_id': group['query_id'],
                        'query': group['query'],
                        'gold_concept_id': group['gold_concept_id'],
                        'gold_ranks': _gold_ranks(group),
                        'rrf_rank': (_rrf_rank(group, arm_depth, rrf_k)
                                     if _gold_ranks(group) else None),
                        'candidate_pool_size': len(group.get('candidate_pool') or []),
                    })
    report = {
        'input_files': [str(path) for path in paths],
        'source_rows_seen': seen,
        'sample_modulus': sample_modulus,
        'arm_depth': arm_depth,
        'rrf_k': rrf_k,
        'tie_policy': 'optimistic',
        'note': ('Conditional on rows retained by the miner; RRF replay excludes exact-match '
                 'pinning and live backend effects. Inspect shard stats for dropped rows.'),
        'global': dict(global_counts),
        'by_vocabulary': {k: dict(v) for k, v in sorted(by_vocabulary.items())},
        'by_query_kind': {k: dict(v) for k, v in sorted(by_kind.items())},
    }
    return report, review


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', nargs='+', help='Mined raw or scored schema-v2 JSONL shards.')
    parser.add_argument('--output', required=True, help='Summary JSON path.')
    parser.add_argument('--review-output', help='Optional JSONL of gold-absent/RRF-beyond-50 rows.')
    parser.add_argument('--review-limit', type=int, default=1000)
    parser.add_argument('--sample-modulus', type=int, default=1,
                        help='Audit every Nth row (1 audits all rows).')
    parser.add_argument('--arm-depth', type=int, default=50,
                        help='Per-arm depth used in the approximate RRF replay.')
    parser.add_argument('--rrf-k', type=int, default=60)
    args = parser.parse_args()
    paths = [Path(value) for value in args.input]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        parser.error(f'Input file(s) not found: {", ".join(missing)}')
    try:
        report, review = audit(paths, args.sample_modulus, args.arm_depth,
                               args.rrf_k, args.review_limit)
    except ValueError as error:
        parser.error(str(error))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding='utf-8')
    if args.review_output:
        review_path = Path(args.review_output)
        review_path.parent.mkdir(parents=True, exist_ok=True)
        with review_path.open('w', encoding='utf-8') as handle:
            for row in review:
                handle.write(json.dumps(row, ensure_ascii=False) + '\n')
    print(json.dumps(report['global'], sort_keys=True))


if __name__ == '__main__':
    main()
