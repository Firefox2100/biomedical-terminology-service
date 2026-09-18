#!/usr/bin/env python3
"""Audit mined reranker JSONL for duplicate, ambiguous, and suspicious supervision.

This is read-only: it writes a JSON report and an optional JSONL review queue, never modifies
the mined shards. It has no bioterms, database, or ML dependency and is safe to run on HPC.
"""
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


def _normalise(text: str) -> str:
    return ' '.join(text.casefold().split())


def audit(paths: list[Path], review_limit: int = 1000) -> tuple[dict, list[dict]]:
    rows = 0
    by_vocabulary: Counter = Counter()
    by_query_kind: Counter = Counter()
    negative_counts: Counter = Counter()
    exact_keys: Counter = Counter()
    query_id_keys: Counter = Counter()
    examples_by_query: dict[tuple[str, str], list[dict]] = defaultdict(list)
    golds_by_query: dict[tuple[str, str], set[str]] = defaultdict(set)

    for path in paths:
        with path.open(encoding='utf-8') as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                group = json.loads(line)
                rows += 1
                prefix = group['prefix']
                query = _normalise(group['query'])
                by_vocabulary[prefix] += 1
                by_query_kind[group.get('query_kind', 'unknown')] += 1
                negative_counts[len(group.get('negatives') or [])] += 1
                exact_keys[(prefix, group['query_id'], group['gold_concept_id'], query)] += 1
                query_id_keys[(prefix, group['query_id'])] += 1
                key = (prefix, query)
                golds_by_query[key].add(group['gold_concept_id'])
                if len(examples_by_query[key]) < 20:
                    examples_by_query[key].append({
                        'file': str(path),
                        'line': line_number,
                        'query_id': group['query_id'],
                        'query': group['query'],
                        'query_kind': group.get('query_kind'),
                        'source_prefix': group.get('source_prefix'),
                        'gold_concept_id': group['gold_concept_id'],
                    })

    ambiguous = [(key, golds) for key, golds in golds_by_query.items() if len(golds) > 1]
    ambiguous.sort(key=lambda item: (-len(item[1]), item[0]))
    duplicate_groups = sum(count - 1 for count in exact_keys.values() if count > 1)

    report = {
        'files': [str(path) for path in paths],
        'rows': rows,
        'rows_by_vocabulary': dict(sorted(by_vocabulary.items())),
        'rows_by_query_kind': dict(sorted(by_query_kind.items())),
        'negative_count_distribution': {
            str(key): value for key, value in sorted(negative_counts.items())
        },
        'exact_duplicate_rows': duplicate_groups,
        'reused_query_id_keys': sum(count > 1 for count in query_id_keys.values()),
        'rows_beyond_first_per_query_id': sum(
            count - 1 for count in query_id_keys.values() if count > 1
        ),
        'unique_normalised_query_keys': len(golds_by_query),
        'ambiguous_query_keys': len(ambiguous),
        'ambiguous_query_fraction': len(ambiguous) / len(golds_by_query) if golds_by_query else 0.0,
        'max_golds_for_one_query': max((len(golds) for _key, golds in ambiguous), default=1),
    }
    review = [
        {
            'prefix': prefix,
            'normalised_query': query,
            'gold_concept_ids': sorted(golds),
            'gold_count': len(golds),
            'examples': examples_by_query[(prefix, query)],
        }
        for (prefix, query), golds in ambiguous[:review_limit]
    ]
    return report, review


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', nargs='+', help='Mined query-group JSONL shard(s).')
    parser.add_argument('--output', required=True, help='JSON summary report path.')
    parser.add_argument('--review-output', help='Optional JSONL queue of ambiguous queries.')
    parser.add_argument('--review-limit', type=int, default=1000)
    args = parser.parse_args()

    paths = [Path(value) for value in args.input]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise SystemExit(f'Input file(s) not found: {", ".join(missing)}')

    report, review = audit(paths, max(0, args.review_limit))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    if args.review_output:
        review_path = Path(args.review_output)
        review_path.parent.mkdir(parents=True, exist_ok=True)
        with review_path.open('w', encoding='utf-8') as handle:
            for record in review:
                handle.write(json.dumps(record, ensure_ascii=False) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
