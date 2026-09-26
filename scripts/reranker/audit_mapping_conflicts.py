#!/usr/bin/env python3
"""Review, without mutating data, context-free cross-vocabulary EXACT mapping conflicts.

An EXACT graph edge is evidence about two *concepts*, not proof that every short alias on
one side uniquely names the other. This streaming audit finds cross-vocabulary query rows
where another target concept has the same exact label/alias, or the gold has no such exact
surface form. It deliberately does not auto-reject them: cases like a disease phrase also
listed on its causal gene require expert review or query context.
"""

import argparse
import json
from collections import Counter
from pathlib import Path

from query_quality import contextless_query_reason, normalise_query


def _load_concepts(directory: Path) -> dict[tuple[str, str], dict]:
    concepts = {}
    for path in sorted(directory.glob('*.concepts.jsonl')):
        prefix = path.name.removesuffix('.concepts.jsonl')
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                concept = json.loads(line)
                concepts[(prefix, concept['concept_id'])] = concept
    return concepts


def _exact(concept: dict | None, query: str) -> bool:
    if not concept:
        return False
    return any(normalise_query(text) == query for text in
               [concept.get('label'), *(concept.get('synonyms') or [])] if text)


def classify_mapping_conflict(group: dict, concepts: dict[tuple[str, str], dict]) -> dict | None:
    """Return one review record for a suspicious cross-vocabulary query, if any."""
    if group.get('query_kind') != 'cross_vocab_exact':
        return None
    prefix = group['prefix']
    query = normalise_query(group['query'])
    gold_id = group['gold_concept_id']
    gold = concepts.get((prefix, gold_id))
    matching = []
    seen = set()
    for candidate in group.get('candidate_pool') or []:
        concept_id = candidate['concept_id']
        if concept_id in seen:
            continue
        seen.add(concept_id)
        concept = concepts.get((prefix, concept_id))
        if _exact(concept, query):
            matching.append({'concept_id': concept_id, 'label': concept.get('label')})
    gold_exact = _exact(gold, query)
    if gold_exact and gold_id not in seen:
        matching.append({'concept_id': gold_id, 'label': gold.get('label')})
    reasons = []
    if not gold_exact and matching:
        reasons.append('other_exact_match_but_not_gold')
    if len(matching) > 1:
        reasons.append('multiple_exact_candidates')
    generic = contextless_query_reason(group['query'])
    if generic:
        reasons.append(generic)
    if not reasons:
        return None
    return {
        'prefix': prefix,
        'source_prefix': group.get('source_prefix'),
        'query_id': group.get('query_id'),
        'query': group['query'],
        'gold_concept_id': gold_id,
        'gold_label': gold.get('label') if gold else None,
        'gold_exact': gold_exact,
        'exact_candidates': matching,
        'reasons': reasons,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--concept-store-dir', required=True)
    parser.add_argument('--output', required=True, help='JSON summary path.')
    parser.add_argument('--review-output', required=True, help='Bounded JSONL review queue path.')
    parser.add_argument('--review-limit', type=int, default=10_000)
    args = parser.parse_args()
    if args.review_limit < 0:
        parser.error('--review-limit must be >= 0')

    concepts = _load_concepts(Path(args.concept_store_dir))
    paths = sorted(Path(args.input_dir).glob('*.jsonl'))
    if not paths:
        parser.error('No JSONL shards found in --input-dir')
    output = Path(args.output)
    review_output = Path(args.review_output)
    output.parent.mkdir(parents=True, exist_ok=True)
    review_output.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    with review_output.open('w', encoding='utf-8') as review:
        for path in paths:
            with path.open(encoding='utf-8') as handle:
                for line in handle:
                    group = json.loads(line)
                    if group.get('query_kind') != 'cross_vocab_exact':
                        continue
                    counts['cross_vocab_rows'] += 1
                    record = classify_mapping_conflict(group, concepts)
                    if record is None:
                        continue
                    counts['flagged_rows'] += 1
                    counts[f'flagged_{record["prefix"]}'] += 1
                    counts.update(record['reasons'])
                    if counts['flagged_rows'] <= args.review_limit:
                        review.write(json.dumps(record, ensure_ascii=False) + '\n')
            print(path.name, dict(counts), flush=True)
    output.write_text(json.dumps(dict(counts), indent=2, sort_keys=True), encoding='utf-8')
    print(f'Saved summary to {output} and review queue to {review_output}')


if __name__ == '__main__':
    main()
