#!/usr/bin/env python3
"""Read-only live recall probe for mined queries or retrieval-miss manifests.

Uses CPU embeddings by default. Query a small, chosen input shard to validate whether
deeper per-arm retrieval actually improves the production-style fused top-k ceiling.
No model training, database writes, or gold insertion is performed.
"""
import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path

from bioterms.database import get_active_doc_db, get_active_vector_db
from bioterms.database.doc_db.doc_db import normalise_search_query
from bioterms.embedding import TextTransformer
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind


def _unique_ids(items) -> list[str]:
    return list(dict.fromkeys(item[0] for item in items))


def _fused_gold_rank(gold: str, arms: dict[str, list[str]], depth: int,
                     rrf_k: int) -> int | None:
    scores: dict[str, float] = {}
    for ids in arms.values():
        for rank, concept_id in enumerate(ids[:depth], start=1):
            scores[concept_id] = scores.get(concept_id, 0.0) + 1.0 / (rrf_k + rank)
    if gold not in scores:
        return None
    gold_score = scores[gold]
    return 1 + sum(score > gold_score for concept_id, score in scores.items()
                   if concept_id != gold)


def _select_rows(path: Path, skip: int, limit: int) -> list[dict]:
    rows = []
    with path.open(encoding='utf-8') as handle:
        for index, line in enumerate(handle):
            if index < skip or not line.strip():
                continue
            rows.append(json.loads(line))
            if len(rows) >= limit:
                break
    return rows


async def probe(rows: list[dict], depths: list[int], rrf_k: int) -> dict:
    transformer = TextTransformer()
    vectors = transformer.embed_strings([row['query'] for row in rows])
    doc_db = await get_active_doc_db()
    vector_db = get_active_vector_db()
    summary: Counter = Counter()
    examples = []
    max_depth = max(depths)
    try:
        for row, query_vector in zip(rows, vectors):
            prefix = ConceptPrefix(row['prefix'])
            gold = row['gold_concept_id']
            lexical, alias, definition = await asyncio.gather(
                doc_db.lexical_search(prefix, row['query'], limit=max_depth),
                vector_db.search_items(query_vector, prefix, EmbeddingKind.ALIAS,
                                       limit=max_depth),
                vector_db.search_items(query_vector, prefix, EmbeddingKind.DEFINITION,
                                       limit=max_depth),
            )
            raw_arms = {
                'lexical': lexical,
                'alias_embedding': alias,
                'definition_embedding': definition,
            }
            summary['queries'] += 1
            if not normalise_search_query(row['query']).words:
                summary['no_lexical_tokens'] += 1
            detail = {
                'prefix': row['prefix'], 'query_id': row.get('query_id'),
                'query': row['query'], 'gold_concept_id': gold,
                'raw_item_hits': {
                    'alias_embedding': len(alias), 'definition_embedding': len(definition),
                },
                'distinct_concept_hits': {
                    arm: len(_unique_ids(items)) for arm, items in raw_arms.items()
                },
                'depths': {},
            }
            for depth in depths:
                # The backend limit is on *items*, not distinct concepts. Truncate first,
                # then deduplicate exactly as the production search path does.
                arms = {arm: _unique_ids(items[:depth])
                        for arm, items in raw_arms.items()}
                arm_hits = {arm: gold in ids for arm, ids in arms.items()}
                fused_rank = _fused_gold_rank(gold, arms, depth, rrf_k)
                summary[f'union@{depth}'] += int(any(arm_hits.values()))
                summary[f'rrf_top50@{depth}'] += int(fused_rank is not None and fused_rank <= 50)
                for arm, found in arm_hits.items():
                    summary[f'{arm}@{depth}'] += int(found)
                detail['depths'][str(depth)] = {
                    'arm_hits': arm_hits, 'fused_rank': fused_rank,
                }
            examples.append(detail)
    finally:
        await doc_db.close()
        await vector_db.close()
    return {'summary': dict(summary), 'examples': examples,
            'tie_policy': 'optimistic',
            'note': 'Live single-vocabulary recall, without exact-match pinning or gold insertion.'}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='Mined JSONL or retrieval-miss manifest.')
    parser.add_argument('--output', required=True, help='Result JSON path.')
    parser.add_argument('--skip', type=int, default=0)
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--depths', type=int, nargs='+', default=[50, 100])
    parser.add_argument('--rrf-k', type=int, default=60)
    parser.add_argument('--device', default='cpu', help='Embedding device; defaults to CPU.')
    args = parser.parse_args()
    if args.skip < 0 or args.limit < 1 or not args.depths or min(args.depths) < 1 or args.rrf_k < 0:
        parser.error('--skip must be nonnegative; --limit/depths positive; --rrf-k nonnegative')
    path = Path(args.input)
    if not path.is_file():
        parser.error(f'Input file not found: {path}')
    rows = _select_rows(path, args.skip, args.limit)
    if not rows:
        parser.error('No input rows selected')
    CONFIG.torch_device = args.device
    result = asyncio.run(probe(rows, sorted(set(args.depths)), args.rrf_k))
    result['input'] = str(path)
    result['device'] = args.device
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(result['summary'], sort_keys=True))


if __name__ == '__main__':
    main()
