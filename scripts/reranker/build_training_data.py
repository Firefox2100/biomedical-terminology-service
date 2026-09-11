#!/usr/bin/env python3
"""
Mine a reranker training dataset from a fully built and embedded bioterms database.

See README.md in this folder for the full design (dataset format, determinism/incremental
growth, negative selection, scope/limitations). Requires the same BTS_* config as the rest of
the service, pointed at a database that has already been loaded AND embedded.
"""
import argparse
import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path

from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.database import DocumentDatabase, VectorDatabase, get_active_doc_db, get_active_vector_db
from bioterms.embedding import TextTransformer
from bioterms.model.concept import Concept, EmbeddingItem
from bioterms.vocabulary import get_vocabulary_config
from bioterms.vocabulary.utils import ALL_VOCABULARIES


# Best-rank bands used to diversify negative selection by retrieval difficulty (see
# _select_negatives).
RANK_BANDS: list[tuple[int, int]] = [(1, 5), (6, 20), (21, 50)]
RANK_BAND_LABELS: list[str] = ['very_hard', 'hard', 'medium']
OVERFLOW_BAND_LABEL = 'long_tail'

RECALL_SOURCES: list[str] = ['lexical', 'alias_embedding', 'definition_embedding']


def _stable_hash_int(*parts: str) -> int:
    """Deterministic hash of a tuple of strings to an integer."""
    digest = hashlib.sha256(':'.join(parts).encode('utf-8')).hexdigest()
    return int(digest[:16], 16)


@dataclass
class QueryUnit:
    """One not-yet-mined training query, before hard negatives are attached."""
    prefix: ConceptPrefix
    concept_id: str
    item: EmbeddingItem


@dataclass
class MiningStats:
    vocabularies: dict[str, int] = field(default_factory=dict)
    total_written: int = 0
    total_negatives: int = 0
    negatives_by_source: dict[str, int] = field(default_factory=dict)
    negatives_by_rank_band: dict[str, int] = field(default_factory=dict)
    duplicate_cross_source_hits_merged: int = 0
    rejected_by_equivalence_filter: int = 0
    skipped_below_min_negatives: int = 0

    def record(self, negatives: list[dict], duplicate_merges: int, rejected: int) -> None:
        self.total_negatives += len(negatives)
        self.duplicate_cross_source_hits_merged += duplicate_merges
        self.rejected_by_equivalence_filter += rejected
        for negative in negatives:
            for source in negative['sources']:
                self.negatives_by_source[source] = self.negatives_by_source.get(source, 0) + 1
            band = negative['rank_band']
            self.negatives_by_rank_band[band] = self.negatives_by_rank_band.get(band, 0) + 1

    def as_dict(self) -> dict:
        average_negatives = (self.total_negatives / self.total_written) if self.total_written else 0.0
        return {
            'query_units_per_vocabulary': self.vocabularies,
            'total_written': self.total_written,
            'total_negatives_mined': self.total_negatives,
            'average_negatives_per_query': average_negatives,
            'negatives_by_source': self.negatives_by_source,
            'negatives_by_rank_band': self.negatives_by_rank_band,
            'duplicate_cross_source_hits_merged': self.duplicate_cross_source_hits_merged,
            'rejected_by_equivalence_filter': self.rejected_by_equivalence_filter,
            'skipped_below_min_negatives': self.skipped_below_min_negatives,
        }


def is_valid_negative(prefix: ConceptPrefix,
                      gold_concept_id: str,
                      candidate_concept_id: str,
                      ) -> bool:
    """
    Single hook for rejecting a negative candidate equivalent to the gold concept (trusted
    same-as/replacement, not just a different id). No equivalence data is wired in yet, so
    this only rejects the gold concept itself -- extend here once that data exists.
    """
    del prefix  # unused for now -- kept in the signature for when equivalence data is wired in
    return candidate_concept_id != gold_concept_id


def _iter_vocabularies(requested: list[str] | None) -> list[ConceptPrefix]:
    """Resolve and sort (by prefix value, for determinism) the vocabularies to mine."""
    if requested:
        prefixes = [ConceptPrefix(value) for value in requested]
    else:
        prefixes = list(ALL_VOCABULARIES.keys())

    return sorted(prefixes, key=lambda p: p.value)


def _build_query_units(prefix: ConceptPrefix,
                       concepts: dict[str, Concept],
                       max_queries_per_concept: int,
                       ) -> list[QueryUnit]:
    """
    Enumerate query units for one vocabulary's concepts (sorted by concept_id). Each concept
    contributes at most `max_queries_per_concept` alias items, chosen by sorting aliases on a
    stable hash rather than taking the first N (see README) -- deterministic, but not biased
    toward the same "front of the list" aliases every time.
    """
    units: list[QueryUnit] = []

    for concept_id in sorted(concepts.keys()):
        concept = concepts[concept_id]
        alias_items = [i for i in concept.embedding_items() if i.kind == EmbeddingKind.ALIAS]
        alias_items.sort(key=lambda item: _stable_hash_int(prefix.value, concept_id, item.item_id))

        for query_item in alias_items[:max_queries_per_concept]:
            units.append(QueryUnit(prefix=prefix, concept_id=concept_id, item=query_item))

    return units


def _band_targets(total: int) -> list[int]:
    """
    Split `total` across RANK_BANDS as evenly as possible, remainder going to later bands
    first (e.g. 6 -> [2, 2, 2], 7 -> [2, 2, 3]).
    """
    if total <= 0:
        return [0] * len(RANK_BANDS)

    n = len(RANK_BANDS)
    base = total // n
    remainder = total % n
    targets = [base] * n

    idx = n - 1
    while remainder > 0:
        targets[idx] += 1
        idx = (idx - 1) % n
        remainder -= 1

    return targets


def _band_label(best_rank: int) -> str:
    for (lo, hi), label in zip(RANK_BANDS, RANK_BAND_LABELS):
        if lo <= best_rank <= hi:
            return label
    return OVERFLOW_BAND_LABEL


def _select_negatives(candidates: dict[str, dict],
                      negatives_per_query: int,
                      ) -> list[dict]:
    """
    Select up to `negatives_per_query` from the merged, filtered candidate pool in three
    passes (see README for rationale): (1) source coverage -- one negative per recall arm
    that found anything; (2) rank-band quotas for the remaining budget; (3) backfill from the
    best leftover candidates regardless of band.
    :param candidates: concept_id -> {"sources", "ranks", "scores"}, already filtered via
        `is_valid_negative`.
    :return: Negative dicts (concept_id, sources, ranks, scores, rank_band), best-rank first.
    """
    if negatives_per_query <= 0 or not candidates:
        return []

    for info in candidates.values():
        info['best_rank'] = min(info['ranks'].values())

    selected_ids: list[str] = []
    selected_set: set[str] = set()

    # Pass 1: source coverage.
    for source in RECALL_SOURCES:
        if len(selected_ids) >= negatives_per_query:
            break
        source_hits = [
            (concept_id, info) for concept_id, info in candidates.items()
            if source in info['ranks'] and concept_id not in selected_set
        ]
        if not source_hits:
            continue
        concept_id, _info = min(source_hits, key=lambda kv: kv[1]['ranks'][source])
        selected_ids.append(concept_id)
        selected_set.add(concept_id)

    # Pass 2: rank-band quotas, from whatever budget pass 1 left.
    remaining_budget = negatives_per_query - len(selected_ids)
    leftover: list[tuple[str, dict]] = []
    if remaining_budget > 0:
        remaining_pool = sorted(
            ((cid, info) for cid, info in candidates.items() if cid not in selected_set),
            key=lambda kv: kv[1]['best_rank'],
        )

        banded: list[list[tuple[str, dict]]] = [[] for _ in RANK_BANDS]
        overflow: list[tuple[str, dict]] = []
        for concept_id, info in remaining_pool:
            band_label = _band_label(info['best_rank'])
            if band_label in RANK_BAND_LABELS:
                banded[RANK_BAND_LABELS.index(band_label)].append((concept_id, info))
            else:
                overflow.append((concept_id, info))

        targets = _band_targets(remaining_budget)
        for band_list, target in zip(banded, targets):
            for concept_id, _info in band_list[:target]:
                selected_ids.append(concept_id)
                selected_set.add(concept_id)
            leftover.extend(band_list[target:])
        leftover.extend(overflow)

    # Pass 3: backfill from the best remaining candidates regardless of band.
    if len(selected_ids) < negatives_per_query:
        leftover.sort(key=lambda kv: kv[1]['best_rank'])
        for concept_id, _info in leftover:
            if len(selected_ids) >= negatives_per_query:
                break
            if concept_id not in selected_set:
                selected_ids.append(concept_id)
                selected_set.add(concept_id)

    selected_ids.sort(key=lambda cid: candidates[cid]['best_rank'])

    return [
        {
            'concept_id': concept_id,
            'sources': sorted(set(candidates[concept_id]['sources'])),
            'ranks': candidates[concept_id]['ranks'],
            'scores': candidates[concept_id]['scores'],
            'rank_band': _band_label(candidates[concept_id]['best_rank']),
        }
        for concept_id in selected_ids[:negatives_per_query]
    ]


async def _mine_negatives(doc_db: DocumentDatabase,
                          vector_db: VectorDatabase,
                          transformer: TextTransformer,
                          unit: QueryUnit,
                          negatives_per_query: int,
                          candidate_pool: int,
                          ) -> tuple[list[dict], int, int]:
    """
    Run the query through the lexical/alias-embedding/definition-embedding recall arms,
    aggregate hits by concept_id (a concept hit by several arms becomes one candidate with all
    evidence attached), reject invalid candidates, and select negatives.
    :return: (negatives, duplicate_cross_source_merges, rejected_by_equivalence_filter).
    """
    query_vector = transformer.embed_strings([unit.item.text])[0]

    lexical_task = doc_db.lexical_search(unit.prefix, unit.item.text, limit=candidate_pool)
    alias_task = vector_db.search_items(query_vector, unit.prefix, EmbeddingKind.ALIAS, limit=candidate_pool)
    definition_task = vector_db.search_items(
        query_vector, unit.prefix, EmbeddingKind.DEFINITION, limit=candidate_pool,
    )
    lexical_hits, alias_hits, definition_hits = await asyncio.gather(lexical_task, alias_task, definition_task)

    merged: dict[str, dict] = {}
    duplicate_merges = 0

    def add_hits(hits, source: str) -> None:
        nonlocal duplicate_merges
        rank = 0
        seen_this_arm: set[str] = set()
        for hit in hits:
            concept_id, score = (hit[0], hit[-1])
            if concept_id in seen_this_arm:
                # Two items of the same concept (e.g. two synonyms) both matched within this
                # arm -- keep only the better (first-seen) rank.
                continue
            seen_this_arm.add(concept_id)
            rank += 1

            entry = merged.get(concept_id)
            if entry is None:
                merged[concept_id] = {
                    'sources': [source],
                    'ranks': {source: rank},
                    'scores': {source: float(score)},
                }
            else:
                duplicate_merges += 1
                entry['sources'].append(source)
                entry['ranks'][source] = rank
                entry['scores'][source] = float(score)

    add_hits(lexical_hits, 'lexical')
    add_hits(alias_hits, 'alias_embedding')
    add_hits(definition_hits, 'definition_embedding')

    rejected = 0
    for concept_id in list(merged.keys()):
        if not is_valid_negative(unit.prefix, unit.concept_id, concept_id):
            del merged[concept_id]
            rejected += 1

    negatives = _select_negatives(merged, negatives_per_query)

    return negatives, duplicate_merges, rejected


def _write_concept_store(concept_store_dir: Path,
                         prefix: ConceptPrefix,
                         concepts: dict[str, Concept],
                         ) -> None:
    """
    Write (overwriting) this vocabulary's concept store: one JSONL line per concept with the
    raw label/synonyms/definition fields (never rendered text) train_reranker.py needs.
    """
    concept_store_dir.mkdir(parents=True, exist_ok=True)
    path = concept_store_dir / f'{prefix.value}.concepts.jsonl'

    with path.open('w', encoding='utf-8') as f:
        for concept_id in sorted(concepts.keys()):
            concept = concepts[concept_id]
            f.write(json.dumps({
                'concept_id': concept_id,
                'label': concept.label,
                'synonyms': concept.synonyms or [],
                'definition': concept.definition,
            }, ensure_ascii=False) + '\n')


async def _run(args: argparse.Namespace) -> None:
    doc_db = await get_active_doc_db()
    vector_db = get_active_vector_db()
    transformer = TextTransformer()

    stats = MiningStats()
    global_index = 0
    written = 0
    semaphore = asyncio.Semaphore(args.concurrency)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    concept_store_dir = Path(args.concept_store_dir) if args.concept_store_dir \
        else output_path.parent / 'concepts'

    # Persisted per-vocabulary unit-count cache, so a later --skip can bypass reloading
    # vocabularies entirely before the requested window (see README). Delete it if the
    # underlying vocabularies have changed since it was written.
    manifest_path = output_path.parent / '.reranker_vocab_unit_counts.json'
    manifest: dict[str, int] = {}
    if manifest_path.exists():
        with manifest_path.open(encoding='utf-8') as manifest_file:
            manifest = json.load(manifest_file)

    start_time = time.perf_counter()

    per_vocab_limit = args.per_vocabulary_limit

    with output_path.open('w', encoding='utf-8') as out_file:
        for prefix in _iter_vocabularies(args.vocabularies):
            if per_vocab_limit is None and global_index >= args.skip + args.limit:
                break

            cached_count = manifest.get(prefix.value)
            if (per_vocab_limit is None and cached_count is not None
                    and global_index + cached_count <= args.skip):
                # This whole vocabulary is before the requested window -- trust the cached
                # count and skip loading its concepts entirely. Only applies to the global
                # --skip/--limit mode; --per-vocabulary-limit always visits every vocabulary.
                global_index += cached_count
                stats.vocabularies[prefix.value] = cached_count
                print(f'[{prefix.value}] before --skip window, using cached count ({cached_count})')
                continue

            config = get_vocabulary_config(prefix)
            concepts: dict[str, Concept] = {}
            async for concept in doc_db.get_terms_iter(prefix=prefix, model_class=config['conceptClass']):
                concepts[concept.concept_id] = concept

            if not concepts:
                print(f'[{prefix.value}] no concepts loaded, skipping')
                manifest[prefix.value] = 0
                continue

            _write_concept_store(concept_store_dir, prefix, concepts)

            units = _build_query_units(prefix, concepts, args.max_queries_per_concept)
            manifest[prefix.value] = len(units)
            stats.vocabularies[prefix.value] = len(units)
            print(f'[{prefix.value}] {len(concepts)} concepts, {len(units)} candidate query units')

            # Global mode: --skip/--limit slice the one cross-vocabulary sequence (units
            # before the window are walked to advance the index, never mined). Per-vocabulary
            # mode: the same values slice *this vocabulary's own* sequence, restarting at 0
            # each time -- see README on why this guarantees per-vocabulary coverage.
            window_skip, window_limit = args.skip, (per_vocab_limit if per_vocab_limit is not None else args.limit)
            window_base = 0 if per_vocab_limit is not None else global_index
            in_window = [
                (idx, u) for idx, u in enumerate(units, start=window_base)
                if window_skip <= idx < window_skip + window_limit
            ]
            global_index += len(units)

            async def process_one(unit: QueryUnit) -> tuple[dict | None, int, int, bool]:
                async with semaphore:
                    negatives, duplicate_merges, rejected = await _mine_negatives(
                        doc_db, vector_db, transformer, unit,
                        negatives_per_query=args.negatives_per_query,
                        candidate_pool=args.candidate_pool,
                    )
                if len(negatives) < args.min_negatives_per_query:
                    return None, duplicate_merges, rejected, True
                record = {
                    'query_id': unit.item.item_id,
                    'prefix': unit.prefix.value,
                    'gold_concept_id': unit.concept_id,
                    'query': unit.item.text,
                    'query_kind': 'alias',
                    'negatives': negatives,
                }
                return record, duplicate_merges, rejected, False

            vocab_written = 0
            for batch_start in range(0, len(in_window), args.batch_size):
                batch = in_window[batch_start:batch_start + args.batch_size]
                results = await asyncio.gather(*(process_one(unit) for _idx, unit in batch))

                for record, duplicate_merges, rejected, below_min in results:
                    if below_min:
                        stats.skipped_below_min_negatives += 1
                        continue
                    out_file.write(json.dumps(record, ensure_ascii=False) + '\n')
                    written += 1
                    vocab_written += 1
                    stats.record(record['negatives'], duplicate_merges, rejected)

                elapsed = time.perf_counter() - start_time
                target = window_limit if per_vocab_limit is not None else args.limit
                progress = vocab_written if per_vocab_limit is not None else written
                print(
                    f'[{prefix.value}] written {progress}/{target} '
                    f'({elapsed:.0f}s elapsed, {written / elapsed:.1f} units/s overall)'
                )

                if per_vocab_limit is not None:
                    if vocab_written >= per_vocab_limit:
                        break
                elif written >= args.limit:
                    break

    stats.total_written = written

    with manifest_path.open('w', encoding='utf-8') as manifest_file:
        json.dump(manifest, manifest_file, indent=2)

    stats_path = output_path.with_suffix(output_path.suffix + '.stats.json')
    with stats_path.open('w', encoding='utf-8') as stats_file:
        json.dump({
            'skip': args.skip,
            'limit': args.limit,
            'per_vocabulary_limit': args.per_vocabulary_limit,
            'elapsed_seconds': time.perf_counter() - start_time,
            **stats.as_dict(),
        }, stats_file, indent=2)

    print(f'Done. Wrote {written} query units to {output_path} (stats: {stats_path}).')
    await doc_db.close()
    await vector_db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Mine a reranker training dataset from a built bioterms database. See README.md.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--output', required=True, help='Output JSONL path (a "<output>.stats.json" is written alongside it).')
    parser.add_argument(
        '--concept-store-dir', default=None,
        help='Directory for per-vocabulary "<prefix>.concepts.jsonl" files. Defaults to a '
             '"concepts" subdirectory next to --output.',
    )
    parser.add_argument(
        '--skip', type=int, default=0,
        help='Query units to skip before mining starts -- see README for the incremental-growth workflow.',
    )
    parser.add_argument('--limit', type=int, default=100_000, help='Query units to mine and write in this run.')
    parser.add_argument(
        '--per-vocabulary-limit', type=int, default=None,
        help='Mine up to this many units from EACH vocabulary instead of one global --limit '
             '(re-interprets --skip/--limit as per-vocabulary) -- see README.',
    )
    parser.add_argument(
        '--vocabularies', nargs='*', default=None,
        help='Restrict to these vocabulary prefixes. Defaults to every vocabulary with data loaded.',
    )
    parser.add_argument(
        '--max-queries-per-concept', type=int, default=4,
        help='Cap on query units per concept, hash-selected (not first-N) from its aliases -- see README.',
    )
    parser.add_argument(
        '--negatives-per-query', type=int, default=8,
        help='Target negatives kept per query, after source-coverage + rank-band selection -- see README.',
    )
    parser.add_argument(
        '--min-negatives-per-query', type=int, default=1,
        help='Drop a query unit if fewer than this many negatives were mined for it.',
    )
    parser.add_argument(
        '--candidate-pool', type=int, default=50,
        help='Results requested per recall arm before merging/filtering (100 is a practical max).',
    )
    parser.add_argument('--batch-size', type=int, default=64, help='Query units mined concurrently per progress batch.')
    parser.add_argument('--concurrency', type=int, default=16, help='Max query units being mined at once.')

    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == '__main__':
    main()
