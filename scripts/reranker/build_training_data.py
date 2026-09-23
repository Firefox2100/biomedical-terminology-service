#!/usr/bin/env python3
"""Mine reranker training data from a fully built and embedded bioterms database."""
import argparse
import asyncio
import hashlib
import json
import time
from contextlib import ExitStack
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, EmbeddingKind
from bioterms.database import DocumentDatabase, GraphDatabase, VectorDatabase, get_active_doc_db, \
    get_active_graph_db, get_active_vector_db
from bioterms.embedding import TextTransformer
from bioterms.model.concept import Concept, EmbeddingItem
from bioterms.vocabulary import get_vocabulary_config
from bioterms.vocabulary.utils import ALL_VOCABULARIES


# Best-rank bands diversify negatives by retrieval difficulty.
RANK_BANDS: list[tuple[int, int]] = [(1, 5), (6, 20), (21, 50)]
RANK_BAND_LABELS: list[str] = ['very_hard', 'hard', 'medium']
OVERFLOW_BAND_LABEL = 'long_tail'

RECALL_SOURCES: list[str] = [
    'lexical', 'alias_embedding', 'definition_embedding', 'exact_mapping',
]


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
    skipped_gold_not_retrieved: int = 0

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
            'skipped_gold_not_retrieved': self.skipped_gold_not_retrieved,
        }


def is_valid_negative(prefix: ConceptPrefix,
                      gold_concept_id: str,
                      candidate_concept_id: str,
                      equivalence_index: dict[str, set[str]] | None = None,
                      ) -> bool:
    """Reject the gold concept and same-vocabulary replacement equivalents."""
    del prefix
    if candidate_concept_id == gold_concept_id:
        return False
    if equivalence_index and candidate_concept_id in equivalence_index.get(gold_concept_id, ()):
        return False
    return True


async def _build_equivalence_index(graph_db: GraphDatabase,
                                   prefix: ConceptPrefix,
                                   ) -> dict[str, set[str]]:
    """
    Build a same-vocabulary equivalence index from `REPLACED_BY` edges, for `is_valid_negative`.
    :param graph_db: The graph database instance.
    :param prefix: The vocabulary prefix to build the index for.
    :return: A dict mapping each concept_id to the set of concept_ids it is equivalent to
        (empty if the vocabulary has no REPLACED_BY edges at all).
    """
    index: dict[str, set[str]] = defaultdict(set)
    async for source_id, target_id in graph_db.get_relationship_edges(prefix, ConceptRelationshipType.REPLACED_BY):
        index[source_id].add(target_id)
        index[target_id].add(source_id)
    return dict(index)


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
    """Build a deterministic, order-unbiased set of alias queries."""
    units: list[QueryUnit] = []

    ordered_concept_ids = sorted(concepts.keys(), key=lambda cid: _stable_hash_int(prefix.value, cid))

    for concept_id in ordered_concept_ids:
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
    passes: (1) source coverage -- one negative per recall arm
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
                          equivalence_index: dict[str, set[str]] | None = None,
                          query_vector: list[float] | None = None,
                          additional_ranked_hits: dict[str, list[str]] | None = None,
                          ) -> tuple[list[dict], list[dict], int, int, dict | None]:
    """
    Run the query through the lexical/alias-embedding/definition-embedding recall arms,
    aggregate hits by concept_id (a concept hit by several arms becomes one candidate with all
    evidence attached), reject invalid candidates, and select negatives.
    :return: (selected_negatives, full_candidate_pool, duplicate_cross_source_merges,
        rejected_by_equivalence_filter, gold_retrieval_evidence).  The full pool is retained
        so later model-in-the-loop scoring and alternative sampling policies never require
        repeating database retrieval.
    """
    if query_vector is None:
        # Keep this fallback for direct callers. Production mining supplies one vector from a
        # batch encode, which is substantially faster and avoids serial one-text GPU launches.
        loop = asyncio.get_running_loop()
        query_vector = (await loop.run_in_executor(
            None, transformer.embed_strings, [unit.item.text]
        ))[0]

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
    for source, concept_ids in (additional_ranked_hits or {}).items():
        add_hits(
            [(concept_id, 1.0 / rank) for rank, concept_id in enumerate(concept_ids, start=1)],
            source,
        )

    gold_evidence = merged.get(unit.concept_id)
    if gold_evidence is not None:
        gold_evidence = {
            'sources': sorted(set(gold_evidence['sources'])),
            'ranks': dict(gold_evidence['ranks']),
            'scores': dict(gold_evidence['scores']),
        }

    rejected = 0
    for concept_id in list(merged.keys()):
        if not is_valid_negative(unit.prefix, unit.concept_id, concept_id, equivalence_index):
            del merged[concept_id]
            rejected += 1

    negatives = _select_negatives(merged, negatives_per_query)
    candidate_pool = []
    for concept_id, evidence in sorted(
        merged.items(), key=lambda item: (min(item[1]['ranks'].values()), item[0])
    ):
        best_rank = min(evidence['ranks'].values())
        candidate_pool.append({
            'concept_id': concept_id,
            'role': 'negative',
            'sources': sorted(set(evidence['sources'])),
            'ranks': dict(evidence['ranks']),
            'scores': dict(evidence['scores']),
            'rank_band': _band_label(best_rank),
            # Additional scorers append values here under a caller-selected key.  Keeping
            # retrieval scores separate makes the schema safe for future LLM/cross-encoder
            # teachers without remapping the source evidence.
            'ranking_scores': {},
        })

    return negatives, candidate_pool, duplicate_merges, rejected, gold_evidence


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


class _MiningOutput:
    """Route mined rows either to one legacy shard or to per-vocabulary shards."""

    def __init__(self, output: str | None, output_dir: str | None, stem: str = 'aliases'):
        self.output_path = Path(output) if output else None
        self.output_dir = Path(output_dir) if output_dir else None
        self.stem = stem
        self._stack = ExitStack()
        self._single_file = None
        self._vocabulary_files = {}

    def __enter__(self):
        if self.output_path:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self._single_file = self._stack.enter_context(
                self.output_path.open('w', encoding='utf-8')
            )
        else:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        return self

    def write(self, prefix: ConceptPrefix, record: dict) -> None:
        output_file = self._single_file
        if output_file is None:
            output_file = self._vocabulary_files.get(prefix.value)
            if output_file is None:
                path = self.output_dir / f'{self.stem}.{prefix.value}.jsonl'
                output_file = self._stack.enter_context(path.open('w', encoding='utf-8'))
                self._vocabulary_files[prefix.value] = output_file
        output_file.write(json.dumps(record, ensure_ascii=False) + '\n')

    def __exit__(self, *exc_info):
        return self._stack.__exit__(*exc_info)

    @property
    def metadata_dir(self) -> Path:
        return self.output_path.parent if self.output_path else self.output_dir

    @property
    def description(self) -> str:
        return str(self.output_path) if self.output_path else f'{self.output_dir}/{self.stem}.<vocabulary>.jsonl'


async def _run(args: argparse.Namespace) -> None:
    doc_db = await get_active_doc_db()
    vector_db = get_active_vector_db()
    graph_db = get_active_graph_db()
    transformer = TextTransformer()

    stats = MiningStats()
    global_index = 0
    written = 0
    semaphore = asyncio.Semaphore(args.concurrency)

    output_router = _MiningOutput(args.output, args.output_dir)
    concept_store_dir = Path(args.concept_store_dir) if args.concept_store_dir \
        else output_router.metadata_dir / 'concepts'

    # Persisted per-vocabulary unit-count cache, so a later --skip can bypass reloading
    # vocabularies entirely before the requested window. Delete it if the
    # underlying vocabularies have changed since it was written.
    manifest_path = output_router.metadata_dir / '.reranker_vocab_unit_counts.json'
    manifest: dict[str, int] = {}
    if manifest_path.exists():
        with manifest_path.open(encoding='utf-8') as manifest_file:
            manifest = json.load(manifest_file)

    start_time = time.perf_counter()

    per_vocab_limit = args.per_vocabulary_limit

    with output_router:
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

            equivalence_index = await _build_equivalence_index(graph_db, prefix)
            if equivalence_index:
                print(
                    f'[{prefix.value}] {len(equivalence_index)} concepts have a REPLACED_BY '
                    f'equivalence -- these will not be mined as negatives for each other.'
                )

            units = _build_query_units(prefix, concepts, args.max_queries_per_concept)
            manifest[prefix.value] = len(units)
            stats.vocabularies[prefix.value] = len(units)
            print(f'[{prefix.value}] {len(concepts)} concepts, {len(units)} candidate query units')

            # Global mode: --skip/--limit slice the one cross-vocabulary sequence (units
            # before the window are walked to advance the index, never mined). Per-vocabulary
            # mode: the same values slice *this vocabulary's own* sequence, restarting at 0
            # each time, which guarantees per-vocabulary coverage.
            window_skip, window_limit = args.skip, (per_vocab_limit if per_vocab_limit is not None else args.limit)
            window_base = 0 if per_vocab_limit is not None else global_index
            in_window = [
                (idx, u) for idx, u in enumerate(units, start=window_base)
                if window_skip <= idx < window_skip + window_limit
            ]
            global_index += len(units)

            async def process_one(unit: QueryUnit,
                                  query_vector: list[float],
                                  ) -> tuple[dict | None, int, int, bool]:
                async with semaphore:
                    negatives, candidate_pool, duplicate_merges, rejected, gold_evidence = await _mine_negatives(
                        doc_db, vector_db, transformer, unit,
                        negatives_per_query=args.negatives_per_query,
                        candidate_pool=args.candidate_pool,
                        equivalence_index=equivalence_index,
                        query_vector=query_vector,
                    )
                if len(negatives) < args.min_negatives_per_query:
                    return None, duplicate_merges, rejected, True
                record = {
                    'schema_version': 2,
                    'query_id': unit.item.item_id,
                    'prefix': unit.prefix.value,
                    'gold_concept_id': unit.concept_id,
                    'query': unit.item.text,
                    'query_kind': 'alias',
                    'gold_retrieval_evidence': gold_evidence,
                    'candidate_pool': candidate_pool,
                    'negatives': negatives,
                }
                return record, duplicate_merges, rejected, False

            vocab_written = 0
            for batch_start in range(0, len(in_window), args.batch_size):
                batch = in_window[batch_start:batch_start + args.batch_size]
                loop = asyncio.get_running_loop()
                query_vectors = await loop.run_in_executor(
                    None, transformer.embed_strings, [unit.item.text for _idx, unit in batch]
                )
                results = await asyncio.gather(*(
                    process_one(unit, query_vector)
                    for (_idx, unit), query_vector in zip(batch, query_vectors)
                ))

                for record, duplicate_merges, rejected, below_min in results:
                    if below_min:
                        stats.skipped_below_min_negatives += 1
                        continue
                    output_router.write(prefix, record)
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

    stats_path = (
        output_router.output_path.with_suffix(output_router.output_path.suffix + '.stats.json')
        if output_router.output_path
        else output_router.output_dir / 'aliases.stats.json'
    )
    with stats_path.open('w', encoding='utf-8') as stats_file:
        json.dump({
            'skip': args.skip,
            'limit': args.limit,
            'per_vocabulary_limit': args.per_vocabulary_limit,
            'elapsed_seconds': time.perf_counter() - start_time,
            **stats.as_dict(),
        }, stats_file, indent=2)

    print(f'Done. Wrote {written} query units to {output_router.description} (stats: {stats_path}).')
    await doc_db.close()
    await vector_db.close()
    await graph_db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Mine a reranker training dataset from a built bioterms database.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument(
        '--output',
        help='Legacy combined JSONL path (a "<output>.stats.json" is written alongside it).',
    )
    output_group.add_argument(
        '--output-dir',
        help='Write one aliases.<prefix>.jsonl file per vocabulary plus aliases.stats.json.',
    )
    parser.add_argument(
        '--concept-store-dir', default=None,
        help='Directory for per-vocabulary "<prefix>.concepts.jsonl" files. Defaults to a '
             '"concepts" subdirectory next to --output.',
    )
    parser.add_argument(
        '--skip', type=int, default=0,
        help='Query units to skip before mining starts.',
    )
    parser.add_argument('--limit', type=int, default=100_000, help='Query units to mine and write in this run.')
    parser.add_argument(
        '--per-vocabulary-limit', type=int, default=None,
        help='Mine up to this many units from EACH vocabulary instead of one global --limit '
             '(re-interprets --skip/--limit as per-vocabulary).',
    )
    parser.add_argument(
        '--vocabularies', nargs='*', default=None,
        help='Restrict to these vocabulary prefixes. Defaults to every vocabulary with data loaded.',
    )
    parser.add_argument(
        '--max-queries-per-concept', type=int, default=4,
        help='Cap on query units per concept, hash-selected from its aliases.',
    )
    parser.add_argument(
        '--negatives-per-query', type=int, default=8,
        help='Target negatives kept per query after source-coverage and rank-band selection.',
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
    if args.skip < 0:
        parser.error('--skip must be >= 0')
    if args.limit < 1:
        parser.error('--limit must be >= 1')
    if args.per_vocabulary_limit is not None and args.per_vocabulary_limit < 1:
        parser.error('--per-vocabulary-limit must be >= 1 when set')
    if args.max_queries_per_concept < 1:
        parser.error('--max-queries-per-concept must be >= 1')
    if args.negatives_per_query < 1:
        parser.error('--negatives-per-query must be >= 1')
    if not 0 <= args.min_negatives_per_query <= args.negatives_per_query:
        parser.error('--min-negatives-per-query must be between 0 and --negatives-per-query')
    if args.candidate_pool < 1:
        parser.error('--candidate-pool must be >= 1')
    if args.batch_size < 1 or args.concurrency < 1:
        parser.error('--batch-size and --concurrency must be >= 1')
    asyncio.run(_run(args))


if __name__ == '__main__':
    main()
