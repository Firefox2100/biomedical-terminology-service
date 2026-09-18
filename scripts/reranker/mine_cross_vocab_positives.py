#!/usr/bin/env python3
"""
Mine additional reranker training query-groups from cross-vocabulary EXACT annotation
mappings, on top of what build_training_data.py mines from each vocabulary's own aliases.

For each declared vocabulary ANNOTATIONS pair with real AnnotationType.EXACT edges between
them, one side's alias text becomes a query and the OTHER side's mapped concept becomes gold
-- mined in BOTH directions, since EXACT is a symmetric equivalence claim regardless of which
side's vocabulary module happens to own the mapping. Only EXACT edges are used: BROAD/NARROW/
RELATED are not equivalence, and mining them as positives would train the model that a parent
category "is" its child (or vice versa), which is exactly the kind of label noise this is
trying to avoid, not introduce.

Negatives are mined exactly the way build_training_data.py does it -- same-vocabulary lexical/
embedding recall, scoped to the TARGET side of the mapping (never the source side the query
text came from), through the same _mine_negatives() and REPLACED_BY equivalence filtering. No
new concept-store entries are needed: every gold/negative concept mined here already exists in
whichever <prefix>.concepts.jsonl a prior build_training_data.py run wrote, since gold/negative
concepts are always drawn from a vocabulary this script requires to already be in scope.

This is a pure addition on top of already-mined files -- it does not touch or re-mine
anything build_training_data.py already produced, and its own output is a syntactically
identical query-group JSONL (just 'query_kind': 'cross_vocab_exact' instead of 'alias') that
train_reranker.py can be pointed at alongside the others via --train-data.

Usage:
    python mine_cross_vocab_positives.py \
        --output data/part-02-crossvocab.jsonl \
        --vocabularies ctv3 ensembl gene hgnc hpo mondo ncit omim ordo reactome snomed

The global candidate sequence is deterministic. Use `--skip` and `--limit` to mine successive
non-overlapping shards, exactly as for build_training_data.py. A per-direction unit-count
manifest lets later shards skip directions wholly before their requested window.
"""
import argparse
import asyncio
import heapq
import json
import time
from pathlib import Path

from bioterms.database import get_active_doc_db, get_active_graph_db, get_active_vector_db
from bioterms.embedding import TextTransformer
from bioterms.etc.enums import AnnotationType, ConceptPrefix, EmbeddingKind
from bioterms.vocabulary import get_vocabulary_config
from bioterms.vocabulary.utils import get_vocabulary_module

from build_training_data import (
    MiningStats,
    QueryUnit,
    _MiningOutput,
    _build_equivalence_index,
    _mine_negatives,
    _stable_hash_int,
)


def _iter_scopes(requested: list[str] | None) -> list[ConceptPrefix]:
    if requested:
        return sorted((ConceptPrefix(v) for v in requested), key=lambda p: p.value)
    return sorted(ConceptPrefix, key=lambda p: p.value)


def _declared_pairs(scope: list[ConceptPrefix]) -> list[tuple[ConceptPrefix, ConceptPrefix]]:
    """
    Every unordered (prefix_1, prefix_2) pair, both within `scope`, that some vocabulary
    module declares an ANNOTATIONS relationship for -- deduplicated regardless of which side
    declared it, since the annotation table itself is shared and undirected in meaning.
    """
    scope_set = set(scope)
    pairs: set[tuple[ConceptPrefix, ConceptPrefix]] = set()

    for prefix in scope:
        module = get_vocabulary_module(prefix)
        for other in getattr(module, 'ANNOTATIONS', []):
            if other not in scope_set:
                continue
            pairs.add(tuple(sorted((prefix, other), key=lambda p: p.value)))

    return sorted(pairs, key=lambda pair: (pair[0].value, pair[1].value))


async def _exact_edges(graph_db, prefix_1: ConceptPrefix, prefix_2: ConceptPrefix):
    """
    Stream AnnotationType.EXACT edges between two vocabularies. Filtering happens in the
    database, avoiding the old all-types NetworkX graph materialisation.
    """
    async for prefix_from, concept_from, prefix_to, concept_to, _annotation_type in (
        graph_db.get_annotation_edges(prefix_1, prefix_2, AnnotationType.EXACT)
    ):
        yield prefix_from, concept_from, prefix_to, concept_to


def _offer_bounded_mapping(heap: list[tuple],
                           seen: set[tuple[str, str]],
                           mapping: tuple[str, str],
                           source_prefix: ConceptPrefix,
                           target_prefix: ConceptPrefix,
                           capacity: int,
                           ) -> None:
    """Keep the deterministically smallest-hash unique mappings in bounded memory."""
    if mapping in seen:
        return
    rank = _stable_hash_int(source_prefix.value, target_prefix.value, *mapping)
    entry = (-rank, mapping[0], mapping[1])
    if len(heap) < capacity:
        heapq.heappush(heap, entry)
        seen.add(mapping)
        return
    if rank >= -heap[0][0]:
        return
    removed = heapq.heapreplace(heap, entry)
    seen.remove((removed[1], removed[2]))
    seen.add(mapping)


def _direction_key(target_prefix: ConceptPrefix, source_prefix: ConceptPrefix) -> str:
    return f'{source_prefix.value}->{target_prefix.value}'


def _order_and_cap_units(units: list[tuple[QueryUnit, str]],
                         target_prefix: ConceptPrefix,
                         source_prefix: ConceptPrefix,
                         max_total_per_direction: int | None,
                         ) -> list[tuple[QueryUnit, str]]:
    """Give a direction a deterministic sequence, then apply its optional stable cap."""
    units.sort(key=lambda pair: (
        _stable_hash_int(
            target_prefix.value, source_prefix.value, pair[0].concept_id,
            pair[0].item.item_id, pair[0].item.text,
        ),
        pair[0].concept_id,
        pair[0].item.item_id,
    ))
    if max_total_per_direction is not None:
        return units[:max_total_per_direction]
    return units


async def _run(args: argparse.Namespace) -> None:
    doc_db = await get_active_doc_db()
    vector_db = get_active_vector_db()
    graph_db = get_active_graph_db()
    transformer = TextTransformer()

    scope = _iter_scopes(args.vocabularies)
    pairs = _declared_pairs(scope)
    print(f'Vocabulary pairs with a declared ANNOTATIONS relationship, both sides in scope: '
          f'{[(p1.value, p2.value) for p1, p2 in pairs]}')

    # (target_prefix, source_prefix) -> {(source_concept_id, gold_concept_id), ...}.
    # With a per-direction output cap, pre-sample at twice that many mappings so huge pairs
    # (notably OHDSI<->SNOMED) never materialise millions of edges/concepts in RAM.
    mapping_by_direction: dict[tuple[ConceptPrefix, ConceptPrefix], set[tuple[str, str]]] = {}
    mapping_heaps: dict[tuple[ConceptPrefix, ConceptPrefix], list[tuple]] = {}
    edge_counts: dict[tuple[ConceptPrefix, ConceptPrefix], int] = {}
    mapping_capacity = (
        args.max_total_per_direction * 2 if args.max_total_per_direction is not None else None
    )

    defer_targets = {ConceptPrefix(v) for v in (args.defer_targets or [])}
    only_targets = {ConceptPrefix(v) for v in (args.only_targets or [])}
    if defer_targets and only_targets:
        raise SystemExit('--defer-targets and --only-targets are mutually exclusive')

    for prefix_1, prefix_2 in pairs:
        edge_count = 0
        async for prefix_from, concept_from, prefix_to, concept_to in _exact_edges(
                graph_db, prefix_1, prefix_2):
            edge_count += 1
            source_prefix = ConceptPrefix(prefix_from)
            target_prefix = ConceptPrefix(prefix_to)
            # Mine both directions: source's alias -> target's gold, AND target's alias ->
            # source's gold, since EXACT is symmetric regardless of which side this
            # particular edge happened to be stored as "from". A direction is only skipped by
            # its TARGET (--defer-targets/--only-targets), never by its source: negatives are
            # always mined scoped to the target vocabulary (see _mine_negatives below), so only
            # the target side actually needs that vocabulary's embeddings to be ready -- the
            # source side just needs its alias text, which comes from doc_db regardless.
            if target_prefix not in defer_targets and (not only_targets or target_prefix in only_targets):
                direction = (target_prefix, source_prefix)
                edge_counts[direction] = edge_counts.get(direction, 0) + 1
                if mapping_capacity is None:
                    mapping_by_direction.setdefault(direction, set()).add((concept_from, concept_to))
                else:
                    _offer_bounded_mapping(
                        mapping_heaps.setdefault(direction, []),
                        mapping_by_direction.setdefault(direction, set()),
                        (concept_from, concept_to), source_prefix, target_prefix, mapping_capacity,
                    )
            if source_prefix not in defer_targets and (not only_targets or source_prefix in only_targets):
                direction = (source_prefix, target_prefix)
                edge_counts[direction] = edge_counts.get(direction, 0) + 1
                if mapping_capacity is None:
                    mapping_by_direction.setdefault(direction, set()).add((concept_to, concept_from))
                else:
                    _offer_bounded_mapping(
                        mapping_heaps.setdefault(direction, []),
                        mapping_by_direction.setdefault(direction, set()),
                        (concept_to, concept_from), target_prefix, source_prefix, mapping_capacity,
                    )
        print(f'[{prefix_1.value}<->{prefix_2.value}] {edge_count} EXACT annotation edges')

    stats = MiningStats()
    written = 0
    semaphore = asyncio.Semaphore(args.concurrency)
    output_router = _MiningOutput(args.output, args.output_dir, stem='cross-vocab')
    start_time = time.perf_counter()
    global_index = 0

    manifest_path = output_router.metadata_dir / '.reranker_relationship_unit_counts.json'
    manifest: dict[str, int] = {}
    if manifest_path.exists():
        with manifest_path.open(encoding='utf-8') as manifest_file:
            manifest = json.load(manifest_file)

    # Cached per target vocabulary: several source vocabularies can map into the same
    # target (e.g. both MONDO and CTV3 map into SNOMED), and each would otherwise trigger
    # its own redundant REPLACED_BY fetch for the same target prefix.
    equivalence_by_target: dict[ConceptPrefix, dict[str, set[str]]] = {}

    with output_router:
        for target_prefix, source_prefix in sorted(
                mapping_by_direction, key=lambda pair: (pair[1].value, pair[0].value)):
            if args.limit is not None and global_index >= args.skip + args.limit:
                break

            source_to_gold = sorted(mapping_by_direction[(target_prefix, source_prefix)])
            if not source_to_gold:
                continue

            direction_key = _direction_key(target_prefix, source_prefix)
            cache_key = (
                f'{direction_key}|queries={args.max_queries_per_pair}|'
                f'cap={args.max_total_per_direction}|mapping-sample=v2'
            )
            cached_count = manifest.get(cache_key)
            if (cached_count is not None and global_index + cached_count <= args.skip):
                global_index += cached_count
                stats.vocabularies[direction_key] = cached_count
                print(f'[{direction_key}] before --skip window, using cached count ({cached_count})')
                continue

            if target_prefix not in equivalence_by_target:
                equivalence_by_target[target_prefix] = await _build_equivalence_index(graph_db, target_prefix)
            equivalence_index = equivalence_by_target[target_prefix]

            source_ids = sorted({source_id for source_id, _gold in source_to_gold})
            source_config = get_vocabulary_config(source_prefix)
            source_concepts = await doc_db.get_terms_by_ids(
                prefix=source_prefix, concept_ids=source_ids, model_class=source_config['conceptClass'],
            )
            source_by_id = {c.concept_id: c for c in source_concepts}

            units: list[tuple[QueryUnit, str]] = []
            for source_id, gold_id in source_to_gold:
                concept = source_by_id.get(source_id)
                if concept is None:
                    continue
                alias_items = [i for i in concept.embedding_items() if i.kind == EmbeddingKind.ALIAS]
                alias_items.sort(
                    key=lambda item: _stable_hash_int(source_prefix.value, source_id, item.item_id)
                )
                for item in alias_items[:args.max_queries_per_pair]:
                    units.append((
                        QueryUnit(prefix=target_prefix, concept_id=gold_id, item=item),
                        source_prefix.value,
                    ))

            if not units:
                continue

            total_candidates = len(units)
            units = _order_and_cap_units(
                units, target_prefix, source_prefix, args.max_total_per_direction,
            )
            manifest[cache_key] = len(units)
            window_end = None if args.limit is None else args.skip + args.limit
            in_window = [
                pair for local_index, pair in enumerate(units, start=global_index)
                if local_index >= args.skip and (window_end is None or local_index < window_end)
            ]
            global_index += len(units)

            print(
                f'[{source_prefix.value}->{target_prefix.value}] {len(source_to_gold)} EXACT-mapped '
                f'concepts sampled from {edge_counts[(target_prefix, source_prefix)]} edges, '
                f'{total_candidates} candidate query units'
                + (f' (capped to {len(units)})' if len(units) != total_candidates else '')
            )
            stats.vocabularies[direction_key] = len(units)

            async def process_one(unit: QueryUnit,
                                  source_prefix_value: str,
                                  query_vector: list[float],
                                  ) -> tuple[dict | None, int, int, str | None]:
                async with semaphore:
                    negatives, duplicate_merges, rejected, gold_evidence = await _mine_negatives(
                        doc_db, vector_db, transformer, unit,
                        negatives_per_query=args.negatives_per_query,
                        candidate_pool=args.candidate_pool,
                        equivalence_index=equivalence_index,
                        query_vector=query_vector,
                    )
                # A reranker cannot learn/use an alias->gold pair that the production recall
                # stage never retrieves. This also removes semantically incompatible aliases
                # attached to otherwise valid cross-vocabulary mappings (e.g. ambiguous
                # acronyms whose other sense maps to the target concept).
                if args.require_gold_retrieved and gold_evidence is None:
                    return None, duplicate_merges, rejected, 'gold_not_retrieved'
                if len(negatives) < args.min_negatives_per_query:
                    return None, duplicate_merges, rejected, 'below_min_negatives'
                record = {
                    'query_id': (
                        f'{source_prefix_value}:{unit.item.item_id}->{unit.prefix.value}:'
                        f'{unit.concept_id}'
                    ),
                    'prefix': unit.prefix.value,
                    'gold_concept_id': unit.concept_id,
                    'query': unit.item.text,
                    'query_kind': 'cross_vocab_exact',
                    'source_prefix': source_prefix_value,
                    'gold_retrieval': gold_evidence,
                    'negatives': negatives,
                }
                return record, duplicate_merges, rejected, None

            for batch_start in range(0, len(in_window), args.batch_size):
                batch = in_window[batch_start:batch_start + args.batch_size]
                loop = asyncio.get_running_loop()
                query_vectors = await loop.run_in_executor(
                    None, transformer.embed_strings, [unit.item.text for unit, _sp in batch]
                )
                results = await asyncio.gather(*(
                    process_one(unit, source_prefix_value, query_vector)
                    for (unit, source_prefix_value), query_vector in zip(batch, query_vectors)
                ))

                for record, duplicate_merges, rejected, skip_reason in results:
                    if skip_reason == 'gold_not_retrieved':
                        stats.skipped_gold_not_retrieved += 1
                        continue
                    if skip_reason == 'below_min_negatives':
                        stats.skipped_below_min_negatives += 1
                        continue
                    output_router.write(target_prefix, record)
                    written += 1
                    stats.record(record['negatives'], duplicate_merges, rejected)

                elapsed = time.perf_counter() - start_time
                print(f'[{source_prefix.value}->{target_prefix.value}] written {written} total so far '
                      f'({elapsed:.0f}s elapsed)')

    stats.total_written = written
    with manifest_path.open('w', encoding='utf-8') as manifest_file:
        json.dump(manifest, manifest_file, indent=2, sort_keys=True)
    stats_path = (
        output_router.output_path.with_suffix(output_router.output_path.suffix + '.stats.json')
        if output_router.output_path
        else output_router.output_dir / 'cross-vocab.stats.json'
    )
    with stats_path.open('w', encoding='utf-8') as stats_file:
        json.dump({
            'skip': args.skip,
            'limit': args.limit,
            'require_gold_retrieved': args.require_gold_retrieved,
            'elapsed_seconds': time.perf_counter() - start_time,
            **stats.as_dict(),
        }, stats_file, indent=2)

    print(
        f'Done. Wrote {written} cross-vocabulary query units to '
        f'{output_router.description} (stats: {stats_path}).'
    )
    await doc_db.close()
    await vector_db.close()
    await graph_db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Mine additional reranker training query-groups from cross-vocabulary '
                    'EXACT annotation mappings.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument('--output', help='Legacy combined output JSONL path.')
    output_group.add_argument(
        '--output-dir',
        help='Write one cross-vocab.<target-prefix>.jsonl file per target vocabulary.',
    )
    parser.add_argument('--skip', type=int, default=0, help='Candidate query units to skip in the deterministic global sequence.')
    parser.add_argument('--limit', type=int, default=None, help='Maximum candidate query units to process after --skip; unset mines the remainder.')
    parser.add_argument(
        '--vocabularies', nargs='*', default=None,
        help='Vocabularies in scope -- both sides of a pair must be in this list to be mined. '
             'Defaults to every vocabulary.',
    )
    parser.add_argument(
        '--defer-targets', nargs='*', default=None,
        help='Vocabularies to skip as a mining TARGET (e.g. one whose embeddings are not '
             'loaded yet) -- still mined as a SOURCE, since only the target side needs that '
             "vocabulary's embeddings for negative mining. Mutually exclusive with "
             '--only-targets.',
    )
    parser.add_argument(
        '--only-targets', nargs='*', default=None,
        help='Mine ONLY directions whose target is one of these vocabularies -- the complement '
             'of a prior --defer-targets run, once the deferred vocabulary is ready. Mutually '
             'exclusive with --defer-targets.',
    )
    parser.add_argument(
        '--max-queries-per-pair', type=int, default=4,
        help='Cap on how many of the source concept\'s aliases become queries for one EXACT '
             'mapping, hash-selected like build_training_data.py\'s --max-queries-per-concept.',
    )
    parser.add_argument(
        '--max-total-per-direction', type=int, default=None,
        help='Cap on total candidate query units for one (source, target) direction, '
             'hash-selected for a representative sample -- some cross-vocabulary pairs have '
             'vastly more EXACT mappings than others (e.g. OHDSI<->SNOMED alone is over a '
             'million), so without this one lopsided pair can dwarf the rest of the dataset. '
             'Unset (default) mines every candidate, matching the original behaviour.',
    )
    parser.add_argument('--negatives-per-query', type=int, default=8)
    parser.add_argument('--min-negatives-per-query', type=int, default=2)
    parser.add_argument('--candidate-pool', type=int, default=50)
    parser.add_argument(
        '--require-gold-retrieved', action=argparse.BooleanOptionalAction, default=True,
        help='Keep only mappings whose gold is found by at least one target-vocabulary recall arm.',
    )
    parser.add_argument('--batch-size', type=int, default=64)
    parser.add_argument('--concurrency', type=int, default=16)

    args = parser.parse_args()
    if args.skip < 0:
        parser.error('--skip must be >= 0')
    if args.limit is not None and args.limit < 1:
        parser.error('--limit must be >= 1 when set')
    if args.max_queries_per_pair < 1:
        parser.error('--max-queries-per-pair must be >= 1')
    if args.max_total_per_direction is not None and args.max_total_per_direction < 1:
        parser.error('--max-total-per-direction must be >= 1 when set')
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
