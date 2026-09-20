#!/usr/bin/env python3
"""Attach late-interaction ranking scores to immutable mined candidate pools.

The input JSONL is never modified.  Output is resumable at a JSONL row boundary and retains
all retrieval evidence, allowing training to switch between hard-label and distillation
objectives without re-mining the databases.
"""
import argparse
import json
from pathlib import Path

from concept_rendering import RenderVariant, render_concept


def _load_concepts(directory: Path) -> dict[tuple[str, str], dict]:
    concepts = {}
    for path in sorted(directory.glob('*.concepts.jsonl')):
        prefix = path.name.removesuffix('.concepts.jsonl')
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                record = json.loads(line)
                concepts[(prefix, record['concept_id'])] = record
    return concepts


def _render(concept: dict, variant: RenderVariant, max_aliases: int | None, key: str) -> str:
    return render_concept(
        label=concept['label'], synonyms=concept.get('synonyms') or [],
        definition=concept.get('definition'), variant=variant, max_aliases=max_aliases,
        alias_selection_key=key,
    )


def _prepare(record: dict, concepts: dict, variant: RenderVariant, max_aliases: int | None):
    prefix = record['prefix']
    gold_id = record['gold_concept_id']
    pool = [dict(candidate) for candidate in record.get('candidate_pool') or []]
    if not any(candidate['concept_id'] == gold_id for candidate in pool):
        pool.insert(0, {
            'concept_id': gold_id, 'role': 'gold', 'sources': [], 'ranks': {}, 'scores': {},
            'rank_band': 'gold', 'ranking_scores': {},
        })
    else:
        for candidate in pool:
            if candidate['concept_id'] == gold_id:
                candidate['role'] = 'gold'

    kept, texts = [], []
    for candidate in pool:
        concept = concepts.get((prefix, candidate['concept_id']))
        if concept is None:
            continue
        kept.append(candidate)
        texts.append(_render(concept, variant, max_aliases, f'{prefix}:{candidate["concept_id"]}'))
    if not any(candidate['concept_id'] == gold_id for candidate in kept):
        raise ValueError(f'gold concept is absent from concept store: {prefix}:{gold_id}')
    return kept, texts


def _build_document_cache(model, source: Path, concepts: dict, args) -> dict[tuple[str, str], object]:
    """Encode each concept document once per shard instead of once per query occurrence."""
    keys = set()
    with source.open(encoding='utf-8') as handle:
        for line in handle:
            record = json.loads(line)
            prefix = record['prefix']
            keys.add((prefix, record['gold_concept_id']))
            keys.update(
                (prefix, candidate['concept_id'])
                for candidate in (record.get('candidate_pool') or [])
            )
    ordered_keys = sorted(key for key in keys if key in concepts)
    variant = RenderVariant(args.render_variant)
    max_aliases = args.max_aliases if args.max_aliases > 0 else None
    texts = [
        _render(concepts[key], variant, max_aliases, f'{key[0]}:{key[1]}')
        for key in ordered_keys
    ]
    print(f'[{source.name}] encoding {len(ordered_keys)} unique candidate documents once')
    embeddings = model.encode(
        texts, is_query=False, batch_size=args.encode_batch_size, show_progress_bar=True,
    )
    return dict(zip(ordered_keys, embeddings))


def _score_batch(model, records: list[dict], concepts: dict,
                 document_cache: dict[tuple[str, str], object], args) -> list[dict]:
    from pylate import rank

    variant = RenderVariant(args.render_variant)
    max_aliases = args.max_aliases if args.max_aliases > 0 else None
    pools, document_embeddings = [], []
    for record in records:
        pool, _texts = _prepare(record, concepts, variant, max_aliases)
        pools.append(pool)
        document_embeddings.append([
            document_cache[(record['prefix'], candidate['concept_id'])]
            for candidate in pool
        ])

    query_embeddings = model.encode(
        [record['query'] for record in records], is_query=True, batch_size=args.encode_batch_size,
        show_progress_bar=False,
    )
    ranked = rank.rerank(
        documents_ids=[[candidate['concept_id'] for candidate in pool] for pool in pools],
        queries_embeddings=query_embeddings, documents_embeddings=document_embeddings,
        device=str(model.device),
    )

    outputs = []
    for record, pool, ranking in zip(records, pools, ranked):
        by_id = {str(item['id']): float(item['score']) for item in ranking}
        for candidate in pool:
            candidate.setdefault('ranking_scores', {})[args.score_key] = by_id[candidate['concept_id']]
        output = dict(record)
        output['schema_version'] = max(2, int(record.get('schema_version', 1)))
        output['candidate_pool'] = pool
        output.setdefault('ranking_supervision', {})[args.score_key] = {
            'model': args.model, 'kind': 'colbert_late_interaction',
            'render_variant': args.render_variant, 'max_aliases': max_aliases,
        }
        outputs.append(output)
    return outputs


def _process_file(model, source: Path, destination: Path, concepts: dict, args) -> None:
    partial = destination.with_suffix(destination.suffix + '.partial')
    completed = 0
    if destination.exists() and not args.overwrite:
        print(f'[{source.name}] complete output exists, skipping')
        return
    if partial.exists() and not args.overwrite:
        with partial.open(encoding='utf-8') as handle:
            completed = sum(1 for _ in handle)
        mode = 'a'
        print(f'[{source.name}] resuming after {completed} rows')
    else:
        mode = 'w'

    document_cache = _build_document_cache(model, source, concepts, args)

    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open(encoding='utf-8') as source_handle, partial.open(mode, encoding='utf-8') as output:
        batch = []
        for index, line in enumerate(source_handle):
            if index < completed:
                continue
            batch.append(json.loads(line))
            if len(batch) < args.group_batch_size:
                continue
            for record in _score_batch(model, batch, concepts, document_cache, args):
                output.write(json.dumps(record, ensure_ascii=False) + '\n')
            output.flush()
            completed += len(batch)
            batch.clear()
            print(f'[{source.name}] scored {completed} rows')
        if batch:
            for record in _score_batch(model, batch, concepts, document_cache, args):
                output.write(json.dumps(record, ensure_ascii=False) + '\n')
            completed += len(batch)
    partial.replace(destination)
    print(f'[{source.name}] complete: {completed} rows -> {destination}')


def main() -> None:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--concept-store-dir', required=True)
    parser.add_argument('--model', required=True, help='Trained ColBERT checkpoint used for re-mining scores.')
    parser.add_argument('--score-key', default='student_v1')
    parser.add_argument('--render-variant', default=RenderVariant.LABEL_ALIASES_DEFINITION.value,
                        choices=[variant.value for variant in RenderVariant])
    parser.add_argument('--max-aliases', type=int, default=6)
    parser.add_argument('--group-batch-size', type=int, default=32)
    parser.add_argument('--encode-batch-size', type=int, default=64)
    parser.add_argument('--include-vocabularies', nargs='*')
    parser.add_argument(
        '--include-files',
        nargs='*',
        help='Optional exact input filenames, useful for non-vocabulary shards such as cross-vocab.full.jsonl.',
    )
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()

    from pylate import models
    concepts = _load_concepts(Path(args.concept_store_dir))
    model = models.ColBERT(model_name_or_path=args.model)
    allowed = set(args.include_vocabularies or [])
    allowed_files = set(args.include_files or [])
    sources = sorted(Path(args.input_dir).glob('*.jsonl'))
    for source in sources:
        if source.name.endswith('.concepts.jsonl'):
            continue
        if allowed_files and source.name not in allowed_files:
            continue
        # aliases.<prefix>.jsonl and cross.<prefix>.jsonl both expose the target prefix in
        # their records; filename filtering here is only an inexpensive optional fast path.
        if not allowed_files and allowed and not any(f'.{prefix}.' in source.name for prefix in allowed):
            continue
        _process_file(model, source, Path(args.output_dir) / source.name, concepts, args)


if __name__ == '__main__':
    main()
