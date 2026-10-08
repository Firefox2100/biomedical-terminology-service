#!/usr/bin/env python3
"""Compare HPO graph similarity with production hybrid retrieval and reranking.

This is deliberately an offline research evaluation.  ORDO concepts are split into train and
test groups.  Structure methods see only HPO--ORDO annotations from train diseases; relevance
judgments are co-phenotypes attached to held-out diseases.  The same HPO anchors and candidate
universe are used for every method.  Hybrid retrieval receives the anchor's preferred label as
its text query and the anchor itself is removed from every ranking.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import itertools
import json
import math
import re
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
from neo4j import GraphDatabase
from sentence_transformers import MultiVectorEncoder, SentenceTransformer

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import load_obo_owl_classes
from bioterms.model.concept import Concept
from bioterms.search.reranker import render_reranker_candidate
from bioterms.similarity.context import AnnotationIndex, OntologyIndex, SimilarityContext
from bioterms.similarity import co_annotation, relevance, relevance_weight


def _stable_bucket(value: str, buckets: int = 100) -> int:
    return int(hashlib.sha256(value.encode()).hexdigest()[:16], 16) % buckets


def _hpo_text_and_graph() -> tuple[dict[str, dict], list[tuple[str, str, str, None]]]:
    _, classes = load_obo_owl_classes('hpo/hp.owl', 'HP_')
    concepts: dict[str, dict] = {}
    edges = []
    synonym_fields = ('hasExactSynonym', 'hasRelatedSynonym', 'hasBroadSynonym', 'hasNarrowSynonym')
    for item in classes:
        if not item.name.startswith('HP_') or bool(getattr(item, 'deprecated', [])):
            continue
        concept_id = item.name.split('_')[-1]
        labels = list(getattr(item, 'label', []))
        if not labels:
            continue
        synonyms = []
        seen = {str(labels[0]).casefold()}
        for field in synonym_fields:
            for synonym in getattr(item, field, []):
                value = str(synonym).strip()
                if value and value.casefold() not in seen:
                    seen.add(value.casefold())
                    synonyms.append(value)
        definitions = list(getattr(item, 'IAO_0000115', []))
        concepts[concept_id] = {
            'label': str(labels[0]),
            'synonyms': synonyms,
            'definition': str(definitions[0]) if definitions else None,
        }
        for child in item.subclasses():
            child_id = child.name.split('_')[-1]
            if child.name.startswith('HP_'):
                edges.append((child_id, concept_id, 'is_a', None))
    allowed = set(concepts)
    edges = [edge for edge in edges if edge[0] in allowed and edge[1] in allowed]
    return concepts, edges


def create_snapshot(uri: str, user: str, password: str, output: Path) -> dict:
    concepts, hpo_edges = _hpo_text_and_graph()
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        ordo_nodes = [record['id'] for record in driver.execute_query(
            "MATCH (n:Concept {prefix:'ordo'}) RETURN DISTINCT n.id AS id"
        ).records]
        ordo_edges = [
            (record['source'], record['target'], record['type'], None)
            for record in driver.execute_query("""
                MATCH (a:Concept {prefix:'ordo'})-[r]->(b:Concept {prefix:'ordo'})
                WHERE type(r) IN ['is_a', 'part_of']
                RETURN DISTINCT a.id AS source, b.id AS target, type(r) AS type
            """).records
        ]
        annotations = [
            (record['hpo'], record['ordo'])
            for record in driver.execute_query("""
                MATCH (h:Concept {prefix:'hpo'})-[r]-(o:Concept {prefix:'ordo'})
                WHERE type(r) = 'annotated_with'
                RETURN DISTINCT h.id AS hpo, o.id AS ordo
            """).records
            if record['hpo'] in concepts
        ]
    finally:
        driver.close()
    payload = {
        'concepts': concepts,
        'hpo_edges': hpo_edges,
        'ordo_nodes': ordo_nodes,
        'ordo_edges': ordo_edges,
        'annotations': annotations,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output, 'wt', encoding='utf-8') as handle:
        json.dump(payload, handle, separators=(',', ':'))
    return payload


def _build_split(payload: dict, sample_size: int):
    by_disease: dict[str, set[str]] = defaultdict(set)
    for hpo_id, disease_id in payload['annotations']:
        by_disease[disease_id].add(hpo_id)
    test_diseases = {disease for disease in by_disease if _stable_bucket(disease) < 20}
    train_pairs = [(hpo, disease) for disease, hpos in by_disease.items()
                   if disease not in test_diseases for hpo in hpos]

    train_coannotations: set[tuple[str, str]] = set()
    for disease, hpos in by_disease.items():
        if disease in test_diseases:
            continue
        ordered = sorted(hpos)
        train_coannotations.update(itertools.combinations(ordered, 2))

    all_gold: dict[str, set[str]] = defaultdict(set)
    novel_gold: dict[str, set[str]] = defaultdict(set)
    for disease in test_diseases:
        ordered = sorted(by_disease[disease])
        for left, right in itertools.combinations(ordered, 2):
            all_gold[left].add(right)
            all_gold[right].add(left)
            if (left, right) not in train_coannotations:
                novel_gold[left].add(right)
                novel_gold[right].add(left)
    eligible = [anchor for anchor, gold in novel_gold.items() if len(gold) >= 3]
    eligible.sort(key=lambda value: hashlib.sha256(f'anchor:{value}'.encode()).digest())
    anchors = eligible[:sample_size]
    return train_pairs, test_diseases, anchors, all_gold, novel_gold


def _score_structure(context: SimilarityContext, anchors: list[str], method: str) -> dict[str, list[str]]:
    graph = context.target
    candidate_indices = np.arange(len(graph.node_ids), dtype=np.int32)
    threshold = 0.2
    if method == 'co_annotation':
        bitmaps, total = co_annotation._build_annotation_sets(context)
        ptr, ids, _sizes = co_annotation._bitmaps_to_csr(bitmaps)
        scorer = lambda lhs, rhs: co_annotation._score_pairs_cpu(
            ptr, ids, lhs, rhs, total, threshold,
        )
    else:
        if method == 'relevance':
            counts = np.fromiter(
                (len(values) for values in context.annotations.target_to_corpus),
                dtype=np.float64, count=len(graph.node_ids),
            )
            for node in graph.topological_order:
                for child in graph.predecessors(int(node)):
                    counts[int(node)] += counts[int(child)]
            maximum = float(counts.max(initial=0))
            ic = np.zeros(len(graph.node_ids), np.float64)
            valid = counts > 0
            ic[valid] = -np.log(counts[valid] / maximum)
            rel = np.zeros(len(graph.node_ids), np.float64)
            rel[valid] = 1.0 - counts[valid] / maximum
            bitmaps = relevance._build_informative_ancestor_sets(graph, rel, valid, threshold)
            ptr, ids = relevance._bitmaps_to_csr(bitmaps)
            scorer = lambda lhs, rhs: relevance._score_cpu(
                ptr, ids, ic, rel, lhs, rhs, threshold,
            )
        elif method == 'weighed_relevance':
            corpus = context.corpus
            t_pred = relevance_weight._build_predecessor_csr(graph)
            c_pred = relevance_weight._build_predecessor_csr(corpus)
            t_direct = relevance_weight._build_direct_annotation_csr(
                context.annotations.target_to_corpus,
            )
            c_direct = relevance_weight._build_direct_annotation_csr(
                context.annotations.corpus_to_target,
            )
            t_unique = relevance_weight._build_unique_annotation_csr(graph, *t_direct)
            c_unique = relevance_weight._build_unique_annotation_csr(corpus, *c_direct)
            ic, _ = relevance_weight._coupled_ic(
                (*t_direct, *t_unique, *t_pred), (*c_direct, *c_unique, *c_pred),
                len(graph.node_ids), len(corpus.node_ids),
            )
            valid = np.isfinite(ic)
            rel = np.zeros(len(graph.node_ids), np.float64)
            rel[valid] = 1.0 - np.exp(-ic[valid])
            bitmaps = relevance_weight._build_informative_ancestor_sets(
                graph, rel, valid, threshold,
            )
            ptr, ids = relevance_weight._bitmaps_to_csr(bitmaps)
            scorer = lambda lhs, rhs: relevance_weight._score_cpu(
                ptr, ids, ic, rel, lhs, rhs, threshold,
            )
        else:
            raise ValueError(method)

    rankings = {}
    for anchor in anchors:
        anchor_index = graph.node_to_index[anchor]
        lhs = np.full(len(candidate_indices), anchor_index, dtype=np.int32)
        scores = scorer(lhs, candidate_indices)
        scores[anchor_index] = np.nan
        finite = np.flatnonzero(np.isfinite(scores))
        order = finite[np.argsort(-scores[finite], kind='stable')]
        rankings[anchor] = [graph.node_ids[int(index)] for index in order[:100]]
    return rankings


def _pg_trigrams(value: str) -> set[str]:
    words = re.findall(r'[\w]+', value.casefold(), flags=re.UNICODE)
    result = set()
    for word in words:
        padded = '  ' + word + ' '
        result.update(padded[i:i + 3] for i in range(len(padded) - 2))
    return result


def _lexical_rank(query: str, concepts: dict[str, dict], limit: int) -> list[str]:
    words = [word for word in query.casefold().split() if len(word) > 2]
    query_grams = _pg_trigrams(query.replace(' ', ''))
    scored = []
    for concept_id, concept in concepts.items():
        text = ' '.join([concept_id, concept['label'], *(concept['synonyms'] or [])]).casefold()
        if words and not any(word in text for word in words):
            continue
        grams = _pg_trigrams(text)
        score = (2 * len(query_grams & grams) / (len(query_grams) + len(grams))) \
            if query_grams and grams else 0.0
        scored.append((score, concept_id))
    scored.sort(key=lambda row: (-row[0], row[1]))
    return [concept_id for _score, concept_id in scored[:limit]]


def _top_concepts(scores: np.ndarray, item_concepts: np.ndarray, limit: int) -> list[str]:
    depth = min(len(scores), max(limit * 8, limit))
    selected = np.argpartition(scores, -depth)[-depth:]
    selected = selected[np.argsort(-scores[selected], kind='stable')]
    result, seen = [], set()
    for index in selected:
        concept_id = str(item_concepts[int(index)])
        if concept_id not in seen:
            seen.add(concept_id)
            result.append(concept_id)
            if len(result) == limit:
                break
    return result


def _load_or_encode_dense(payload: dict, cache_dir: Path, model_name: str):
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / 'biolord-hpo-items.npz'
    if cache_file.exists():
        data = np.load(cache_file, allow_pickle=False)
        return data['vectors'], data['concept_ids'], data['kinds']
    texts, concept_ids, kinds = [], [], []
    for concept_id, concept in payload['concepts'].items():
        for alias in [concept['label'], *(concept['synonyms'] or [])]:
            texts.append(alias); concept_ids.append(concept_id); kinds.append(0)
        if concept['definition']:
            texts.append(concept['definition']); concept_ids.append(concept_id); kinds.append(1)
    model = SentenceTransformer(model_name, device='cuda')
    vectors = model.encode(
        texts, batch_size=256, normalize_embeddings=True, show_progress_bar=True,
    ).astype(np.float16)
    np.savez(cache_file, vectors=vectors,
             concept_ids=np.asarray(concept_ids), kinds=np.asarray(kinds, np.uint8))
    return vectors, np.asarray(concept_ids), np.asarray(kinds, np.uint8)


def _hybrid_rankings(payload: dict, anchors: list[str], cache_dir: Path,
                     dense_model: str, reranker_path: str,
                     ) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    vectors, item_concepts, kinds = _load_or_encode_dense(payload, cache_dir, dense_model)
    concepts = payload['concepts']
    queries = [concepts[anchor]['label'] for anchor in anchors]
    dense = SentenceTransformer(dense_model, device='cuda')
    query_vectors = dense.encode(
        queries, batch_size=256, normalize_embeddings=True, show_progress_bar=False,
    ).astype(np.float32)
    alias_mask, definition_mask = kinds == 0, kinds == 1
    alias_vectors = vectors[alias_mask].astype(np.float32)
    definition_vectors = vectors[definition_mask].astype(np.float32)
    alias_concepts, definition_concepts = item_concepts[alias_mask], item_concepts[definition_mask]

    candidate_lists = {}
    for row, (anchor, query) in enumerate(zip(anchors, queries)):
        lexical = _lexical_rank(query, concepts, 50)
        alias = _top_concepts(alias_vectors @ query_vectors[row], alias_concepts, 50)
        definition = _top_concepts(
            definition_vectors @ query_vectors[row], definition_concepts, 50,
        ) if len(definition_vectors) else []
        scores = defaultdict(float)
        for ranked in (lexical, alias, definition):
            for rank, concept_id in enumerate(ranked, 1):
                scores[concept_id] += 1.0 / (60 + rank)
        scores.pop(anchor, None)
        candidate_lists[anchor] = sorted(scores, key=scores.get, reverse=True)[:50]

    reranker = MultiVectorEncoder(
        model_name_or_path=reranker_path, device='cuda', trust_remote_code=True,
    )
    transformer = reranker[0]
    if transformer.query_length is None and transformer.query_expansion is not None:
        transformer.query_length = transformer.query_expansion['length']
    rankings = {}
    for anchor, query in zip(anchors, queries):
        candidates = candidate_lists[anchor]
        documents = [Concept(
            prefix=ConceptPrefix.HPO, conceptId=concept_id,
            label=concepts[concept_id]['label'], synonyms=concepts[concept_id]['synonyms'],
            definition=concepts[concept_id]['definition'], conceptTypes=[],
        ) for concept_id in candidates]
        query_embedding = reranker.encode_query([query], batch_size=32, show_progress_bar=False)
        document_embeddings = reranker.encode_document(
            [render_reranker_candidate(item) for item in documents],
            batch_size=32, show_progress_bar=False,
        )
        scores = reranker.similarity(query_embedding, document_embeddings)[0]
        order = scores.argsort(descending=True).tolist()
        rankings[anchor] = [candidates[index] for index in order]
    return candidate_lists, rankings


def _metrics(rankings: dict[str, list[str]], gold: dict[str, set[str]], anchors: list[str]) -> dict:
    cutoffs = (1, 5, 10, 50)
    values = {f'recall_at_{k}': [] for k in cutoffs}
    values.update({f'hit_at_{k}': [] for k in cutoffs})
    values['mrr'] = []
    values['ndcg_at_10'] = []
    values['coverage'] = []
    for anchor in anchors:
        ranking, relevant = rankings.get(anchor, []), gold[anchor]
        values['coverage'].append(float(bool(ranking)))
        first = next((rank for rank, item in enumerate(ranking, 1) if item in relevant), None)
        values['mrr'].append(1.0 / first if first else 0.0)
        for k in cutoffs:
            found = len(set(ranking[:k]) & relevant)
            values[f'recall_at_{k}'].append(found / len(relevant))
            values[f'hit_at_{k}'].append(float(found > 0))
        dcg = sum(1.0 / math.log2(rank + 1) for rank, item in enumerate(ranking[:10], 1)
                  if item in relevant)
        ideal = sum(1.0 / math.log2(rank + 1) for rank in range(1, min(10, len(relevant)) + 1))
        values['ndcg_at_10'].append(dcg / ideal if ideal else 0.0)
    return {name: float(np.mean(metric)) for name, metric in values.items()}


def _paired_analysis(rankings: dict[str, dict[str, list[str]]], gold: dict[str, set[str]],
                     anchors: list[str], reference: str) -> dict:
    """Paired reciprocal-rank differences with deterministic bootstrap intervals."""
    rng = np.random.default_rng(13)

    def reciprocal_ranks(ranking_by_anchor):
        return np.asarray([
            next((1.0 / rank for rank, item in enumerate(ranking_by_anchor.get(anchor, []), 1)
                  if item in gold[anchor]), 0.0)
            for anchor in anchors
        ])

    reference_values = reciprocal_ranks(rankings[reference])
    output = {}
    for method, ranking in rankings.items():
        if method == reference:
            continue
        values = reciprocal_ranks(ranking)
        delta = values - reference_values
        samples = rng.integers(0, len(anchors), size=(2000, len(anchors)))
        bootstrap = delta[samples].mean(axis=1)
        output[method] = {
            'reference': reference,
            'mrr_delta': float(delta.mean()),
            'mrr_delta_ci95': [float(value) for value in np.quantile(bootstrap, [0.025, 0.975])],
            'wins': int((delta > 0).sum()),
            'ties': int((delta == 0).sum()),
            'losses': int((delta < 0).sum()),
        }
    return output


def _fuse_rankings(rankings: dict[str, dict[str, list[str]]], anchors: list[str],
                   methods: tuple[str, ...]) -> dict[str, list[str]]:
    """Equal-weight RRF fusion used only as an untuned complementarity check."""
    output = {}
    for anchor in anchors:
        scores = defaultdict(float)
        for method in methods:
            for rank, concept_id in enumerate(rankings[method].get(anchor, []), 1):
                scores[concept_id] += 1.0 / (60 + rank)
        output[anchor] = sorted(scores, key=scores.get, reverse=True)[:100]
    return output


def _examples(payload, rankings, novel_gold, anchors, limit=8):
    labels = {key: value['label'] for key, value in payload['concepts'].items()}
    methods = list(rankings)
    rows = []
    for anchor in anchors:
        recalls = {method: len(set(rankings[method].get(anchor, [])[:10]) & novel_gold[anchor])
                   for method in methods}
        if len(set(recalls.values())) == 1:
            continue
        rows.append({
            'anchor': anchor, 'anchor_label': labels.get(anchor), 'gold_count': len(novel_gold[anchor]),
            'recall_counts_at_10': recalls,
            'gold_sample': [[item, labels.get(item)] for item in sorted(novel_gold[anchor])[:8]],
            'top5': {method: [[item, labels.get(item)] for item in ranking.get(anchor, [])[:5]]
                     for method, ranking in rankings.items()},
        })
        if len(rows) == limit:
            break
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--neo4j-uri', default='bolt://127.0.0.1:8902')
    parser.add_argument('--neo4j-user', default='neo4j')
    parser.add_argument('--neo4j-password', default='password')
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--sample-size', type=int, default=300)
    parser.add_argument('--dense-model', default='FremyCompany/BioLORD-2023')
    parser.add_argument('--reranker', default='scripts/reranker/runs/production/final')
    parser.add_argument('--v7-reranker', default=None)
    parser.add_argument('--reuse-snapshot', action='store_true')
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    snapshot = args.output_dir / 'hpo-ordo-snapshot.json.gz'
    started = time.time()
    if args.reuse_snapshot and snapshot.exists():
        with gzip.open(snapshot, 'rt', encoding='utf-8') as handle:
            payload = json.load(handle)
    else:
        payload = create_snapshot(args.neo4j_uri, args.neo4j_user, args.neo4j_password, snapshot)

    train_pairs, test_diseases, anchors, all_gold, novel_gold = _build_split(
        payload, args.sample_size,
    )
    target = OntologyIndex.build(payload['concepts'], payload['hpo_edges'])
    corpus = OntologyIndex.build(payload['ordo_nodes'], payload['ordo_edges'])
    annotations = AnnotationIndex.build(target, corpus, train_pairs)
    context = SimilarityContext(
        ConceptPrefix.HPO, target, ConceptPrefix.ORDO, corpus, annotations, 0.2,
    )
    rankings = {}
    for method in ('co_annotation', 'relevance', 'weighed_relevance'):
        rankings[method] = _score_structure(context, anchors, method)
    rankings['hybrid_rrf'], rankings['hybrid_reranker'] = _hybrid_rankings(
        payload, anchors, args.output_dir / 'cache', args.dense_model, args.reranker,
    )
    if args.v7_reranker:
        _rrf, rankings['hybrid_v7_reranker'] = _hybrid_rankings(
            payload, anchors, args.output_dir / 'cache', args.dense_model, args.v7_reranker,
        )
    rankings['fusion_weighted_production'] = _fuse_rankings(
        rankings, anchors, ('weighed_relevance', 'hybrid_reranker'),
    )
    rankings['fusion_weighted_rrf'] = _fuse_rankings(
        rankings, anchors, ('weighed_relevance', 'hybrid_rrf'),
    )
    results = {
        'protocol': {
            'target': 'hpo', 'corpus': 'ordo', 'split': 'sha256 disease-level 80/20',
            'annotations_total': len(payload['annotations']), 'annotations_train': len(train_pairs),
            'test_diseases': len(test_diseases), 'anchors': len(anchors),
            'candidate_concepts': len(target.node_ids), 'threshold': 0.2,
            'dense_model': args.dense_model, 'reranker': args.reranker,
            'v7_reranker': args.v7_reranker,
            'elapsed_seconds': time.time() - started,
        },
        'all_heldout': {method: _metrics(ranking, all_gold, anchors)
                        for method, ranking in rankings.items()},
        'novel_heldout': {method: _metrics(ranking, novel_gold, anchors)
                          for method, ranking in rankings.items()},
        'paired_all_heldout': _paired_analysis(
            rankings, all_gold, anchors, 'hybrid_reranker',
        ),
        'paired_novel_heldout': _paired_analysis(
            rankings, novel_gold, anchors, 'hybrid_reranker',
        ),
        'examples': _examples(payload, rankings, novel_gold, anchors),
    }
    with gzip.open(args.output_dir / 'rankings.json.gz', 'wt', encoding='utf-8') as handle:
        json.dump({'anchors': anchors, 'rankings': rankings}, handle, separators=(',', ':'))
    with open(args.output_dir / 'results.json', 'w', encoding='utf-8') as handle:
        json.dump(results, handle, indent=2)
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
