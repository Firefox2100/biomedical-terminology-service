"""Hybrid lexical and vector concept search using Reciprocal Rank Fusion.

Exact ID, label, and synonym matches are pinned first. Vocabularies without embeddings fall
back to lexical search without running the embedding model.
"""
import asyncio
import math
from dataclasses import dataclass
from collections.abc import AsyncIterator
from collections.abc import Sequence
from functools import lru_cache

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.database import Cache, get_active_graph_db
from bioterms.database.doc_db import DocumentDatabase
from bioterms.database.vector_db import VectorDatabase
from bioterms.database.graph_db import GraphDatabase
from bioterms.embedding import TextTransformer
from bioterms.model.concept import Concept
from bioterms.vocabulary import get_vocabulary_status, get_vocabulary_config
from bioterms.vocabulary.utils import get_vocabulary_module
from bioterms.search.reranker import reranker_enabled, rerank_concepts


# Probe beyond the requested limit so lexical exact matches remain available for pinning.
_EXACT_MATCH_PROBE_LIMIT = 50


def _reciprocal_rank_fusion(ranked_lists: list[list[str]],
                            k: int,
                            ) -> list[str]:
    """Fuse best-first concept rankings using Reciprocal Rank Fusion."""
    scores: dict[str, float] = {}

    for ranked in ranked_lists:
        seen: set[str] = set()
        for rank, concept_id in enumerate(ranked, start=1):
            if concept_id in seen:
                # Do not let one recall arm count the same concept twice.
                continue
            seen.add(concept_id)
            scores[concept_id] = scores.get(concept_id, 0.0) + 1.0 / (k + rank)

    return sorted(scores, key=lambda concept_id: scores[concept_id], reverse=True)


def _reciprocal_rank_fusion_scores(ranked_lists: list[list[str]],
                                   k: int,
                                   ) -> dict[str, float]:
    """Return concept-level RRF scores for merging independently queried vocabularies."""
    scores: dict[str, float] = {}
    for ranked in ranked_lists:
        seen: set[str] = set()
        for rank, concept_id in enumerate(ranked, start=1):
            if concept_id in seen:
                continue
            seen.add(concept_id)
            scores[concept_id] = scores.get(concept_id, 0.0) + 1.0 / (k + rank)
    return scores


def _dedupe_by_concept(items: list[tuple[str, str, float]]) -> list[str]:
    """Keep the best-ranked vector hit for each concept."""
    seen: set[str] = set()
    ordered: list[str] = []

    for concept_id, _text, _score in items:
        if concept_id in seen:
            continue
        seen.add(concept_id)
        ordered.append(concept_id)

    return ordered


def _exact_match(concept: Concept,
                 query_folded: str,
                 ) -> tuple[str, str] | None:
    """Return the exactly matched public field and text, if any."""
    if concept.concept_id.casefold() == query_folded:
        return 'conceptId', concept.concept_id

    if concept.label and concept.label.casefold() == query_folded:
        return 'label', concept.label

    for synonym in concept.synonyms or []:
        if synonym and synonym.casefold() == query_folded:
            return 'synonym', synonym

    return None


def _is_exact_match(concept: Concept,
                    query_folded: str,
                    ) -> bool:
    """Compatibility helper for callers/tests interested only in exactness."""
    return _exact_match(concept, query_folded) is not None


@dataclass(frozen=True)
class SearchHit:
    """A ranked concept plus stable, implementation-independent match information."""
    concept: Concept
    exact: bool = False
    match_field: str | None = None
    matched_text: str | None = None


@dataclass(frozen=True)
class SearchExecution:
    """Materialized result of one shared V1/V2 hybrid search execution."""
    hits: list[SearchHit]
    vector_used: bool
    reranker_used: bool
    mapped_used: bool = False


@dataclass(frozen=True)
class _PrefixCandidates:
    exact: list[SearchHit]
    semantic: list[tuple[Concept, float]]
    vector_used: bool


@lru_cache(maxsize=None)
def _annotation_partners(target_prefix: ConceptPrefix) -> tuple[ConceptPrefix, ...]:
    """Return vocabularies declaring an annotation relationship with the target."""
    partners = set(getattr(get_vocabulary_module(target_prefix), 'ANNOTATIONS', []))
    for prefix in ConceptPrefix:
        if target_prefix in getattr(get_vocabulary_module(prefix), 'ANNOTATIONS', []):
            partners.add(prefix)
    partners.discard(target_prefix)
    return tuple(sorted(partners, key=lambda prefix: prefix.value))


async def _mapped_recall(query: str,
                         target_prefix: ConceptPrefix,
                         doc_db: DocumentDatabase,
                         graph_db: GraphDatabase,
                         source_depth: int | None = None,
                         candidate_limit: int | None = None,
                         ) -> list[str]:
    """Lexically retrieve annotation partners and map their hits into the target vocabulary."""
    source_depth = (CONFIG.search_mapped_recall_limit
                    if source_depth is None else source_depth)
    candidate_limit = (CONFIG.search_mapped_candidate_limit
                       if candidate_limit is None else candidate_limit)
    if source_depth <= 0:
        return []
    partners = _annotation_partners(target_prefix)
    if not partners:
        return []
    lexical_by_partner = await asyncio.gather(*[
        doc_db.lexical_search(prefix=partner, query=query, limit=source_depth)
        for partner in partners
    ])
    lexical_ids_by_partner = [
        list(dict.fromkeys(concept_id for concept_id, _score in hits))
        for hits in lexical_by_partner
    ]
    source_concepts_by_partner = await asyncio.gather(*[
        doc_db.get_terms_by_ids(
            prefix=partner,
            concept_ids=source_ids,
            model_class=get_vocabulary_config(partner)['conceptClass'],
        )
        for partner, source_ids in zip(partners, lexical_ids_by_partner)
    ])
    query_folded = query.casefold()
    source_ids_by_partner = []
    for lexical_ids, concepts in zip(lexical_ids_by_partner, source_concepts_by_partner):
        by_id = {concept.concept_id: concept for concept in concepts}
        source_ids_by_partner.append([
            concept_id for concept_id in lexical_ids
            if concept_id in by_id and _is_exact_match(by_id[concept_id], query_folded)
        ])
    mappings_by_partner = await asyncio.gather(*[
        graph_db.get_exact_mappings(partner, source_ids, target_prefix)
        for partner, source_ids in zip(partners, source_ids_by_partner)
    ])
    ranked: list[str] = []
    seen: set[str] = set()
    # Interleave partners by source rank so a large vocabulary cannot consume the cap first.
    for source_rank in range(source_depth):
        for source_ids, mappings in zip(source_ids_by_partner, mappings_by_partner):
            if source_rank >= len(source_ids):
                continue
            for target_id in sorted(mappings.get(source_ids[source_rank], [])):
                if target_id in seen:
                    continue
                seen.add(target_id)
                ranked.append(target_id)
                if len(ranked) >= candidate_limit:
                    return ranked
    return ranked


async def _add_mapped_recall(candidates: _PrefixCandidates,
                             query: str,
                             target_prefix: ConceptPrefix,
                             doc_db: DocumentDatabase,
                             graph_db: GraphDatabase,
                             model_class: type[Concept],
                             ) -> _PrefixCandidates:
    """Add mapped concepts as an ordinary, unpinned RRF arm."""
    mapped_ranked = await _mapped_recall(query, target_prefix, doc_db, graph_db)
    if not mapped_ranked:
        return candidates
    exact_ids = {hit.concept.concept_id for hit in candidates.exact}
    scores = {concept.concept_id: score for concept, score in candidates.semantic}
    concepts_by_id = {concept.concept_id: concept for concept, _score in candidates.semantic}
    missing_ids = [concept_id for concept_id in mapped_ranked
                   if concept_id not in exact_ids and concept_id not in concepts_by_id]
    if missing_ids:
        missing = await doc_db.get_terms_by_ids(
            prefix=target_prefix, concept_ids=missing_ids, model_class=model_class,
        )
        concepts_by_id.update((concept.concept_id, concept) for concept in missing)
    for rank, concept_id in enumerate(mapped_ranked, start=1):
        if concept_id in exact_ids or concept_id not in concepts_by_id:
            continue
        scores[concept_id] = scores.get(concept_id, 0.0) + 1.0 / (CONFIG.search_rrf_k + rank)
    semantic = sorted(
        ((concepts_by_id[concept_id], score) for concept_id, score in scores.items()),
        key=lambda item: item[1], reverse=True,
    )
    return _PrefixCandidates(candidates.exact, semantic, candidates.vector_used)


async def _retrieve_prefix(query: str,
                           query_folded: str,
                           prefix: ConceptPrefix,
                           doc_db: DocumentDatabase,
                           vector_db: VectorDatabase,
                           model_class: type[Concept],
                           retrieval_limit: int,
                           vector_retrieval_limit: int,
                           vectors_loaded: bool,
                           query_vector: list[float] | None,
                           ) -> _PrefixCandidates:
    """Retrieve exact and fused candidates for one vocabulary without reranking them."""
    probe_limit = max(retrieval_limit, _EXACT_MATCH_PROBE_LIMIT)

    if vectors_loaded:
        assert query_vector is not None
        lexical_results, alias_results, definition_results, exact_id_matches = await asyncio.gather(
            doc_db.lexical_search(prefix=prefix, query=query, limit=probe_limit),
            vector_db.search_items(
                query_vector=query_vector, prefix=prefix, kind=EmbeddingKind.ALIAS,
                limit=vector_retrieval_limit,
            ),
            vector_db.search_items(
                query_vector=query_vector, prefix=prefix, kind=EmbeddingKind.DEFINITION,
                limit=vector_retrieval_limit,
            ),
            doc_db.get_terms_by_ids(prefix=prefix, concept_ids=[query], model_class=model_class),
        )
    else:
        alias_results, definition_results = [], []
        lexical_results, exact_id_matches = await asyncio.gather(
            doc_db.lexical_search(prefix=prefix, query=query, limit=probe_limit),
            doc_db.get_terms_by_ids(prefix=prefix, concept_ids=[query], model_class=model_class),
        )

    lexical_ranked = [concept_id for concept_id, _score in lexical_results]
    alias_ranked = _dedupe_by_concept(alias_results)
    definition_ranked = _dedupe_by_concept(definition_results)

    exact_hits: list[SearchHit] = []
    exact_seen: set[str] = set()
    for concept in exact_id_matches:
        exact_hits.append(SearchHit(
            concept=concept, exact=True, match_field='conceptId', matched_text=concept.concept_id,
        ))
        exact_seen.add(concept.concept_id)

    if lexical_ranked:
        probe_concepts = await doc_db.get_terms_by_ids(
            prefix=prefix, concept_ids=lexical_ranked, model_class=model_class,
        )
        probe_by_id = {concept.concept_id: concept for concept in probe_concepts}
        for concept_id in lexical_ranked:
            if concept_id in exact_seen:
                continue
            concept = probe_by_id.get(concept_id)
            if concept is None:
                continue
            match = _exact_match(concept, query_folded)
            if match is not None:
                exact_hits.append(SearchHit(
                    concept=concept, exact=True, match_field=match[0], matched_text=match[1],
                ))
                exact_seen.add(concept_id)

    rrf_scores = _reciprocal_rank_fusion_scores(
        [lexical_ranked[:retrieval_limit], alias_ranked, definition_ranked],
        k=CONFIG.search_rrf_k,
    )
    fused_ranked = sorted(rrf_scores, key=rrf_scores.__getitem__, reverse=True)
    non_exact_ids = [concept_id for concept_id in fused_ranked if concept_id not in exact_seen]
    if not non_exact_ids:
        return _PrefixCandidates(exact_hits, [], vectors_loaded)

    concepts = await doc_db.get_terms_by_ids(
        prefix=prefix, concept_ids=non_exact_ids, model_class=model_class,
    )
    concepts_by_id = {concept.concept_id: concept for concept in concepts}
    semantic = [
        (concepts_by_id[concept_id], rrf_scores[concept_id])
        for concept_id in non_exact_ids
        if concept_id in concepts_by_id
    ]
    return _PrefixCandidates(exact_hits, semantic, vectors_loaded)


async def execute_hybrid_search(query: str,
                                prefixes: Sequence[ConceptPrefix],
                                doc_db: DocumentDatabase,
                                vector_db: VectorDatabase,
                                model_classes: dict[ConceptPrefix, type[Concept]] | None = None,
                                limit: int = 10,
                                cache: Cache = None,
                                ) -> SearchExecution:
    """Run the shared V1/V2 search pipeline across one or more vocabularies."""
    normalized_query = query.strip()
    ordered_prefixes = list(dict.fromkeys(prefixes))
    if not normalized_query or not ordered_prefixes:
        return SearchExecution([], vector_used=False, reranker_used=False)

    query_folded = normalized_query.casefold()
    rerank_limit = max(limit, CONFIG.reranker_candidate_limit) if reranker_enabled() else limit
    retrieval_limit = max(limit, rerank_limit, CONFIG.search_retrieval_candidate_limit)
    vector_retrieval_limit = math.ceil(
        retrieval_limit * CONFIG.search_vector_overretrieve_factor
    )
    model_classes = model_classes or {}

    # Resolve vector availability first, then embed once for every vector-backed vocabulary
    # in a multi-vocabulary V2 request. Vocabularies without vectors still use lexical recall.
    vocabulary_statuses = await asyncio.gather(*[
        get_vocabulary_status(
            prefix=prefix, cache=cache, doc_db=doc_db, vector_db=vector_db,
        )
        for prefix in ordered_prefixes
    ])
    vectors_loaded = [status.vector_count > 0 for status in vocabulary_statuses]
    query_vector = (
        TextTransformer().embed_strings([normalized_query])[0]
        if any(vectors_loaded)
        else None
    )

    per_prefix = await asyncio.gather(*[
        _retrieve_prefix(
            normalized_query, query_folded, prefix, doc_db, vector_db,
            model_classes.get(prefix, Concept), retrieval_limit, vector_retrieval_limit,
            prefix_vectors_loaded, query_vector,
        )
        for prefix, prefix_vectors_loaded in zip(ordered_prefixes, vectors_loaded)
    ])

    if CONFIG.search_mapped_recall_limit > 0:
        graph_db = get_active_graph_db()
        # The active graph backend is process-shared and is closed by application shutdown,
        # not by an individual search request.
        per_prefix = await asyncio.gather(*[
            _add_mapped_recall(
                candidates, normalized_query, prefix, doc_db, graph_db,
                model_classes.get(prefix, Concept),
            )
            for prefix, candidates in zip(ordered_prefixes, per_prefix)
        ])

    exact_hits: list[SearchHit] = []
    semantic_candidates: list[tuple[Concept, float, int]] = []
    for prefix_index, candidates in enumerate(per_prefix):
        exact_hits.extend(candidates.exact)
        semantic_candidates.extend(
            (concept, score, prefix_index) for concept, score in candidates.semantic
        )

    remaining_slots = max(0, limit - len(exact_hits))
    reranker_was_used = False
    semantic_hits: list[SearchHit] = []
    if remaining_slots:
        semantic_candidates.sort(key=lambda item: (-item[1], item[2]))
        candidates = [item[0] for item in semantic_candidates[:rerank_limit]]
        if candidates and reranker_enabled():
            candidates = await rerank_concepts(normalized_query, candidates)
            reranker_was_used = True
        semantic_hits = [SearchHit(concept=concept) for concept in candidates[:remaining_slots]]

    return SearchExecution(
        hits=(exact_hits + semantic_hits)[:limit],
        vector_used=any(candidates.vector_used for candidates in per_prefix),
        reranker_used=reranker_was_used,
        mapped_used=CONFIG.search_mapped_recall_limit > 0,
    )


async def hybrid_search(query: str,
                        prefix: ConceptPrefix,
                        doc_db: DocumentDatabase,
                        vector_db: VectorDatabase,
                        model_class: type[Concept] = Concept,
                        limit: int = 10,
                        cache: Cache = None,
                        ) -> AsyncIterator[Concept]:
    """
    Search a vocabulary for concepts matching `query`, fusing lexical, alias-embedding, and
    definition-embedding recall (see module docstring), with exact matches pinned ahead of
    the fused results.
    :param query: The search query string.
    :param prefix: The vocabulary prefix to search within.
    :param doc_db: The document database instance.
    :param vector_db: The vector database instance.
    :param model_class: The Concept subclass to instantiate results as.
    :param limit: The maximum number of concepts to return.
    :param cache: The cache instance.
    :return: An async iterator of matching Concept instances, best match first.
    """
    execution = await execute_hybrid_search(
        query=query,
        prefixes=[prefix],
        doc_db=doc_db,
        vector_db=vector_db,
        model_classes={prefix: model_class},
        limit=limit,
        cache=cache,
    )
    for hit in execution.hits:
        yield hit.concept
