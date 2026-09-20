"""Hybrid lexical and vector concept search using Reciprocal Rank Fusion.

Exact ID, label, and synonym matches are pinned first. Vocabularies without embeddings fall
back to lexical search without running the embedding model.
"""
import asyncio
import math
from collections.abc import AsyncIterator

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.database import Cache
from bioterms.database.doc_db import DocumentDatabase
from bioterms.database.vector_db import VectorDatabase
from bioterms.embedding import TextTransformer
from bioterms.model.concept import Concept
from bioterms.vocabulary import get_vocabulary_status
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


def _is_exact_match(concept: Concept,
                    query_folded: str,
                    ) -> bool:
    """Return whether the ID, label, or a synonym exactly matches the query."""
    if concept.concept_id.casefold() == query_folded:
        return True

    candidates = ([concept.label] if concept.label else []) + (concept.synonyms or [])
    return any(candidate and candidate.casefold() == query_folded for candidate in candidates)


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
    normalized_query = query.strip()
    if not normalized_query:
        return

    query_folded = normalized_query.casefold()
    rerank_limit = max(limit, CONFIG.reranker_candidate_limit) if reranker_enabled() else limit
    retrieval_limit = max(limit, rerank_limit, CONFIG.search_retrieval_candidate_limit)
    vector_retrieval_limit = math.ceil(
        retrieval_limit * CONFIG.search_vector_overretrieve_factor
    )
    probe_limit = max(retrieval_limit, _EXACT_MATCH_PROBE_LIMIT)

    # No embedding items loaded for this vocabulary (offline-only generation, restore not run
    # yet, or a vocabulary type with no embedding step at all) means the alias/definition recall
    # arms can only ever come back empty -- so skip embedding the query (and the two vector
    # searches) entirely rather than paying for a model inference call whose result is
    # guaranteed to be discarded. The vector count comes off the cached vocabulary status
    # (`get_vocabulary_status` -- the same one `/data/status` uses, invalidated by
    # `cache.rotate_dataset_version()` on every embed/restore) rather than a fresh
    # `vector_db.count_vectors` call on every search request, so the common case (repeated
    # searches between dataset changes) costs a cache read, not a live DB round trip.
    vocab_status = await get_vocabulary_status(prefix=prefix, cache=cache, doc_db=doc_db, vector_db=vector_db)
    vectors_loaded = vocab_status.vector_count > 0

    if vectors_loaded:
        query_vector = TextTransformer().embed_strings([normalized_query])[0]

        lexical_results, alias_results, definition_results, exact_id_matches = await asyncio.gather(
            doc_db.lexical_search(prefix=prefix, query=normalized_query, limit=probe_limit),
            vector_db.search_items(
                query_vector=query_vector, prefix=prefix, kind=EmbeddingKind.ALIAS,
                limit=vector_retrieval_limit,
            ),
            vector_db.search_items(
                query_vector=query_vector, prefix=prefix, kind=EmbeddingKind.DEFINITION,
                limit=vector_retrieval_limit,
            ),
            doc_db.get_terms_by_ids(prefix=prefix, concept_ids=[normalized_query], model_class=model_class),
        )
    else:
        alias_results, definition_results = [], []
        lexical_results, exact_id_matches = await asyncio.gather(
            doc_db.lexical_search(prefix=prefix, query=normalized_query, limit=probe_limit),
            doc_db.get_terms_by_ids(prefix=prefix, concept_ids=[normalized_query], model_class=model_class),
        )

    lexical_ranked = [concept_id for concept_id, _score in lexical_results]
    alias_ranked = _dedupe_by_concept(alias_results)
    definition_ranked = _dedupe_by_concept(definition_results)

    exact_concept_ids: list[str] = [c.concept_id for c in exact_id_matches]
    exact_seen = set(exact_concept_ids)

    if lexical_ranked:
        probe_concepts = await doc_db.get_terms_by_ids(
            prefix=prefix, concept_ids=lexical_ranked, model_class=model_class,
        )
        probe_by_id = {c.concept_id: c for c in probe_concepts}

        for concept_id in lexical_ranked:
            if concept_id in exact_seen:
                continue
            concept = probe_by_id.get(concept_id)
            if concept is not None and _is_exact_match(concept, query_folded):
                exact_concept_ids.append(concept_id)
                exact_seen.add(concept_id)

    fused_ranked = _reciprocal_rank_fusion(
        [lexical_ranked[:retrieval_limit], alias_ranked, definition_ranked],
        k=CONFIG.search_rrf_k,
    )

    non_exact_ids = [
        concept_id for concept_id in fused_ranked
        if concept_id not in exact_seen
    ][:rerank_limit]
    candidate_ids = exact_concept_ids + non_exact_ids
    if not candidate_ids:
        return

    # Fetched by ID rather than streamed straight from the recall arms, and re-ordered here to
    # `final_ids` explicitly: document databases matching on an ID list (Mongo's `$in`, SQL's
    # `IN (...)`) are not guaranteed to preserve that list's order.
    concepts = await doc_db.get_terms_by_ids(prefix=prefix, concept_ids=candidate_ids, model_class=model_class)
    concepts_by_id = {c.concept_id: c for c in concepts}

    exact_concepts = [
        concepts_by_id[concept_id] for concept_id in exact_concept_ids
        if concept_id in concepts_by_id
    ]
    remaining_slots = max(0, limit - len(exact_concepts))
    if remaining_slots:
        non_exact_concepts = [
            concepts_by_id[concept_id] for concept_id in non_exact_ids
            if concept_id in concepts_by_id
        ]
        # Exact ID/label/synonym matches bypass both RRF and the reranker. Only the semantic
        # remainder is scored, and no model is loaded when exact matches fill the response.
        non_exact_concepts = await rerank_concepts(normalized_query, non_exact_concepts)
    else:
        non_exact_concepts = []

    for concept in (exact_concepts + non_exact_concepts[:remaining_slots])[:limit]:
        yield concept
