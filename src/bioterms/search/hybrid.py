"""
Hybrid concept search: fuses three independent recall arms -- lexical (BM25/native full-text
ranking, or n-gram overlap where a backend has neither), alias-embedding (a concept's label
and each of its synonyms, individually embedded), and definition-embedding -- into a single
ranked list of concepts, via Reciprocal Rank Fusion (RRF).

An exact match (the query string equals a concept's ID, label, or a synonym, case-
insensitively) bypasses RRF entirely and is pinned to the front of the results: rank-based
fusion can otherwise bury a short, exact term under a flood of merely-similar embeddings or
partial lexical hits, which is the one failure mode this is designed to categorically avoid.

This module is the single implementation shared by the REST `/search/v1` endpoint, the
GraphQL `search` resolver, and the MCP `search_vocabulary` tool -- previously each of the
three duplicated the same "embed query, run vector search, fetch documents" logic.
"""
import asyncio
from collections.abc import AsyncIterator

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.database.doc_db import DocumentDatabase
from bioterms.database.vector_db import VectorDatabase
from bioterms.embedding import TextTransformer
from bioterms.model.concept import Concept


# The lexical recall arm is probed for more results than were actually requested, so that an
# exact label/synonym match -- which lexical scoring puts at or near the top of its own
# ranking -- is reliably present for the exact-match check below even when `limit` is small.
# This avoids needing a dedicated normalised-equality index/column across every document
# database backend just to guarantee exact-match recall.
_EXACT_MATCH_PROBE_LIMIT = 50


def _reciprocal_rank_fusion(ranked_lists: list[list[str]],
                            k: int,
                            ) -> list[str]:
    """
    Fuse several best-first ranked lists of concept IDs into one, by Reciprocal Rank Fusion:
    score(c) = sum, over every list containing c, of 1 / (k + rank), rank being 1-based.
    :param ranked_lists: The recall arms' ranked concept ID lists.
    :param k: The RRF k constant (see `BTS_SEARCH_RRF_K`).
    :return: Concept IDs ordered by fused score, best first.
    """
    scores: dict[str, float] = {}

    for ranked in ranked_lists:
        seen: set[str] = set()
        for rank, concept_id in enumerate(ranked, start=1):
            if concept_id in seen:
                # A recall arm should not repeat a concept ID, but guard anyway so a
                # misbehaving arm can't double-count itself.
                continue
            seen.add(concept_id)
            scores[concept_id] = scores.get(concept_id, 0.0) + 1.0 / (k + rank)

    return sorted(scores, key=lambda concept_id: scores[concept_id], reverse=True)


def _dedupe_by_concept(items: list[tuple[str, str, float]]) -> list[str]:
    """
    Collapse a vector search arm's (concept_id, item_text, score) hits -- several items (e.g.
    two different synonyms) can belong to the same concept -- into one best-first concept ID
    ranking, keeping each concept's first (best-scoring) occurrence.
    :param items: (concept_id, item_text, score) tuples, best match first.
    :return: A best-first list of distinct concept IDs.
    """
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
    """
    Whether a concept's ID, label, or any synonym exactly matches the query (concept ID
    compared as-is, since IDs are not casing-conventional; label/synonyms case-insensitively).
    :param concept: The concept to check.
    :param query_folded: The case-folded query string.
    :return: True if this is an exact match.
    """
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
    :return: An async iterator of matching Concept instances, best match first.
    """
    normalized_query = query.strip()
    if not normalized_query:
        return

    query_folded = normalized_query.casefold()
    probe_limit = max(limit, _EXACT_MATCH_PROBE_LIMIT)

    query_vector = TextTransformer().embed_strings([normalized_query])[0]

    lexical_results, alias_results, definition_results, exact_id_matches = await asyncio.gather(
        doc_db.lexical_search(prefix=prefix, query=normalized_query, limit=probe_limit),
        vector_db.search_items(query_vector=query_vector, prefix=prefix, kind=EmbeddingKind.ALIAS, limit=limit),
        vector_db.search_items(
            query_vector=query_vector, prefix=prefix, kind=EmbeddingKind.DEFINITION, limit=limit,
        ),
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
        [lexical_ranked[:limit], alias_ranked, definition_ranked],
        k=CONFIG.search_rrf_k,
    )

    final_ids: list[str] = []
    seen: set[str] = set()
    for concept_id in exact_concept_ids + fused_ranked:
        if concept_id in seen:
            continue
        seen.add(concept_id)
        final_ids.append(concept_id)
        if len(final_ids) >= limit:
            break

    if not final_ids:
        return

    # Fetched by ID rather than streamed straight from the recall arms, and re-ordered here to
    # `final_ids` explicitly: document databases matching on an ID list (Mongo's `$in`, SQL's
    # `IN (...)`) are not guaranteed to preserve that list's order.
    concepts = await doc_db.get_terms_by_ids(prefix=prefix, concept_ids=final_ids, model_class=model_class)
    concepts_by_id = {c.concept_id: c for c in concepts}

    for concept_id in final_ids:
        concept = concepts_by_id.get(concept_id)
        if concept is not None:
            yield concept
