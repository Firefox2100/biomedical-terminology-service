"""
API router for searching terms within vocabularies. This is a more advanced search
endpoint that fuses lexical and embedding-based recall (see `bioterms.search.hybrid`) to
find relevant terms based on the input query.
"""

from time import perf_counter
from typing import Annotated, List, Optional
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.metrics import SEARCH_ITEMS, SEARCH_LIMIT, SEARCH_QUERY_LEN
from bioterms.database import DocumentDatabase, VectorDatabase, get_active_doc_db, \
    get_active_vector_db
from bioterms.vocabulary import get_vocabulary_config
from bioterms.model.concept import Concept
from bioterms.model.search import SearchMatchV2, SearchMetadataV2, SearchPipelineV2, \
    SearchResponseV2, SearchResultV2
from bioterms.search.hybrid import SearchExecution, execute_hybrid_search
from .utils import response_generator


search_router = APIRouter(
    prefix='/api',
    tags=['Search'],
)


async def _search(query: str,
                  prefixes: list[ConceptPrefix],
                  limit: int,
                  doc_db: DocumentDatabase,
                  vector_db: VectorDatabase,
                  ) -> SearchExecution:
    """Shared V1/V2 query path; presentation is handled by the individual endpoints."""
    model_classes = {
        prefix: get_vocabulary_config(prefix)['conceptClass'] for prefix in prefixes
    }
    return await execute_hybrid_search(
        query=query,
        prefixes=prefixes,
        doc_db=doc_db,
        vector_db=vector_db,
        model_classes=model_classes,
        limit=limit,
    )


def _metrics_prefix(prefixes: list[ConceptPrefix]) -> str:
    return prefixes[0].value if len(prefixes) == 1 else 'multi'


@search_router.get('/vocabularies/{prefix}/search/v1', response_model=List[Concept])
async def search_terms_v1(prefix: ConceptPrefix,
                          query: Annotated[str, Query(description='The search query string')],
                          doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                          vector_db: Annotated[VectorDatabase, Depends(get_active_vector_db)],
                          limit: Annotated[
                              Optional[int],
                              Query(
                                  description='Maximum number of concepts to return.',
                                  ge=1,
                              )
                          ] = 10,
                          ):
    """
    Search for terms matching the query within the specified vocabulary prefix.

    This method fuses lexical, alias-embedding, and definition-embedding recall (see
    `bioterms.search.hybrid`) via Reciprocal Rank Fusion, with exact matches bypassing fusion.
    \f
    :param prefix: The vocabulary prefix to search within.
    :param query: The search query string.
    :param limit: Maximum number of concepts to return.
    :param doc_db: The document database instance.
    :param vector_db: The vector database instance.
    :return: A list of matching Concept instances.
    """
    SEARCH_QUERY_LEN.labels(prefix=prefix.value).observe(len(query))
    SEARCH_LIMIT.labels(prefix=prefix.value).observe(limit)

    execution = await _search(
        query=query,
        prefixes=[prefix],
        doc_db=doc_db,
        vector_db=vector_db,
        limit=limit or 10,
    )

    async def counting_generator():
        items = 0
        for hit in execution.hits:
            items += 1
            yield hit.concept
        SEARCH_ITEMS.labels(prefix=prefix.value).observe(items)

    return StreamingResponse(
        response_generator(counting_generator()),
        media_type='application/json'
    )


async def _search_terms_v2(query: str,
                           vocabularies: list[ConceptPrefix],
                           limit: int,
                           include_match_details: bool,
                           doc_db: DocumentDatabase,
                           vector_db: VectorDatabase,
                           ) -> SearchResponseV2:
    started = perf_counter()
    vocabularies = list(dict.fromkeys(vocabularies))
    metric_prefix = _metrics_prefix(vocabularies)
    SEARCH_QUERY_LEN.labels(prefix=metric_prefix).observe(len(query))
    SEARCH_LIMIT.labels(prefix=metric_prefix).observe(limit)

    execution = await _search(query, vocabularies, limit, doc_db, vector_db)
    results = []
    for rank, hit in enumerate(execution.hits, start=1):
        match = None
        if include_match_details:
            match = SearchMatchV2(
                type='exact' if hit.exact else 'hybrid',
                exact=hit.exact,
                field=hit.match_field,
                text=hit.matched_text,
            )
        results.append(SearchResultV2(rank=rank, concept=hit.concept, match=match))

    SEARCH_ITEMS.labels(prefix=metric_prefix).observe(len(results))
    return SearchResponseV2(
        query=query.strip(),
        results=results,
        meta=SearchMetadataV2(
            returned=len(results),
            limit=limit,
            durationMs=(perf_counter() - started) * 1000,
            vocabularies=vocabularies,
            pipeline=SearchPipelineV2(
                vector=execution.vector_used,
                mapped=execution.mapped_used,
                reranker=execution.reranker_used,
            ),
        ),
    )


@search_router.get(
    '/search/v2', response_model=SearchResponseV2, response_model_exclude_none=True,
)
async def search_terms_v2(
    query: Annotated[str, Query(description='The search query string', min_length=1)],
    vocabulary: Annotated[
        list[ConceptPrefix],
        Query(description='Vocabulary to search. Repeat this parameter to search multiple vocabularies.'),
    ],
    doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
    vector_db: Annotated[VectorDatabase, Depends(get_active_vector_db)],
    limit: Annotated[int, Query(description='Maximum number of concepts to return.', ge=1, le=100)] = 10,
    include_match_details: Annotated[
        bool,
        Query(alias='includeMatchDetails', description='Include exact-versus-hybrid match details.'),
    ] = True,
) -> SearchResponseV2:
    """Search one or more vocabularies using lexical, vector, fusion, and reranking stages."""
    return await _search_terms_v2(
        query, vocabulary, limit, include_match_details, doc_db, vector_db,
    )


@search_router.get(
    '/vocabularies/{prefix}/search/v2',
    response_model=SearchResponseV2,
    response_model_exclude_none=True,
)
async def search_terms_v2_scoped(
    prefix: ConceptPrefix,
    query: Annotated[str, Query(description='The search query string', min_length=1)],
    doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
    vector_db: Annotated[VectorDatabase, Depends(get_active_vector_db)],
    limit: Annotated[int, Query(description='Maximum number of concepts to return.', ge=1, le=100)] = 10,
    include_match_details: Annotated[
        bool,
        Query(alias='includeMatchDetails', description='Include exact-versus-hybrid match details.'),
    ] = True,
) -> SearchResponseV2:
    """Vocabulary-scoped convenience form of GET /api/search/v2."""
    return await _search_terms_v2(
        query, [prefix], limit, include_match_details, doc_db, vector_db,
    )
