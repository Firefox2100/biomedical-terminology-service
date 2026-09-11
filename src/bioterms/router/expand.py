"""
Router for expanding terms in vocabularies.
"""

from typing import Annotated, List
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.metrics import EXPAND_ROOTS, EXPAND_DEPTH, EXPAND_LIMIT, EXPAND_REQS
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.base import JsonModel
from bioterms.model.related_term import RelatedTerm
from .utils import response_generator


expand_router = APIRouter(
    prefix='/api/vocabularies',
    tags=['Expansion'],
)


class ExpandRequestV1(JsonModel):
    """
    Request model for the expand terms endpoint (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    term_ids: List[str] = Field(
        ...,
        description='List of term IDs to expand.',
        alias='termIds',
    )


class ExpandedTermV1(JsonModel):
    """
    Data model for an expanded term in the expand terms response (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    term_id: str = Field(
        ...,
        description='The term ID of the expanded term.',
        alias='termId',
    )
    children: List[str] = Field(
        ...,
        description='List of child term IDs.',
    )
    depth: int = Field(
        ...,
        description='The depth of the term in the expansion hierarchy.',
    )


@expand_router.post('/{prefix}/expand/v1', response_model=List[ExpandedTermV1])
async def expand_terms_v1(prefix: ConceptPrefix,
                          requested_terms: ExpandRequestV1,
                          graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                          depth: Annotated[
                              int,
                              Query(description='The depth of the expansion. 0 for no limit.')
                          ] = 3,
                          result_threshold: Annotated[
                              int,
                              Query(
                                  description='The maximum number of terms to return in the response. '
                                              '0 for no limit.'
                              )
                          ] = 0,
                          ):
    """
    Expand terms to their descendants up to a specified depth (V1). This API is compatible with
    Cafe Variome V3 backend.
    \f
    :param prefix: The vocabulary prefix.
    :param requested_terms: The request body containing term IDs to expand.
    :param depth: The depth of the expansion. 0 for no limit.
    :param result_threshold: The maximum number of terms to return in the response. 0 for no limit.
    :param graph_db: The graph database instance.
    :return: A list of expanded terms.
    """
    mode = 'unbounded' if depth is None else 'bounded'
    EXPAND_REQS.labels(prefix=prefix.value, mode=mode).inc()
    EXPAND_ROOTS.labels(prefix=prefix.value).observe(len(requested_terms.term_ids))
    if depth is not None:
        EXPAND_DEPTH.labels(prefix=prefix.value).observe(depth)
    EXPAND_LIMIT.labels(
        prefix=prefix.value,
        has_limit=('no' if not result_threshold else 'yes')
    ).observe(result_threshold)

    expand_iter = graph_db.expand_terms_iter(
        prefix=prefix,
        concept_ids=requested_terms.term_ids,
        max_depth=depth if depth > 0 else None,
        limit=result_threshold if result_threshold > 0 else None,
    )

    v1_expanded_terms = []
    async for expanded_term in expand_iter:
        v1_expanded_term = ExpandedTermV1(
            termId=expanded_term.concept_id,
            children=expanded_term.related_concepts,
            depth=depth,
        )
        v1_expanded_terms.append(v1_expanded_term)

    return v1_expanded_terms


@expand_router.get('/{prefix}/expand/v2', response_model=List[RelatedTerm])
async def expand_terms_v2(prefix: ConceptPrefix,
                          concept_ids: Annotated[
                              List[str],
                              Query(description='List of concept IDs to expand.')
                          ],
                          graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                          depth: Annotated[
                              int | None,
                              Query(description='Maximum depth for expansion.')
                          ] = None,
                          limit: Annotated[
                              int | None,
                              Query(description='Maximum number of descendants to return for each term.')
                          ] = None,
                          ):
    """
    Expand terms to their descendants up to a specified depth (V2). This API returns
    an asynchronous stream of results to handle large datasets efficiently.
    \f
    :param prefix: The vocabulary prefix.
    :param concept_ids: List of concept IDs to expand.
    :param depth: Maximum depth for expansion.
    :param limit: Maximum number of descendants to return for each term.
    :param graph_db: The graph database instance.
    """
    mode = 'unbounded' if depth is None else 'bounded'
    EXPAND_REQS.labels(prefix=prefix.value, mode=mode).inc()
    EXPAND_ROOTS.labels(prefix=prefix.value).observe(len(concept_ids))
    if depth is not None:
        EXPAND_DEPTH.labels(prefix=prefix.value).observe(depth)
    EXPAND_LIMIT.labels(
        prefix=prefix.value,
        has_limit=('no' if limit is None else 'yes')
    ).observe(limit or 0)

    expand_iter = graph_db.expand_terms_iter(
        prefix=prefix,
        concept_ids=concept_ids,
        max_depth=depth,
        limit=limit,
    )

    return StreamingResponse(
        response_generator(expand_iter),
        media_type='application/json'
    )
