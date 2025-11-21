from typing import List
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.base import JsonModel
from bioterms.model.expanded_term import ExpandedTerm
from .utils import response_generator


expand_router = APIRouter(
    prefix='/api',
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
                          depth: int = Query(
                              3,
                              description='The depth of the expansion. 0 for no limit.'
                          ),
                          result_threshold: int = Query(
                              0,
                              description='The maximum number of terms to return in the response. 0 for no limit.'
                          ),
                          graph_db: GraphDatabase = Depends(get_active_graph_db),
                          ):
    """
    Expand terms to their descendants up to a specified depth (V1). This API is compatible with Cafe Variome
    V3 backend.
    \f
    :param prefix: The vocabulary prefix.
    :param requested_terms: The request body containing term IDs to expand.
    :param depth: The depth of the expansion. 0 for no limit.
    :param result_threshold: The maximum number of terms to return in the response. 0 for no limit.
    :param graph_db: The graph database instance.
    :return: A list of expanded terms.
    """
    expand_iter = graph_db.expand_terms_iter(
        prefix=prefix,
        concept_ids=requested_terms.term_ids,
        max_depth=depth,
    )

    v1_expanded_terms = []
    async for expanded_term in expand_iter:
        v1_expanded_term = ExpandedTermV1(
            termId=expanded_term.concept_id,
            children=expanded_term.descendants[:result_threshold]
                if result_threshold > 0 else expanded_term.descendants,
            depth=depth,
        )
        v1_expanded_terms.append(v1_expanded_term)


@expand_router.get('/{prefix}/expand/v2', response_model=List[ExpandedTerm])
async def expand_terms_v2(prefix: ConceptPrefix,
                          concept_ids: List[str] = Query(
                              ...,
                              description='List of concept IDs to expand.'
                          ),
                          depth: int | None = Query(
                              None,
                              description='Maximum depth for expansion.'
                          ),
                          graph_db: GraphDatabase = Depends(get_active_graph_db),
                          ):
    """
    Expand terms to their descendants up to a specified depth (V2). This API returns
    an asynchronous stream of results to handle large datasets efficiently.
    \f
    :param prefix: The vocabulary prefix.
    :param concept_ids: List of concept IDs to expand.
    :param depth: Maximum depth for expansion.
    :param graph_db: The graph database instance.
    """
    expand_iter = graph_db.expand_terms_iter(
        prefix=prefix,
        concept_ids=concept_ids,
        max_depth=depth,
    )

    return StreamingResponse(
        response_generator(expand_iter),
        media_type='application/json'
    )
