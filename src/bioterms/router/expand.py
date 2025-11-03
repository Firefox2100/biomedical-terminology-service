from typing import List
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.base import JsonModel


expand_router = APIRouter(
    tags=['Expansion']
)


class ExpandedTermV1(JsonModel):
    """
    Data model for an expanded term in the expand terms response (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    concept_id: str = Field(
        ...,
        description='The concept ID of the expanded term.',
        alias='conceptId',
    )
    descendants: List[str] = Field(
        ...,
        description='List of descendant concept IDs.',
    )


class ExpandTermsResponseV1(JsonModel):
    """
    Data model for the response of the expand terms endpoint (v1).
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    expanded_terms: List[ExpandedTermV1] = Field(
        ...,
        description='List of expanded terms with their descendants.',
        alias='expandedTerms',
    )


@expand_router.get('/{prefix}/expand/v1', response_model=ExpandTermsResponseV1)
async def expand_terms_v1(prefix: ConceptPrefix,
                          concept_ids: List[str] = Query(..., description='List of concept IDs to expand.'),
                          depth: int | None = Query(None, description='Maximum depth for expansion.'),
                          graph_db: GraphDatabase = Depends(get_active_graph_db),
                          ):
    """
    Expand terms to their descendants up to a specified depth.
    :param prefix: The vocabulary prefix.
    :param concept_ids: List of concept IDs to expand.
    :param depth: Maximum depth for expansion.
    :param graph_db: The graph database instance.
    """
    expanded_dict = await graph_db.expand_terms(
        prefix=prefix,
        concept_ids=concept_ids,
        max_depth=depth,
    )

    expanded_terms = [
        ExpandedTermV1(
            conceptId=concept_id,
            descendants=list(descendants),
        )
        for concept_id, descendants in expanded_dict.items()
    ]

    return ExpandTermsResponseV1(
        expandedTerms=expanded_terms
    )
