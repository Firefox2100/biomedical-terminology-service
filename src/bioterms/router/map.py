from typing import List
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.base import JsonModel
from bioterms.model.related_term import RelatedTerm
from .utils import response_generator


map_router = APIRouter(
    prefix='/api/vocabularies',
    tags=['Mapping'],
)


class MapRequestV1(JsonModel):
    """
    Request model for the mapping terms endpoint (v1).
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


class MappedTermV1(JsonModel):
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
    mapped_ids: List[str] = Field(
        ...,
        description='List of child term IDs.',
        alias='mappedIds',
    )
    target_type: ConceptPrefix = Field(
        ...,
        description='The target concept prefix of the mapped terms.',
        alias='targetType',
    )


@map_router.post('/{prefix}/map/v1/{target_prefix}', response_model=List[MappedTermV1])
async def map_terms_v1(prefix: ConceptPrefix,
                       target_prefix: ConceptPrefix,
                       requested_terms: MapRequestV1,
                       result_threshold: int = Query(
                           0,
                           description='The maximum number of terms to return in the response. 0 for no limit.'
                       ),
                       graph_db: GraphDatabase = Depends(get_active_graph_db),
                       ):
    """
    Map terms from one prefix to another (V1). This API is compatible with Cafe Variome V3 backend.
    \f
    :param prefix: The prefix of the input terms.
    :param target_prefix: The prefix of the target terms.
    :param requested_terms: The requested terms to map.
    :param result_threshold: The maximum number of terms to return in the response.
    :param graph_db: The active graph database.
    :return: A list of mapped terms.
    """
    map_iter = graph_db.map_terms_iter(
        prefix=prefix,
        target_prefix=target_prefix,
        concept_ids=requested_terms.term_ids,
        limit=result_threshold or None,
    )

    v1_mapped_terms = []
    async for mapped_term in map_iter:
        v1_mapped_terms.append(
            MappedTermV1(
                termId=mapped_term.concept_id,
                mappedIds=mapped_term.related_concepts,
                targetType=target_prefix,
            )
        )

    return v1_mapped_terms


@map_router.get('/{prefix}/map/v2/{target_prefix}', response_model=List[RelatedTerm])
async def map_terms_v2(prefix: ConceptPrefix,
                       target_prefix: ConceptPrefix,
                       concept_ids: List[str] = Query(
                           ...,
                           description='List of concept IDs to expand.'
                       ),
                       max_hops: int = Query(
                           1,
                           description='Maximum number of hops between source and target terms. Set this '
                                       'to higher for going across more than one vocabulary.',
                           ge=1,
                       ),
                       limit: int | None = Query(
                           None,
                           description='Maximum number of descendants to return for each term.'
                       ),
                       graph_db: GraphDatabase = Depends(get_active_graph_db),
                       ):
    """
    Map terms from one prefix to another (V2).
    \f
    :param prefix: The prefix of the input terms.
    :param target_prefix: The prefix of the target terms.
    :param concept_ids: List of concept IDs to map.
    :param max_hops: Maximum number of hops between source and target terms. Set this to higher for going
                     across more than one vocabulary.
    :param limit: Maximum number of descendants to return for each term.
    :param graph_db: The active graph database.
    :return: A list of mapped terms.
    """
    map_iter = graph_db.map_terms_iter(
        prefix=prefix,
        target_prefix=target_prefix,
        concept_ids=concept_ids,
        max_hops=max_hops,
        limit=limit,
    )

    return StreamingResponse(
        response_generator(
            map_iter,
        ),
        media_type='application/json',
    )
