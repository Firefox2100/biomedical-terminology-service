"""
Router for mapping concepts between different vocabularies.
"""

from typing import List
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, AnnotationType
from bioterms.etc.metrics import MAP_REQS, MAP_ROOTS, MAP_HOPS, MAP_LIMIT
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.base import JsonModel
from bioterms.model.concept_path import ConceptPath
from .utils import response_generator


trace_router = APIRouter(
    prefix='/api/vocabularies',
    tags=['Trace'],
)


@trace_router.get('/{prefix}/trace/v1/{target_prefix}', response_model=List[ConceptPath])
async def trace_terms_v1(prefix: ConceptPrefix,
                         target_prefix: ConceptPrefix,
                         start_id: str = Query(
                             ...,
                             description='The ID of the starting concept.',
                         ),
                         end_id: str = Query(
                             ...,
                             description='The ID of the ending concept.',
                         ),
                         relationship: ConceptRelationshipType | AnnotationType = Query(
                             ...,
                             description='The type of relationship to trace.',
                         ),
                         forward: bool | None = Query(
                             True,
                             description='If True, the direction of path must be from start to end; if '
                                         'False, it shall be from end to start. If None, direction is '
                                         'ignored, but only the shortest path is returned.'
                         ),
                         max_hops: int = Query(
                             12,
                             description='The maximum number of hops to trace from start to end.'
                         ),
                         graph_db: GraphDatabase = Depends(get_active_graph_db),
                         ):
    """
    Trace paths between two concepts in the graph database.

    It returns all available paths without repeating sequence. If a path is a subset of another
    path with order preserved, only the shorter path is returned.
    \f
    :param prefix: The prefix of the starting concept ID.
    :param target_prefix: The prefix of the ending concept ID.
    :param start_id: The ID of the starting concept.
    :param end_id: The ID of the ending concept.
    :param relationship: The type of relationship to trace.
    :param forward: If True, the direction of path must be from start to end; if False, it shall be from
        end to start. If None, direction is ignored, but only the shortest path is returned.
    :param max_hops: The maximum number of hops to trace from start to end.
    :param graph_db: The graph database instance to use.
    :return: A list of concept paths between the two concepts.
    """
    trace_iter = graph_db.trace_term_iter(
        prefix_start=prefix,
        prefix_end=target_prefix,
        id_start=start_id,
        id_end=end_id,
        relationship_type=relationship,
        forward=forward,
        max_depth=max_hops,
    )

    return StreamingResponse(
        response_generator(
            trace_iter,
        ),
        media_type='application/json',
    )
