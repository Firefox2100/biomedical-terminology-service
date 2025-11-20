from typing import List, Optional
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, get_active_doc_db
from bioterms.model.base import JsonModel
from bioterms.model.concept import Concept
from .utils import response_generator


auto_complete_router = APIRouter(
    tags=['Auto Complete']
)


class AutoCompleteV2Concept(JsonModel):
    """
    Concept model for auto-complete v2 endpoint.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    term_id: str = Field(
        ...,
        description='The unique identifier of the concept within its vocabulary.',
        alias='termId',
    )
    label: str = Field(
        ...,
        description='The human-readable label or name of the concept.',
    )
    definition: Optional[str] = Field(
        None,
        description='A textual definition or description of the concept.',
    )


@auto_complete_router.get('/{prefix}/auto-complete/v1/query/{query_str}', response_model=List[str])
async def auto_complete_v1(prefix: ConceptPrefix,
                           query_str: str,
                           long: bool = Query(False, description='Whether to return more results.'),
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           ):
    """
    Auto-complete search endpoint (v1). This endpoint is meant to support Cafe Variome V2 frontend searches.
    In newer applications, please use later versions of the auto-complete endpoint.
    400 responses are not actually returned here to maintain compatibility with the legacy API.
    \f
    :param prefix: The vocabulary prefix to search within.
    :param query_str: The search query string.
    :param long: Whether to return more results.
    :param doc_db: The document database instance.
    :return: A list of matching concepts.
    """
    if len(query_str) < CONFIG.auto_complete_min_length:
        # Does not actually return 400 because it is a legacy API
        return ['Search term needs at least 3 characters.']

    if long:
        limit = 250
    else:
        limit = 25

    concepts = await doc_db.auto_complete_search(
        prefix=prefix,
        query=query_str,
        limit=limit,
    )

    results = []
    for concept in concepts:
        concept_str = f'{concept.prefix}:{concept.concept_id}'

        if concept.label:
            concept_str += f' ({concept.label})'

        if concept.synonyms:
            concept_str += f' synonyms:[{", ".join(concept.synonyms)}]'

        results.append(concept_str)

    return results


@auto_complete_router.get('/{prefix}/auto-complete/v2', response_model=List[AutoCompleteV2Concept])
async def auto_complete_v2(prefix: ConceptPrefix,
                           query: str = Query(
                               ...,
                               min_length=CONFIG.auto_complete_min_length,
                               description='The search query string.'
                           ),
                           with_definition: bool = Query(
                               False,
                               description='Whether to include definition in the response.'
                           ),
                           result_threshold: int = Query(
                               0,
                               ge=0,
                               description='The maximum number of terms to return in the response. 0 for no limit.'
                           ),
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           ):
    """
    Auto-complete search endpoint (v2). This endpoint is compatible with Cafe Variome V3 frontend searches.
    \f
    :param prefix: The vocabulary prefix to search within.
    :param query: The search query string.
    :param with_definition: Whether to include definition in the response.
    :param result_threshold: The maximum number of terms to return in the response. 0 for no limit.
    :param doc_db: The document database instance.
    :return: A list of matching concepts, in V2 API format.
    """
    concepts_iter = doc_db.auto_complete_iter(
        prefix=prefix,
        query=query,
        limit=result_threshold or None,
    )

    v2_concepts = []
    async for concept in concepts_iter:
        v2_concept = AutoCompleteV2Concept(
            termId=concept.concept_id,
            label=concept.label,
            definition=concept.definition if with_definition else None,
        )
        v2_concepts.append(v2_concept)

    return v2_concepts


@auto_complete_router.get('/{prefix}/auto-complete/v3', response_model=List[Concept])
async def auto_complete_v3(prefix: ConceptPrefix,
                           query: str = Query(
                               ...,
                               min_length=CONFIG.auto_complete_min_length,
                               description='The search query string.'
                           ),
                           limit: int = Query(
                               20,
                               ge=0,
                           ),
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           ):
    """
    Auto-complete search endpoint (v3). This endpoint returns full Concept models, and uses
    streaming to handle large result sets.
    \f
    :param prefix: The vocabulary prefix to search within.
    :param query: The search query string.
    :param limit: The maximum number of results to return.
    :param doc_db: The document database instance.
    :return: A list of matching concepts.
    """
    concepts_iter = doc_db.auto_complete_iter(
        prefix=prefix,
        query=query,
        limit=limit or None,
    )

    return StreamingResponse(
        response_generator(concepts_iter),
        media_type='application/json',
    )
