from typing import List
from fastapi import APIRouter, Query, Depends

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, get_active_doc_db
from bioterms.model.concept import Concept


auto_complete_router = APIRouter(
    tags=['Auto Complete']
)


@auto_complete_router.get('/{prefix}/auto-complete/v1/query/{query_str}', response_model=List[str])
async def auto_complete_v1(prefix: ConceptPrefix,
                           query_str: str,
                           long: bool = Query(False, description='Whether to return more results.'),
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           ):
    """
    Auto-complete search endpoint (v1).
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


@auto_complete_router.get('/{prefix}/auto-complete/v2', response_model=List[Concept])
async def auto_complete_v2(prefix: ConceptPrefix,
                           query: str = Query(
                               ...,
                               min_length=CONFIG.auto_complete_min_length,
                               description='The search query string.'
                           ),
                           result_threshold: int = Query(
                               20,
                               ge=0,
                           ),
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           ):
    """
    Auto-complete search endpoint (v2).
    :param prefix: The vocabulary prefix to search within.
    :param query: The search query string.
    :param result_threshold: The maximum number of results to return.
    :param doc_db: The document database instance.
    :return: A list of matching concepts.
    """
    concepts = await doc_db.auto_complete_search(
        prefix=prefix,
        query=query,
        limit=result_threshold or None,
    )

    return concepts
