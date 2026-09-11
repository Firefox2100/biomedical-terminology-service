"""
API router for searching terms within vocabularies. This is a more advanced search
endpoint that utilizes embedding-based search to find relevant terms based on the input
query.
"""

from typing import Annotated, List, Optional
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.metrics import SEARCH_ITEMS, SEARCH_LIMIT, SEARCH_QUERY_LEN
from bioterms.database import DocumentDatabase, VectorDatabase, get_active_doc_db, \
    get_active_vector_db
from bioterms.vocabulary import get_vocabulary_config
from bioterms.model.concept import Concept
from .utils import response_generator


search_router = APIRouter(
    prefix='/api/vocabularies',
    tags=['Search'],
)


@search_router.get('/{prefix}/search/v1', response_model=List[Concept])
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

    This method uses embedding-based search to find relevant terms. The input query is
    converted into an embedding vector, and the vector database is queried to find terms
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

    config = get_vocabulary_config(prefix)

    concept_ids = await vector_db.search_concepts(
        query=query,
        prefix=prefix,
        limit=limit or 10,
    )

    SEARCH_ITEMS.labels(prefix=prefix.value).observe(len(concept_ids))

    concepts_iter = doc_db.get_terms_by_ids_iter(
        prefix=prefix,
        concept_ids=concept_ids,
        model_class=config['conceptClass']
    )

    return StreamingResponse(
        response_generator(concepts_iter),
        media_type='application/json'
    )
