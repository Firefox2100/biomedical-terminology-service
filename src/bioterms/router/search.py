"""
API router for searching terms within vocabularies. This is a more advanced search
endpoint that fuses lexical and embedding-based recall (see `bioterms.search.hybrid`) to
find relevant terms based on the input query.
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
from bioterms.search import hybrid_search
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

    config = get_vocabulary_config(prefix)

    concepts_iter = hybrid_search(
        query=query,
        prefix=prefix,
        doc_db=doc_db,
        vector_db=vector_db,
        model_class=config['conceptClass'],
        limit=limit or 10,
    )

    async def counting_generator():
        items = 0
        async for concept in concepts_iter:
            items += 1
            yield concept
        SEARCH_ITEMS.labels(prefix=prefix.value).observe(items)

    return StreamingResponse(
        response_generator(counting_generator()),
        media_type='application/json'
    )
