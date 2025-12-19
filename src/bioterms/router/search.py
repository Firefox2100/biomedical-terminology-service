from typing import List, Optional, Union
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.database import DocumentDatabase, VectorDatabase, get_active_doc_db, get_active_vector_db
from bioterms.vocabulary import get_vocabulary_config
from bioterms.model.concept import Concept
from .utils import response_generator


search_router = APIRouter(
    prefix='/api/vocabularies',
    tags=['Search'],
)


@search_router.get('/{prefix}/search/v1', response_model=List[Concept])
async def search_terms_v1(prefix: ConceptPrefix,
                          query: str = Query(..., description='The search query string'),
                          limit: Optional[int] = Query(
                              10,
                              description='Maximum number of concepts to return.',
                              ge=1,
                          ),
                          doc_db: DocumentDatabase = Depends(get_active_doc_db),
                          vector_db: VectorDatabase = Depends(get_active_vector_db),
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
    config = get_vocabulary_config(prefix)

    concept_ids = await vector_db.search_concepts(
        query=query,
        prefix=prefix,
        limit=limit or 10,
    )

    concepts_iter = doc_db.get_terms_by_ids_iter(
        prefix=prefix,
        concept_ids=concept_ids,
        model_class=config['conceptClass']
    )

    return StreamingResponse(
        response_generator(concepts_iter),
        media_type='application/json'
    )
