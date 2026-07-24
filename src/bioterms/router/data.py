"""
API router for data management endpoints. These endpoints are for manipulating the data
directly, such as ingesting new documents, deleting vocabularies, and retrieving
vocabulary status information.
"""

import zlib
from typing import Annotated, AsyncIterator
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Query, Depends, Request, HTTPException
from fastapi.responses import StreamingResponse, Response

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, VectorDatabase, \
    get_active_cache, get_active_doc_db, get_active_graph_db, get_active_vector_db
from bioterms.model.base import JsonModel
from bioterms.model.concept import ConceptUnion
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.vocabulary import get_vocabulary_config, delete_vocabulary, get_vocabulary_license, \
    get_vocabulary_status
from .utils import response_generator, api_key_required


data_router = APIRouter(
    prefix='/api/vocabularies',
    tags=['Data Management'],
)


class IngestResponse(JsonModel):
    """
    Response model for the ingest documents endpoint.
    """

    model_config = ConfigDict(
        serialize_by_alias=True,
        extra='forbid',
    )

    concept_count: int = Field(
        ...,
        description='The total number of concepts in the vocabulary after ingestion.',
        alias='conceptCount',
    )


class ConceptInfoResponse(JsonModel):
    """
    Response model for the get concept endpoint.
    """

    model_config = ConfigDict(
        serialize_by_alias=True,
        extra='forbid',
    )

    concept: ConceptUnion = Field(
        ...,
        description='The retrieved concept.',
    )
    children: list[ConceptUnion] = Field(
        ...,
        description='List of child concepts.',
    )
    parents: list[ConceptUnion] = Field(
        ...,
        description='List of parent concepts.',
    )


@data_router.get('/{prefix}', response_model=VocabularyStatus)
async def get_vocabulary_status_info(prefix: ConceptPrefix,
                                     cache: Annotated[Cache, Depends(get_active_cache)],
                                     doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                                     graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                                     ):
    """
    Get status about the specified vocabulary.
    \f
    :param prefix: The vocabulary prefix.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :return: The vocabulary status information.
    """
    vocab_status = await get_vocabulary_status(
        prefix=prefix,
        cache=cache,
        doc_db=doc_db,
        graph_db=graph_db,
    )

    return vocab_status


@data_router.delete('/{prefix}')
async def delete_vocabulary_data(prefix: ConceptPrefix,
                                 cache: Annotated[Cache, Depends(get_active_cache)],
                                 doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                                 graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                                 vector_db: Annotated[VectorDatabase, Depends(get_active_vector_db)],
                                 _: Annotated[str, Depends(api_key_required)],
                                 ):
    """
    Delete all documents/records for the specified vocabulary from the document database.
    \f
    :param prefix: The vocabulary prefix.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param vector_db: The vector database instance.
    :param _: API key authentication dependency.
    """
    await delete_vocabulary(
        prefix=prefix,
        cache=cache,
        doc_db=doc_db,
        graph_db=graph_db,
        vector_db=vector_db,
    )


@data_router.get(
    '/{prefix}/license',
    response_class=Response,
    responses={404: {'description': 'No license information found for the vocabulary.'}},
)
async def get_license(prefix: ConceptPrefix):
    """
    Get the licence information for the specified vocabulary.
    \f
    :param prefix: The vocabulary prefix.
    :return: The licence information as a Markdown response.
    """
    license_str = get_vocabulary_license(prefix)
    if license_str is None:
        raise HTTPException(
            status_code=404,
            detail=f'No license information found for vocabulary {prefix.value}'
        )

    return Response(
        content=license_str,
        media_type='text/markdown',
    )


@data_router.get('/{prefix}/random', response_model=list[str])
async def get_random_concept_ids(prefix: ConceptPrefix,
                                 doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                                 count: Annotated[
                                     int,
                                     Query(
                                         description='Number of random concepts to return.',
                                         ge=1,
                                         le=100,
                                     )
                                 ] = 10,
                                 ):
    """
    Get a list of random concept IDs from the specified vocabulary.
    \f
    :param prefix: The vocabulary prefix.
    :param count: The number of random concept IDs to return.
    :param doc_db: The document database instance.
    :return: A list of random concept IDs.
    """
    random_ids = await doc_db.get_random_term_ids(
        prefix=prefix,
        count=count,
    )

    return random_ids


@data_router.get('/{prefix}/documents')
async def get_documents(prefix: ConceptPrefix,
                        doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                        ):
    """
    Download all documents from the specified vocabulary database as a JSON list.
    \f
    :param prefix: The vocabulary prefix.
    :param doc_db: The document database instance.
    :return: A stream response containing all documents in JSON format.
    """
    config = get_vocabulary_config(prefix)
    concepts_iter = doc_db.get_terms_iter(
        prefix=prefix,
        model_class=config['conceptClass'],
    )

    file_name = f'{prefix.value}_documents.json'
    headers = {
        'Content-Disposition': f'attachment; filename="{file_name}"'
    }

    return StreamingResponse(
        response_generator(concepts_iter),
        media_type='application/json', headers=headers,
    )


async def _iter_request_lines(request: Request,
                              is_gz: bool,
                              ) -> AsyncIterator[bytes]:
    """
    Decode a streamed request body (optionally gzip-compressed) into newline-delimited lines.
    :param request: The FastAPI request object to stream from.
    :param is_gz: Whether the request body is gzip-compressed.
    :return: An async iterator yielding non-empty line bytes.
    """
    decomp = zlib.decompressobj(16 + zlib.MAX_WBITS) if is_gz else None
    buffer: bytes = b''

    async for chunk in request.stream():
        if is_gz:
            chunk = decomp.decompress(chunk)

        buffer += chunk

        while True:
            nl = buffer.find(b'\n')
            if nl < 0:
                # No complete line yet
                break

            line, buffer = buffer[:nl], buffer[nl + 1:]
            if line.strip():
                yield line

    if is_gz:
        tail = decomp.flush()
        if tail:
            buffer += tail

    if buffer.strip():
        yield buffer


@data_router.post(
    '/{prefix}/documents',
    response_model=IngestResponse,
    responses={400: {'description': 'Failed to ingest one or more of the uploaded documents.'}},
)
async def ingest_documents(prefix: ConceptPrefix,
                           request: Request,
                           doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                           _: Annotated[str, Depends(api_key_required)],
                           ):
    """
    Ingest documents into the specified vocabulary database.

    This endpoint is used to upload the vocabulary directly into the database, without the need
    to download and parse the original upstream release files, or in case of modified vocabulary
    files. The server expects a stream upload with one document per line in JSON format.
    \f
    :param prefix: The vocabulary prefix.
    :param request: The FastAPI request object.
    :param doc_db: The document database instance.
    :param _: API key authentication dependency.
    :return: An IngestResponse containing the total number of concepts after ingestion.
    """
    is_gz = request.headers.get('Content-Encoding', '') == 'gzip'

    batch: list[ConceptUnion] = []
    batch_size = 1000
    vocabulary_config = get_vocabulary_config(prefix)
    concept_class: ConceptUnion = vocabulary_config['conceptClass']

    async def flush_batch():
        if not batch:
            return

        await doc_db.save_terms(batch)
        batch.clear()

    try:
        async for line in _iter_request_lines(request, is_gz):
            obj = concept_class.model_validate_json(line)
            batch.append(obj)

            if len(batch) >= batch_size:
                await flush_batch()

        await flush_batch()
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f'Failed to ingest documents: {e}'
        ) from e

    concept_count = await doc_db.count_terms(prefix)

    return IngestResponse(
        conceptCount=concept_count,
    )


@data_router.get(
    '/{prefix}/{concept_id}',
    response_model=ConceptInfoResponse,
    responses={404: {'description': 'Concept not found in the specified vocabulary.'}},
)
async def get_concept(prefix: ConceptPrefix,
                      concept_id: str,
                      doc_db: Annotated[DocumentDatabase, Depends(get_active_doc_db)],
                      graph_db: Annotated[GraphDatabase, Depends(get_active_graph_db)],
                      ):
    """
    Get a specific concept by its ID from the specified vocabulary.
    \f
    :param prefix: The vocabulary prefix.
    :param concept_id: The ID of the concept to retrieve.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :return: The Concept instance.
    """
    config = get_vocabulary_config(prefix)
    concept = await doc_db.get_terms_by_ids(
        prefix=prefix,
        concept_ids=[concept_id],
        model_class=config['conceptClass'],
    )

    if not concept:
        raise HTTPException(
            status_code=404,
            detail=f'Concept {concept_id} not found in vocabulary {prefix.value}'
        )

    children = await graph_db.expand_terms(
        prefix=prefix,
        concept_ids=[concept_id],
        max_depth=1,
    )
    parents = await graph_db.trace_ancestors(
        prefix=prefix,
        concept_ids=[concept_id],
        max_depth=1,
    )

    children_concepts = await doc_db.get_terms_by_ids(
        prefix=prefix,
        concept_ids=children[0].related_concepts if children else [],
        model_class=config['conceptClass'],
    )
    parents_concepts = await doc_db.get_terms_by_ids(
        prefix=prefix,
        concept_ids=parents[0].related_concepts if parents else [],
        model_class=config['conceptClass'],
    )

    return ConceptInfoResponse(
        concept=concept[0],
        children=children_concepts,
        parents=parents_concepts,
    )
