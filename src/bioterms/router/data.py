import zlib
from typing import AsyncIterator
from pydantic import Field, ConfigDict
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import StreamingResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.base import JsonModel
from bioterms.model.concept import Concept
from bioterms.vocabulary import get_vocabulary_config, delete_vocabulary


data_router = APIRouter(
    tags=['Data Manipulation']
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


@data_router.delete('/{prefix}/data')
async def delete_vocabulary_data(prefix: ConceptPrefix,
                                 doc_db: DocumentDatabase = Depends(get_active_doc_db),
                                 graph_db: GraphDatabase = Depends(get_active_graph_db),
                                 ):
    """
    Delete all documents/records for the specified vocabulary from the document database.
    :param prefix: The vocabulary prefix.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    """
    await delete_vocabulary(
        prefix=prefix,
        doc_db=doc_db,
        graph_db=graph_db,
    )


@data_router.get('/{prefix}/data/documents')
async def get_documents(prefix: ConceptPrefix,
                             doc_db: DocumentDatabase = Depends(get_active_doc_db),
                             ):
    """
    Get all documents from the specified vocabulary database as a JSON list.
    :param prefix: The vocabulary prefix.
    :param doc_db: The document database instance.
    :return: A stream response containing all documents in JSON format.
    """
    concepts_iter = doc_db.get_item_iter(prefix)

    file_name = f'{prefix.value}_documents.json'

    async def gen() -> AsyncIterator[bytes]:
        yield b'['
        first = True

        async for concept in concepts_iter:
            if not first:
                yield b',\n'
            else:
                first = False

            yield concept.model_dump_json().encode('utf-8')

        yield b']'

    headers = {
        'Content-Disposition': f'attachment; filename="{file_name}"'
    }

    return StreamingResponse(gen(), media_type='application/json', headers=headers)


@data_router.post('/{prefix}/data/documents', response_model=IngestResponse)
async def ingest_documents(prefix: ConceptPrefix,
                           request: Request,
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           ):
    """
    Ingest documents into the specified vocabulary database.

    This endpoint is used to upload the vocabulary directly into the database, without the need
    to download and parse the original upstream release files, or in case of modified vocabulary files.
    The server expects a stream upload with one document per line in JSON format.
    :param prefix: The vocabulary prefix.
    :param request: The FastAPI request object.
    :param doc_db: The document database instance.
    """
    is_gz = request.headers.get('Content-Encoding', '') == 'gzip'
    decomp = zlib.decompressobj(16 + zlib.MAX_WBITS) if is_gz else None

    buffer: bytes = b''
    batch: list[Concept] = []
    batch_size = 1000
    vocabulary_config = get_vocabulary_config(prefix)
    concept_class: type[Concept] = vocabulary_config['conceptClass']

    async def flush_batch():
        nonlocal buffer
        if not batch:
            return

        await doc_db.save_terms(batch)
        batch.clear()

    try:
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
                if not line.strip():
                    # Empty line
                    continue

                obj = concept_class.model_validate_json(line)
                batch.append(obj)

                if len(batch) >= batch_size:
                    await flush_batch()

        # Flush remaining buffer
        if is_gz:
            tail = decomp.flush()

            if tail:
                buffer += tail

        if buffer.strip():
            obj = concept_class.model_validate_json(buffer)
            batch.append(obj)

        await flush_batch()
    except Exception as err:
        raise HTTPException(
            status_code=400,
            detail=f'Failed to ingest documents: {err}'
        )

    concept_count = await doc_db.count_terms(prefix)

    return IngestResponse(
        conceptCount=concept_count,
    )
