"""
Unit tests for QdrantVectorDatabase against qdrant-client's embedded in-memory engine, so the
real collection/upsert/scroll/query code paths run without a Qdrant server.
"""
import pytest
import pytest_asyncio
from qdrant_client import AsyncQdrantClient

from bioterms.database.vector_db import qdrant_vector_db
from bioterms.database.vector_db.qdrant_vector_db import QdrantVectorDatabase, _stable_uuid
from bioterms.database.vector_db.vector_db import EmbeddingItemVector
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind, QdrantStorageType


class _FixedDimensionTransformer:
    dimension = 3


@pytest_asyncio.fixture
async def vector_db(monkeypatch):
    monkeypatch.setattr(qdrant_vector_db, 'TextTransformer', _FixedDimensionTransformer)
    monkeypatch.setattr(CONFIG, 'qdrant_storage_type', QdrantStorageType.FLOAT32)
    db = QdrantVectorDatabase(AsyncQdrantClient(location=':memory:'))
    try:
        yield db
    finally:
        await db.close()


def _item(concept_id, index, kind, vector):
    return EmbeddingItemVector(
        item_id=f'{concept_id}:{kind.value}:{index}', concept_id=concept_id, kind=kind,
        text=f'{concept_id} {kind.value} {index}', vector=vector,
    )


async def _aiter(items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_load_count_resume_search_and_delete(vector_db):
    items = [
        _item('HP:1', 0, EmbeddingKind.ALIAS, [1.0, 0.0, 0.0]),
        _item('HP:1', 1, EmbeddingKind.ALIAS, [0.9, 0.1, 0.0]),
        _item('HP:1', 0, EmbeddingKind.DEFINITION, [0.0, 1.0, 0.0]),
        _item('HP:2', 0, EmbeddingKind.ALIAS, [0.0, 0.0, 1.0]),
    ]

    written = await vector_db.load_embedding_items(ConceptPrefix.HPO, _aiter(items))

    assert written == 4
    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 4
    assert await vector_db.get_embedded_concept_ids(ConceptPrefix.HPO) == {'HP:1', 'HP:2'}

    aliases = [hit async for hit in vector_db.search_items_iter(
        [1.0, 0.0, 0.0], ConceptPrefix.HPO, EmbeddingKind.ALIAS, limit=2,
    )]
    assert [(concept_id, text) for concept_id, text, _ in aliases] == [
        ('HP:1', 'HP:1 alias 0'), ('HP:1', 'HP:1 alias 1'),
    ]
    assert aliases[0][2] > aliases[1][2]

    # The kind filter keeps definition items out of alias recall and vice versa.
    definitions = [hit async for hit in vector_db.search_items_iter(
        [1.0, 0.0, 0.0], ConceptPrefix.HPO, EmbeddingKind.DEFINITION,
    )]
    assert [concept_id for concept_id, _, _ in definitions] == ['HP:1']

    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HPO)
    assert await vector_db.get_embedded_concept_ids(ConceptPrefix.HPO) == set()


@pytest.mark.asyncio
async def test_reloading_same_items_upserts_rather_than_duplicates(vector_db):
    items = [_item('HP:1', 0, EmbeddingKind.ALIAS, [1.0, 0.0, 0.0])]

    await vector_db.load_embedding_items(ConceptPrefix.HPO, _aiter(items))
    await vector_db.load_embedding_items(ConceptPrefix.HPO, _aiter(items))

    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 1


@pytest.mark.asyncio
async def test_large_loads_flush_whole_concepts_across_batches(vector_db):
    # 600 concepts x 2 items crosses the 1000-point flush threshold more than once.
    items = [
        _item(f'HP:{n}', index, EmbeddingKind.ALIAS, [1.0, float(n), float(index)])
        for n in range(600) for index in range(2)
    ]

    written = await vector_db.load_embedding_items(ConceptPrefix.HPO, _aiter(items))

    assert written == 1200
    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 1200
    assert len(await vector_db.get_embedded_concept_ids(ConceptPrefix.HPO)) == 600


@pytest.mark.asyncio
async def test_missing_collection_reads_as_empty(vector_db):
    assert await vector_db.get_embedded_concept_ids(ConceptPrefix.MONDO) == set()
    await vector_db.delete_vectors_for_prefix(ConceptPrefix.MONDO)  # no-op, must not raise


def test_client_must_be_configured():
    with pytest.raises(ValueError, match='Qdrant client is not set'):
        _ = QdrantVectorDatabase().client


def test_stable_uuid_is_deterministic_per_item():
    first, repeated = (_stable_uuid('HP:1:alias:0') for _ in range(2))

    assert first == repeated
    assert first != _stable_uuid('HP:1:alias:1')
