import os

import pytest

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')

from bioterms.database.vector_db.mongo_vector_db import MongoVectorDatabase
from bioterms.database.vector_db.vector_db import EmbeddingItemVector
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind


class FakeCursor:
    def __init__(self, items):
        self._items = list(items)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for item in self._items:
            yield item


class FakeCollection:
    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.search_indexes: dict[str, dict] = {}
        self.create_search_index_calls = 0
        self.dropped = False

    async def list_search_indexes(self, name=None):
        matches = [
            {'name': index_name, **definition}
            for index_name, definition in self.search_indexes.items()
            if name is None or index_name == name
        ]
        return FakeCursor(matches)

    async def create_search_index(self, model):
        self.create_search_index_calls += 1
        document = model.document
        self.search_indexes[document['name']] = document
        return document['name']

    async def bulk_write(self, operations):
        for op in operations:
            item_id = op._filter['_id']
            update = op._doc['$set']
            if item_id not in self.docs and not op._upsert:
                continue
            self.docs.setdefault(item_id, {'_id': item_id})
            self.docs[item_id].update(update)

    async def count_documents(self, filt):
        return len(self.docs)

    async def aggregate(self, pipeline):
        # Ignore the actual vector math for this fake; just return every item matching the
        # $vectorSearch filter's "kind", honouring "limit".
        vector_search_stage = pipeline[0]['$vectorSearch']
        limit = vector_search_stage['limit']
        kind = vector_search_stage['filter']['kind']
        matches = [
            {'conceptId': d['conceptId'], 'text': d['text'], 'score': 1.0}
            for d in self.docs.values()
            if d.get('kind') == kind
        ][:limit]
        return FakeCursor(matches)

    async def drop(self):
        self.dropped = True
        self.docs = {}
        self.search_indexes = {}


class FakeDatabase:
    def __init__(self):
        self.collections: dict[str, FakeCollection] = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


class FakeClient:
    def __init__(self):
        self._db = FakeDatabase()

    def __getitem__(self, _name):
        return self._db


class FakeTextTransformer:
    def __init__(self, *args, **kwargs):
        pass

    @property
    def dimension(self):
        return 3

    def embed_strings(self, texts):
        return [[0.1, 0.2, 0.3] for _ in texts]


def make_vector_db(monkeypatch) -> MongoVectorDatabase:
    monkeypatch.setattr(
        'bioterms.database.vector_db.mongo_vector_db.TextTransformer',
        FakeTextTransformer,
    )
    return MongoVectorDatabase(client=FakeClient())


async def _items_iter(items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_load_embedding_items_upserts_items_and_creates_index_once(monkeypatch):
    vector_db = make_vector_db(monkeypatch)

    written = await vector_db.load_embedding_items(
        prefix=ConceptPrefix.HPO,
        items=_items_iter([
            EmbeddingItemVector('HP:0000001:alias:0', 'HP:0000001', EmbeddingKind.ALIAS, 'foo', [1.0, 2.0, 3.0]),
            EmbeddingItemVector('HP:0000002:alias:0', 'HP:0000002', EmbeddingKind.ALIAS, 'bar', [4.0, 5.0, 6.0]),
        ]),
    )

    assert written == 2

    collection = vector_db.db['hpo.vectors']
    assert collection.docs['HP:0000001:alias:0']['vector'] == [1.0, 2.0, 3.0]
    assert collection.docs['HP:0000002:alias:0']['conceptId'] == 'HP:0000002'
    assert 'vector_index' in collection.search_indexes
    assert collection.create_search_index_calls == 1


@pytest.mark.asyncio
async def test_count_vectors_counts_items_not_concepts(monkeypatch):
    vector_db = make_vector_db(monkeypatch)
    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.MONDO,
        items=_items_iter([
            EmbeddingItemVector('MONDO:1:alias:0', 'MONDO:1', EmbeddingKind.ALIAS, 'a', [1.0, 0.0, 0.0]),
            EmbeddingItemVector('MONDO:1:alias:1', 'MONDO:1', EmbeddingKind.ALIAS, 'b', [0.0, 1.0, 0.0]),
            EmbeddingItemVector('MONDO:1:definition:0', 'MONDO:1', EmbeddingKind.DEFINITION, 'd', [0.0, 0.0, 1.0]),
        ]),
    )

    # Three items for a single concept: count_vectors reports items, not distinct concepts.
    assert await vector_db.count_vectors(ConceptPrefix.MONDO) == 3


@pytest.mark.asyncio
async def test_delete_vectors_for_prefix_drops_collection(monkeypatch):
    vector_db = make_vector_db(monkeypatch)
    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.HGNC,
        items=_items_iter([
            EmbeddingItemVector('HGNC:1:alias:0', 'HGNC:1', EmbeddingKind.ALIAS, 'a', [1.0, 2.0, 3.0]),
        ]),
    )

    collection = vector_db.db['hgnc.vectors']
    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HGNC)

    assert collection.dropped is True
    assert await vector_db.count_vectors(ConceptPrefix.HGNC) == 0


@pytest.mark.asyncio
async def test_search_items_iter_filters_by_kind(monkeypatch):
    vector_db = make_vector_db(monkeypatch)
    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.SNOMED,
        items=_items_iter([
            EmbeddingItemVector('123:alias:0', '123', EmbeddingKind.ALIAS, 'diabetes', [1.0, 2.0, 3.0]),
            EmbeddingItemVector('456:definition:0', '456', EmbeddingKind.DEFINITION, 'a disease', [4.0, 5.0, 6.0]),
        ]),
    )

    alias_hits = await vector_db.search_items(
        query_vector=[0.1, 0.2, 0.3],
        prefix=ConceptPrefix.SNOMED,
        kind=EmbeddingKind.ALIAS,
        limit=10,
    )
    assert [concept_id for concept_id, _text, _score in alias_hits] == ['123']

    definition_hits = await vector_db.search_items(
        query_vector=[0.1, 0.2, 0.3],
        prefix=ConceptPrefix.SNOMED,
        kind=EmbeddingKind.DEFINITION,
        limit=10,
    )
    assert [concept_id for concept_id, _text, _score in definition_hits] == ['456']


@pytest.mark.asyncio
async def test_close_closes_underlying_client():
    closed = {'value': False}

    class FakeClientWithClose(FakeClient):
        async def close(self):
            closed['value'] = True

    vector_db = MongoVectorDatabase(client=FakeClientWithClose())
    await vector_db.close()

    assert closed['value'] is True
