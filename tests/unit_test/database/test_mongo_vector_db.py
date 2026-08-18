import os

import pytest

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')

from bioterms.database.vector_db.mongo_vector_db import MongoVectorDatabase
from bioterms.etc.enums import ConceptPrefix


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
            concept_id = op._filter['conceptId']
            update = op._doc['$set']
            if concept_id not in self.docs and not op._upsert:
                continue
            self.docs.setdefault(concept_id, {'conceptId': concept_id})
            self.docs[concept_id].update(update)

    async def count_documents(self, filt):
        return sum(1 for d in self.docs.values() if 'vector' in d)

    def find(self, filt, projection=None):
        matches = [
            {'conceptId': d['conceptId'], 'vector': d['vector']}
            for d in self.docs.values()
            if 'vector' in d
        ]
        return FakeCursor(matches)

    async def aggregate(self, pipeline):
        # Ignore the actual vector math for this fake; just return every concept
        # that currently has a vector, honouring $vectorSearch's "limit".
        vector_search_stage = pipeline[0]['$vectorSearch']
        limit = vector_search_stage['limit']
        matches = [
            {'conceptId': d['conceptId']}
            for d in self.docs.values()
            if 'vector' in d
        ][:limit]
        return FakeCursor(matches)

    async def update_many(self, filt, update):
        unset_fields = update.get('$unset', {})
        for doc in self.docs.values():
            for field in unset_fields:
                doc.pop(field, None)


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

    def embed_strings(self, texts):
        return [[0.1, 0.2, 0.3] for _ in texts]


def make_vector_db() -> MongoVectorDatabase:
    return MongoVectorDatabase(client=FakeClient(), embedding_dimension=3)


async def _embeddings_iter(items):
    for concept_id, vector_id, vector in items:
        yield concept_id, vector_id, vector


@pytest.mark.asyncio
async def test_load_embeddings_upserts_vector_and_creates_index_once():
    vector_db = make_vector_db()

    id_map = await vector_db.load_embeddings(
        prefix=ConceptPrefix.HPO,
        embeddings=_embeddings_iter([
            ('HP:0000001', 'v1', [1.0, 2.0, 3.0]),
            ('HP:0000002', 'v2', [4.0, 5.0, 6.0]),
        ]),
    )

    assert id_map == {'HP:0000001': 'v1', 'HP:0000002': 'v2'}

    collection = vector_db.db['hpo']
    assert collection.docs['HP:0000001']['vector'] == [1.0, 2.0, 3.0]
    assert collection.docs['HP:0000002']['vectorId'] == 'v2'
    assert 'vector_index' in collection.search_indexes
    assert collection.create_search_index_calls == 1


@pytest.mark.asyncio
async def test_count_and_get_vectors_for_prefix():
    vector_db = make_vector_db()
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.MONDO,
        embeddings=_embeddings_iter([
            ('MONDO:1', 'v1', [1.0, 0.0, 0.0]),
        ]),
    )

    count = await vector_db.count_vectors(ConceptPrefix.MONDO)
    assert count == 1

    vectors = await vector_db.get_vectors_for_prefix(ConceptPrefix.MONDO)
    assert vectors == {'MONDO:1': [1.0, 0.0, 0.0]}


@pytest.mark.asyncio
async def test_delete_vectors_for_prefix_unsets_vector_fields():
    vector_db = make_vector_db()
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.HGNC,
        embeddings=_embeddings_iter([
            ('HGNC:1', 'v1', [1.0, 2.0, 3.0]),
        ]),
    )

    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HGNC)

    collection = vector_db.db['hgnc']
    assert 'vector' not in collection.docs['HGNC:1']
    assert 'vectorId' not in collection.docs['HGNC:1']
    assert await vector_db.count_vectors(ConceptPrefix.HGNC) == 0


@pytest.mark.asyncio
async def test_search_concepts_iter_uses_vector_search_pipeline(monkeypatch):
    monkeypatch.setattr(
        'bioterms.database.vector_db.mongo_vector_db.TextTransformer',
        FakeTextTransformer,
    )

    vector_db = make_vector_db()
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.SNOMED,
        embeddings=_embeddings_iter([
            ('123', 'v1', [1.0, 2.0, 3.0]),
            ('456', 'v2', [4.0, 5.0, 6.0]),
        ]),
    )

    concept_ids = await vector_db.search_concepts(
        query='diabetes',
        prefix=ConceptPrefix.SNOMED,
        limit=1,
    )

    assert concept_ids == ['123']


@pytest.mark.asyncio
async def test_close_closes_underlying_client():
    closed = {'value': False}

    class FakeClientWithClose(FakeClient):
        async def close(self):
            closed['value'] = True

    vector_db = MongoVectorDatabase(client=FakeClientWithClose())
    await vector_db.close()

    assert closed['value'] is True
