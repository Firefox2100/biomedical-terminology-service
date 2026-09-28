from types import SimpleNamespace

import pytest

from bioterms.database.doc_db.elasticsearch_doc_db import ElasticsearchDocumentDatabase
from bioterms.database.vector_db.elasticsearch_vector_db import ElasticsearchVectorDatabase
from bioterms.database.vector_db.vector_db import EmbeddingItemVector
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.model.concept import Concept


class FakeIndices:
    def __init__(self):
        self.names = set()
        self.created = []
        self.deleted = []

    async def exists(self, index):
        return index in self.names

    async def create(self, index, **kwargs):
        self.names.add(index)
        self.created.append((index, kwargs))

    async def delete(self, index):
        self.names.discard(index)
        self.deleted.append(index)

    async def put_mapping(self, **kwargs):
        return None


class FakeClient:
    def __init__(self):
        self.indices = FakeIndices()
        self.search_calls = []
        self.count_value = 0
        self.search_response = {'hits': {'hits': []}}
        self.closed = False

    async def search(self, **kwargs):
        self.search_calls.append(kwargs)
        return self.search_response

    async def count(self, **kwargs):
        return {'count': self.count_value}

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_document_save_uses_concept_id_as_bulk_document_id(monkeypatch):
    client = FakeClient()
    database = ElasticsearchDocumentDatabase(client)
    captured = []

    async def fake_bulk(_client, actions, **kwargs):
        captured.extend([action async for action in actions])

    monkeypatch.setattr(
        'bioterms.database.doc_db.elasticsearch_doc_db.async_bulk', fake_bulk,
    )
    concept = Concept(prefix=ConceptPrefix.HPO, conceptId='HP:1', label='Heart disease')

    await database.save_terms([concept])

    assert captured[0]['_id'] == 'HP:1'
    assert captured[0]['_op_type'] == 'index'
    mapping = client.indices.created[0][1]['mappings']['properties']
    assert mapping['label']['analyzer'] == 'bts_ngram'
    assert mapping['label']['fields']['fuzzy']['analyzer'] == 'standard'
    assert mapping['conceptId']['fields']['fuzzy']['analyzer'] == 'standard'


@pytest.mark.asyncio
async def test_document_missing_index_reads_as_empty():
    database = ElasticsearchDocumentDatabase(FakeClient())

    assert await database.count_terms(ConceptPrefix.HPO) == 0
    assert await database.get_terms(ConceptPrefix.HPO) == []


@pytest.mark.asyncio
async def test_document_lexical_search_rejects_query_shorter_than_three_characters():
    client = FakeClient()
    database = ElasticsearchDocumentDatabase(client)

    assert await database.lexical_search(ConceptPrefix.HPO, 'ab') == []
    assert client.search_calls == []


@pytest.mark.asyncio
async def test_document_fuzzy_search_uses_bounded_edit_distance(monkeypatch):
    client = FakeClient()
    client.search_response = {'hits': {'hits': [{'_id': 'HP:1', '_score': 7.5}]}}
    database = ElasticsearchDocumentDatabase(client)
    client.indices.names.add(database._index_name(ConceptPrefix.HPO))

    results = await database.fuzzy_search(ConceptPrefix.HPO, 'congenital diabtes', limit=12)

    assert results == [('HP:1', 7.5)]
    call = client.search_calls[-1]
    assert call['size'] == 12
    fuzzy = call['query']['dis_max']['queries'][0]['multi_match']
    assert fuzzy['fuzziness'] == 'AUTO:4,7'
    assert fuzzy['prefix_length'] == 1
    assert fuzzy['query'] == 'congenital diabtes'


@pytest.mark.asyncio
async def test_document_fuzzy_search_skips_short_symbols():
    client = FakeClient()
    database = ElasticsearchDocumentDatabase(client)

    assert await database.fuzzy_search(ConceptPrefix.HPO, 'B2') == []
    assert client.search_calls == []


@pytest.mark.asyncio
async def test_vector_index_and_filtered_knn_query(monkeypatch):
    client = FakeClient()
    client.search_response = {'hits': {'hits': [{
        '_score': 0.9, '_source': {'conceptId': 'HP:1', 'text': 'Heart disease'},
    }]}}
    database = ElasticsearchVectorDatabase(client)
    monkeypatch.setattr(
        'bioterms.database.vector_db.elasticsearch_vector_db.TextTransformer',
        lambda: SimpleNamespace(dimension=3),
    )
    captured = []

    async def fake_bulk(_client, actions, **kwargs):
        captured.extend([action async for action in actions])

    monkeypatch.setattr(
        'bioterms.database.vector_db.elasticsearch_vector_db.async_bulk', fake_bulk,
    )

    async def items():
        yield EmbeddingItemVector('item-1', 'HP:1', EmbeddingKind.ALIAS, 'Heart disease', [1, 0, 0])

    assert await database.load_embedding_items(ConceptPrefix.HPO, items()) == 1
    results = await database.search_items([1, 0, 0], ConceptPrefix.HPO, EmbeddingKind.ALIAS, limit=5)

    assert captured[0]['_id'] == 'item-1'
    vector_mapping = client.indices.created[0][1]['mappings']['properties']['vector']
    assert vector_mapping == {
        'type': 'dense_vector', 'dims': 3, 'index': True, 'similarity': 'cosine',
    }
    assert client.search_calls[-1]['knn']['filter'] == {'term': {'kind': 'alias'}}
    assert results == [('HP:1', 'Heart disease', 0.9)]
