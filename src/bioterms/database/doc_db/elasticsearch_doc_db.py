"""Elasticsearch implementation of the document database interface."""

from typing import AsyncIterator
from uuid import UUID

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from bioterms.etc.consts import CONFIG, LOGGER
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.model.concept import Concept, ConceptUnion
from bioterms.model.user import User, UserApiKey, UserRepository
from .doc_db import DocumentDatabase, SearchQuery, normalise_search_query


def _index_part(value: str) -> str:
    """Return a lowercase Elasticsearch-safe index-name component."""
    return ''.join(c if c.isalnum() or c in '-_' else '-' for c in value.lower())


class ElasticsearchUserRepository(UserRepository):
    def __init__(self, client: AsyncElasticsearch, index_name: str):
        self.client = client
        self.index_name = index_name

    async def _ensure_index(self) -> None:
        if not await self.client.indices.exists(index=self.index_name):
            LOGGER.info('Creating Elasticsearch user index: %s', self.index_name)
            await self.client.indices.create(index=self.index_name, mappings={
                'properties': {
                    'username': {'type': 'keyword'},
                    'password': {'type': 'keyword', 'index': False},
                    'apiKeys': {
                        'type': 'nested',
                        'properties': {
                            'keyId': {'type': 'keyword'},
                            'keyHash': {'type': 'keyword'},
                            'name': {'type': 'keyword'},
                            'createdAt': {'type': 'date'},
                        },
                    },
                },
            })

    async def get(self, username: str) -> User | None:
        await self._ensure_index()
        response = await self.client.options(ignore_status=404).get(
            index=self.index_name, id=username,
        )
        if not response.get('found', False):
            return None
        return User.model_validate(response['_source'])

    async def filter(self) -> list[User]:
        await self._ensure_index()
        response = await self.client.search(
            index=self.index_name, query={'match_all': {}}, size=10_000,
            sort=[{'username': 'asc'}],
        )
        return [User.model_validate(hit['_source']) for hit in response['hits']['hits']]

    async def save(self, user: User):
        await self._ensure_index()
        await self.client.index(
            index=self.index_name, id=user.username,
            document=user.model_dump(exclude_none=True, mode='json'), refresh='wait_for',
        )

    async def update(self, user: User):
        await self._ensure_index()
        if not await self.client.exists(index=self.index_name, id=user.username):
            return
        await self.client.index(
            index=self.index_name, id=user.username,
            document=user.model_dump(exclude_none=True, mode='json'), refresh='wait_for',
        )

    async def delete(self, username: str):
        await self._ensure_index()
        await self.client.options(ignore_status=404).delete(
            index=self.index_name, id=username, refresh='wait_for',
        )

    async def save_api_key(self, username: str, api_key: UserApiKey):
        user = await self.get(username)
        if user is None:
            return
        keys = list(user.api_keys or [])
        keys.append(api_key)
        await self.save(user.model_copy(update={'api_keys': keys}))

    async def delete_api_key(self, username: str, key_id: UUID):
        user = await self.get(username)
        if user is None:
            return
        keys = [key for key in user.api_keys or [] if key.key_id != key_id]
        await self.save(user.model_copy(update={'api_keys': keys or None}))

    async def get_user_by_api_key(self, key_hash: str) -> User | None:
        await self._ensure_index()
        response = await self.client.search(
            index=self.index_name,
            query={'nested': {'path': 'apiKeys', 'query': {'term': {'apiKeys.keyHash': key_hash}}}},
            size=1,
        )
        hits = response['hits']['hits']
        return User.model_validate(hits[0]['_source']) if hits else None


class ElasticsearchDocumentDatabase(DocumentDatabase):
    """Store one concept per document and use Elasticsearch's native text ranking."""

    _backend_name = 'elasticsearch'
    _client: AsyncElasticsearch | None = None

    def __init__(self, client: AsyncElasticsearch | None = None):
        if client is not None:
            self._client = client

    @classmethod
    def set_client(cls, client: AsyncElasticsearch):
        cls._client = client

    @property
    def client(self) -> AsyncElasticsearch:
        if self._client is None:
            raise ValueError('Elasticsearch client is not set')
        return self._client

    def _index_name(self, prefix: ConceptPrefix) -> str:
        return f'{_index_part(CONFIG.elasticsearch_index_prefix)}-concept-{_index_part(prefix.value)}'

    @property
    def users(self) -> ElasticsearchUserRepository:
        return ElasticsearchUserRepository(
            self.client, f'{_index_part(CONFIG.elasticsearch_index_prefix)}-users',
        )

    async def initialize(self):
        await self.users._ensure_index()

    async def close(self):
        if self._client is not None:
            await self._client.close()

    async def _ensure_index(self, prefix: ConceptPrefix) -> str:
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            LOGGER.info('Creating Elasticsearch concept index: %s', name)
            autocomplete = {
                'type': 'text', 'analyzer': 'bts_ngram', 'search_analyzer': 'standard',
            }
            await self.client.indices.create(
                index=name,
                settings={
                    'index.max_ngram_diff': 17,
                    'analysis': {'analyzer': {'bts_ngram': {
                        'type': 'custom', 'tokenizer': 'bts_ngram_tokenizer',
                        'filter': ['lowercase'],
                    }}, 'tokenizer': {'bts_ngram_tokenizer': {
                        'type': 'ngram', 'min_gram': 3, 'max_gram': 20,
                        'token_chars': ['letter', 'digit'],
                    }}},
                },
                mappings={'properties': {
                    'conceptId': {'type': 'keyword', 'fields': {'search': autocomplete}},
                    'prefix': {'type': 'keyword'},
                    'label': {**autocomplete, 'fields': {'exact': {'type': 'keyword'}}},
                    'synonyms': autocomplete,
                }})
        return name

    async def create_index(self, prefix, field, unique=False, overwrite=False):
        name = await self._ensure_index(prefix)
        if unique and field != 'conceptId':
            raise IndexCreationError('Elasticsearch cannot enforce uniqueness except via document _id')
        # These are part of the base mapping created with every vocabulary index. Reapplying
        # them as a generic keyword mapping would conflict with label's n-gram text mapping.
        if field in {'conceptId', 'label', 'synonyms', 'prefix'}:
            return
        try:
            await self.client.indices.put_mapping(
                index=name, properties={field: {'type': 'keyword'}},
            )
        except Exception as exc:
            if not overwrite:
                raise IndexCreationError(f'Failed to map {name}.{field}: {exc}') from exc

    async def delete_index(self, prefix, field):
        # Elasticsearch mappings cannot remove individual fields without reindexing. Keeping
        # the mapping is harmless after callers stop writing/querying that field.
        return None

    async def save_terms(self, terms: list[Concept], no_upsert: bool = False):
        if not terms:
            return
        name = await self._ensure_index(terms[0].prefix)
        LOGGER.info(
            'Writing %s concepts to Elasticsearch index %s (create_only=%s)',
            len(terms), name, no_upsert,
        )

        async def actions():
            for term in terms:
                yield {
                    '_op_type': 'create' if no_upsert else 'index',
                    '_index': name,
                    '_id': term.concept_id,
                    '_source': term.model_dump(exclude_none=True, mode='json'),
                }

        await async_bulk(
            self.client, actions(), chunk_size=CONFIG.elasticsearch_batch_size,
            refresh='wait_for',
        )
        LOGGER.info('Elasticsearch concept write complete: %s (%s concepts)', name, len(terms))

    async def count_terms(self, prefix):
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return 0
        return int((await self.client.count(index=name))['count'])

    async def get_terms_iter(self, prefix, limit=0, model_class=Concept) -> AsyncIterator[ConceptUnion]:
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return
        remaining = limit if limit > 0 else None
        search_after = None
        while remaining is None or remaining > 0:
            size = min(1000, remaining) if remaining is not None else 1000
            kwargs = dict(
                index=name, query={'match_all': {}}, size=size,
                sort=[{'conceptId': 'asc'}],
            )
            if search_after is not None:
                kwargs['search_after'] = search_after
            response = await self.client.search(**kwargs)
            hits = response['hits']['hits']
            if not hits:
                return
            for hit in hits:
                yield model_class.model_validate(hit['_source'])
                if remaining is not None:
                    remaining -= 1
            search_after = hits[-1]['sort']

    async def get_terms_by_ids_iter(self, prefix, concept_ids, model_class=Concept):
        if not concept_ids:
            return
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return
        response = await self.client.mget(index=name, ids=concept_ids)
        for doc in response['docs']:
            if doc.get('found'):
                yield model_class.model_validate(doc['_source'])

    async def delete_all_for_label(self, prefix):
        name = self._index_name(prefix)
        if await self.client.indices.exists(index=name):
            LOGGER.info('Deleting Elasticsearch concept index: %s', name)
            await self.client.indices.delete(index=name)
        await self._ensure_index(prefix)

    @staticmethod
    def _text_fields() -> list[str]:
        return ['conceptId.search', 'label', 'synonyms']

    async def lexical_search_iter(self, prefix, query, limit=10):
        search_query = normalise_search_query(query)
        if not search_query.words:
            return
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return
        response = await self.client.search(
            index=name, size=limit,
            query={'multi_match': {
                'query': search_query.clean, 'fields': self._text_fields(), 'type': 'best_fields',
            }},
            source=False,
        )
        for hit in response['hits']['hits']:
            yield hit['_id'], float(hit['_score'] or 0.0)

    async def _auto_complete_iter(self, prefix, search_query: SearchQuery, limit, model_class):
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return
        response = await self.client.search(
            index=name, size=limit or 10_000,
            query={'multi_match': {
                'query': search_query.clean, 'fields': self._text_fields(),
                'type': 'best_fields', 'operator': 'and',
            }},
            sort=[
                {'_score': 'desc'},
                {'label.exact': {'order': 'asc', 'unmapped_type': 'keyword'}},
                {'conceptId': 'asc'},
            ],
        )
        for hit in response['hits']['hits']:
            yield model_class.model_validate(hit['_source'])

    async def get_random_term_ids(self, prefix, count):
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return []
        response = await self.client.search(
            index=name, size=count, source=False,
            query={'function_score': {'query': {'match_all': {}}, 'random_score': {}}},
        )
        return [hit['_id'] for hit in response['hits']['hits']]
