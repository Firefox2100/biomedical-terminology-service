"""Elasticsearch dense-vector implementation of :class:`VectorDatabase`."""

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from bioterms.database.doc_db.elasticsearch_doc_db import _index_part
from bioterms.etc.consts import CONFIG, LOGGER
from bioterms.etc.enums import ConceptPrefix
from bioterms.embedding import TextTransformer
from .vector_db import VectorDatabase


class ElasticsearchVectorDatabase(VectorDatabase):
    """Store embedding items as documents in one dense-vector index per vocabulary."""

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
        return f'{_index_part(CONFIG.elasticsearch_index_prefix)}-vector-{_index_part(prefix.value)}'

    async def close(self):
        if self._client is not None:
            await self._client.close()

    async def _ensure_index(self, prefix: ConceptPrefix) -> str:
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            LOGGER.info('Creating Elasticsearch vector index: %s', name)
            await self.client.indices.create(index=name, mappings={'properties': {
                'itemId': {'type': 'keyword'},
                'conceptId': {'type': 'keyword'},
                'kind': {'type': 'keyword'},
                'text': {'type': 'text', 'index': False},
                'vector': {
                    'type': 'dense_vector',
                    'dims': TextTransformer().dimension,
                    'index': True,
                    'similarity': 'cosine',
                },
            }})
        return name

    async def load_embedding_items(self, prefix, items, total_items=None) -> int:
        name = await self._ensure_index(prefix)
        written = 0
        LOGGER.info(
            'Writing embeddings to Elasticsearch index %s (expected=%s)',
            name, total_items if total_items is not None else 'unknown',
        )

        async def actions():
            nonlocal written
            async for item in items:
                written += 1
                yield {
                    '_op_type': 'index', '_index': name, '_id': item.item_id,
                    '_source': {
                        'itemId': item.item_id, 'conceptId': item.concept_id,
                        'kind': item.kind.value, 'text': item.text, 'vector': item.vector,
                    },
                }

        await async_bulk(
            self.client, actions(), chunk_size=CONFIG.elasticsearch_batch_size,
            refresh='wait_for',
        )
        LOGGER.info('Elasticsearch embedding write complete: %s (%s items)', name, written)
        return written

    async def get_embedded_concept_ids(self, prefix) -> set[str]:
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return set()
        result: set[str] = set()
        after = None
        while True:
            composite = {
                'size': 1000,
                'sources': [{'concept': {'terms': {'field': 'conceptId'}}}],
            }
            if after is not None:
                composite['after'] = after
            response = await self.client.search(
                index=name, size=0, aggs={'concepts': {'composite': composite}},
            )
            page = response['aggregations']['concepts']
            result.update(bucket['key']['concept'] for bucket in page['buckets'])
            after = page.get('after_key')
            if after is None:
                return result

    async def count_vectors(self, prefix) -> int:
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return 0
        return int((await self.client.count(index=name))['count'])

    async def search_items_iter(self, query_vector, prefix, kind, limit=10):
        if limit <= 0:
            return
        name = self._index_name(prefix)
        if not await self.client.indices.exists(index=name):
            return
        candidates = max(limit, limit * CONFIG.elasticsearch_vector_num_candidates_multiplier)
        response = await self.client.search(
            index=name,
            knn={
                'field': 'vector', 'query_vector': query_vector, 'k': limit,
                'num_candidates': candidates, 'filter': {'term': {'kind': kind.value}},
            },
            size=limit,
            source=['conceptId', 'text'],
        )
        for hit in response['hits']['hits']:
            source = hit['_source']
            yield source['conceptId'], source.get('text', ''), float(hit['_score'] or 0.0)

    async def delete_vectors_for_prefix(self, prefix) -> None:
        name = self._index_name(prefix)
        if await self.client.indices.exists(index=name):
            LOGGER.info('Deleting Elasticsearch vector index: %s', name)
            await self.client.indices.delete(index=name)
