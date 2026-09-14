"""
MongoDB implementation of the VectorDatabase interface.
"""
from typing import AsyncIterator
from pymongo import AsyncMongoClient, UpdateOne
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.operations import SearchIndexModel

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.embedding import TextTransformer
from .vector_db import VectorDatabase, EmbeddingItemVector


class MongoVectorDatabase(VectorDatabase):
    """
    MongoDB implementation of the VectorDatabase interface.

    Embedding items are stored in a dedicated "<prefix>.vectors" collection -- one document
    per embedding item (a concept's label, a single synonym, or its definition), keyed on
    `itemId`, separate from the "<prefix>" collection `MongoDocumentDatabase` uses for concept
    documents. The dot in the collection name follows this deployment's naming convention for
    a "sub-collection" of a vocabulary's data, distinguishing it from a literal document
    sub-resource. Similarity search is performed with the ``$vectorSearch`` aggregation
    stage, filtered on `kind`, which requires a MongoDB deployment with Atlas Search / mongot
    support (MongoDB Atlas, Atlas Local, or a self-managed deployment with Search enabled).
    """

    _client: AsyncMongoClient | None = None

    def __init__(self,
                 client: AsyncMongoClient | None = None,
                 ):
        """
        Initialise the MongoDB vector database.
        :param client: Optional AsyncMongoClient instance or None to use class variable
        """
        if client is not None:
            self._client = client

    @property
    def db(self) -> AsyncDatabase:
        """
        Return the MongoDB database instance.
        :return: The MongoDB database
        """
        if self._client is None:
            raise ValueError(
                'MongoDB client is not set. Please set it using set_client method or pass it '
                'during initialization.'
            )

        return self._client[CONFIG.mongodb_db_name]

    @classmethod
    def set_client(cls, client: AsyncMongoClient):
        """
        Set the MongoDB client for the class.
        :param client: The AsyncMongoClient instance
        """
        cls._client = client

    async def close(self):
        """
        Close the MongoDB client connection.
        """
        if self._client is not None:
            await self._client.close()

    @staticmethod
    def _vector_collection_name(prefix: ConceptPrefix) -> str:
        """
        The name of the embedding-item collection for a vocabulary prefix.
        :param prefix: The vocabulary prefix.
        :return: The collection name, e.g. "snomed.vectors".
        """
        return f'{prefix.value}.vectors'

    async def _ensure_vector_index(self,
                                   collection_name: str,
                                   ):
        """
        Ensure a $vectorSearch index exists on the "vector" field of the given collection,
        filterable on "kind", creating it if necessary. Newly created indexes are built
        asynchronously by mongot, so they may not be immediately queryable.

        This always checks the server rather than caching the result, since operations such
        as vocabulary deletion drop and recreate the underlying collection (and, with it, any
        search index), and this instance's lifetime can span multiple such reloads.
        :param collection_name: The name of the collection to ensure the index for.
        """
        collection = self.db[collection_name]
        index_name = CONFIG.mongodb_vector_index_name

        existing_indexes = await collection.list_search_indexes(name=index_name)
        if not [idx async for idx in existing_indexes]:
            dimension = TextTransformer().dimension

            await collection.create_search_index(
                SearchIndexModel(
                    definition={
                        'fields': [
                            {
                                'type': 'vector',
                                'path': 'vector',
                                'numDimensions': dimension,
                                'similarity': 'cosine',
                            },
                            {
                                'type': 'filter',
                                'path': 'kind',
                            },
                        ],
                    },
                    name=index_name,
                    type='vectorSearch',
                )
            )

    async def load_embedding_items(self,
                                   prefix: ConceptPrefix,
                                   items: AsyncIterator[EmbeddingItemVector],
                                   total_items: int | None = None,
                                   ) -> int:
        """
        Load precomputed embedding items into the vocabulary's "<prefix>.vectors" collection.

        Items are flushed in batches, but a batch is only ever cut at a concept boundary -- a
        concept's items are never split across two flushes. This makes "this concept has an
        item in the collection" a reliable proxy for "this concept's items were fully
        written", which `get_embedded_concept_ids` (and `insert_concepts`'s resume support)
        depends on.
        :param prefix: The vocabulary prefix of the embedding items
        :param items: An async iterator of EmbeddingItemVector instances
        :param total_items: Optional total number of items, used for progress tracking (unused
            by this driver, kept for interface compatibility)
        :return: The number of embedding items written
        """
        collection_name = self._vector_collection_name(prefix)
        await self._ensure_vector_index(collection_name)
        collection = self.db[collection_name]

        operations: list[UpdateOne] = []
        pending_concept_ops: list[UpdateOne] = []
        pending_concept_id: str | None = None
        written = 0

        async for item in items:
            if item.concept_id != pending_concept_id:
                operations.extend(pending_concept_ops)
                pending_concept_ops = []
                pending_concept_id = item.concept_id

                if len(operations) >= 1000:
                    await collection.bulk_write(operations)
                    operations = []

            pending_concept_ops.append(UpdateOne(
                {'_id': item.item_id},
                {'$set': {
                    'conceptId': item.concept_id,
                    'kind': item.kind.value,
                    'text': item.text,
                    'vector': item.vector,
                }},
                upsert=True,
            ))
            written += 1

        operations.extend(pending_concept_ops)
        if operations:
            await collection.bulk_write(operations)

        return written

    async def get_embedded_concept_ids(self,
                                       prefix: ConceptPrefix,
                                       ) -> set[str]:
        """
        Return the concept IDs that already have embedding items stored for a given prefix.
        :param prefix: The vocabulary prefix to check.
        :return: The set of concept IDs with at least one stored embedding item.
        """
        collection = self.db[self._vector_collection_name(prefix)]
        return set(await collection.distinct('conceptId'))

    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of embedding items for a given prefix in the vector database.
        :param prefix: The vocabulary prefix to count embedding items for.
        :return: The number of embedding items as an integer.
        """
        collection = self.db[self._vector_collection_name(prefix)]
        return await collection.count_documents({})

    async def search_items_iter(self,
                                query_vector: list[float],
                                prefix: ConceptPrefix,
                                kind: EmbeddingKind,
                                limit: int = 10,
                                ) -> AsyncIterator[tuple[str, str, float]]:
        """
        Search for embedding items of the given kind whose vector is closest to
        `query_vector`, within the specified vocabulary prefix.
        :param query_vector: The already-embedded query vector.
        :param prefix: The vocabulary prefix to search within.
        :param kind: Which embedding items to search (ALIAS or DEFINITION).
        :param limit: The top number of items to return.
        :return: An async iterator of (concept_id, item_text, score) tuples, best match first.
        """
        collection_name = self._vector_collection_name(prefix)
        await self._ensure_vector_index(collection_name)
        collection = self.db[collection_name]

        pipeline = [
            {
                '$vectorSearch': {
                    'index': CONFIG.mongodb_vector_index_name,
                    'path': 'vector',
                    'queryVector': query_vector,
                    'filter': {'kind': kind.value},
                    'numCandidates': limit * CONFIG.mongodb_vector_num_candidates_multiplier,
                    'limit': limit,
                },
            },
            {
                '$project': {
                    '_id': 0,
                    'conceptId': 1,
                    'text': 1,
                    'score': {'$meta': 'vectorSearchScore'},
                },
            },
        ]

        cursor = await collection.aggregate(pipeline)
        async for doc in cursor:
            concept_id = doc.get('conceptId')
            if concept_id is not None:
                yield concept_id, doc.get('text', ''), doc.get('score', 0.0)

    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all embedding items for a given prefix from the vector database, dropping the
        "<prefix>.vectors" collection (and its search index) outright so that a later
        `load_embedding_items` starts from a clean schema.
        :param prefix: The vocabulary prefix to delete embedding items for.
        """
        await self.db[self._vector_collection_name(prefix)].drop()
