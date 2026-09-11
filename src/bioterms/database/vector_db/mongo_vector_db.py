"""
MongoDB implementation of the VectorDatabase interface.
"""
from uuid import uuid4
from typing import AsyncIterator
from pymongo import AsyncMongoClient, UpdateOne
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.operations import SearchIndexModel

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from bioterms.embedding import ConceptTransformer, TextTransformer
from .vector_db import VectorDatabase


class MongoVectorDatabase(VectorDatabase):
    """
    MongoDB implementation of the VectorDatabase interface.

    Unlike the Qdrant driver, which keeps vectors in a dedicated point store, this driver
    embeds the vector directly as a "vector" field on the concept documents in the same
    MongoDB database used by ``MongoDocumentDatabase`` (one collection per vocabulary
    prefix, matched on "conceptId"). Similarity search is performed with the
    ``$vectorSearch`` aggregation stage, which requires a MongoDB deployment with Atlas
    Search / mongot support (MongoDB Atlas, Atlas Local, or a self-managed deployment with
    Search enabled).
    """

    _client: AsyncMongoClient | None = None

    def __init__(self,
                 client: AsyncMongoClient | None = None,
                 embedding_dimension: int = 768,
                 ):
        """
        Initialise the MongoDB vector database.
        :param client: Optional AsyncMongoClient instance or None to use class variable
        :param embedding_dimension: Dimension of the embedding vectors, defaults to 768 (for BGE)
        """
        if client is not None:
            self._client = client

        self._embedding_dimension = embedding_dimension

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

    async def _ensure_vector_index(self,
                                   collection_name: str,
                                   ):
        """
        Ensure a $vectorSearch index exists on the "vector" field of the given collection,
        creating it if necessary. Newly created indexes are built asynchronously by mongot,
        so they may not be immediately queryable.

        This always checks the server rather than caching the result, since operations such
        as vocabulary deletion drop and recreate the underlying collection (and, with it, any
        search index), and this instance's lifetime can span multiple such reloads.
        :param collection_name: The name of the collection to ensure the index for.
        """
        collection = self.db[collection_name]
        index_name = CONFIG.mongodb_vector_index_name

        existing_indexes = await collection.list_search_indexes(name=index_name)
        if not [idx async for idx in existing_indexes]:
            await collection.create_search_index(
                SearchIndexModel(
                    definition={
                        'fields': [
                            {
                                'type': 'vector',
                                'path': 'vector',
                                'numDimensions': self._embedding_dimension,
                                'similarity': 'cosine',
                            },
                        ],
                    },
                    name=index_name,
                    type='vectorSearch',
                )
            )

    async def load_embeddings(self,
                              prefix: ConceptPrefix,
                              embeddings: AsyncIterator[tuple[str, str, list[float]]],
                              total_embeddings: int | None = None,
                              ) -> dict[str, str]:
        """
        Load precomputed embeddings into the vocabulary's MongoDB collection.
        :param prefix: The vocabulary prefix of the embeddings
        :param embeddings: An async iterator of tuples containing (concept_id, text, embedding_vector)
        :param total_embeddings: Optional total number of embeddings, used for progress tracking
        :return: A mapping of concept IDs to their assigned vector IDs
        """
        collection_name = prefix.value
        await self._ensure_vector_index(collection_name)
        collection = self.db[collection_name]

        operations: list[UpdateOne] = []
        id_map: dict[str, str] = {}

        async for concept_id, vector_id, vector in embeddings:
            operations.append(UpdateOne(
                {'conceptId': concept_id},
                {'$set': {'vector': vector, 'vectorId': vector_id}},
                upsert=True,
            ))

            id_map[concept_id] = vector_id

            if len(operations) >= 1000:
                await collection.bulk_write(operations)
                operations = []

        if operations:
            await collection.bulk_write(operations)

        return id_map

    async def insert_concepts(self,
                              concepts: list[Concept] | AsyncIterator[Concept],
                              prefix: ConceptPrefix,
                              total_concepts: int | None = None,
                              ) -> dict[str, str]:
        """
        Embed and insert concepts' vectors into the vocabulary's MongoDB collection.
        :param concepts: list of Concept instances to insert, or an async iterator of Concept instances
        :param prefix: The prefix of the concepts being inserted
        :param total_concepts: Optional total number of concepts, used for progress tracking
        :return: A mapping of concept IDs to their assigned vector IDs
        """
        if not concepts:
            return {}

        if isinstance(concepts, list):
            total_concepts = len(concepts)

        transformer = ConceptTransformer()

        async def embedding_iter():
            async for embedded_batch in transformer.embed_concepts(concepts, total_concepts=total_concepts):
                for concept_id, vector in embedded_batch:
                    yield concept_id, str(uuid4()), vector

        return await self.load_embeddings(
            prefix=prefix,
            embeddings=embedding_iter(),
            total_embeddings=total_concepts,
        )

    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of concept vectors for a given prefix in the vector database.
        :param prefix: The vocabulary prefix to count vectors for.
        :return: The number of vectors as an integer.
        """
        collection = self.db[prefix.value]
        return await collection.count_documents({'vector': {'$exists': True}})

    async def get_vectors_for_prefix_iter(self,
                                          prefix: ConceptPrefix,
                                          ) -> AsyncIterator[tuple[str, list[float]]]:
        """
        Get all vectors for a given prefix from the vector database as an async iterator.
        :param prefix: The vocabulary prefix to get vectors for.
        :return: An asynchronous iterator yielding tuples of concept IDs and their embedding vectors.
        """
        collection = self.db[prefix.value]
        cursor = collection.find(
            {'vector': {'$exists': True}},
            {'_id': 0, 'conceptId': 1, 'vector': 1},
        )

        async for doc in cursor:
            yield doc['conceptId'], doc['vector']

    async def search_concepts_iter(self,
                                   query: str,
                                   prefix: ConceptPrefix,
                                   limit: int = 10,
                                   ) -> AsyncIterator[str]:
        """
        Search for concepts matching the query within the specified vocabulary prefix, and
        return an async iterator of matching concept IDs.
        :param query: The search query string.
        :param prefix: The vocabulary prefix to search within.
        :param limit: The top number of concepts to return.
        :return: An async iterator of matching concept IDs.
        """
        collection_name = prefix.value
        await self._ensure_vector_index(collection_name)
        collection = self.db[collection_name]

        text_transformer = TextTransformer()
        query_vector = text_transformer.embed_strings([query])[0]

        pipeline = [
            {
                '$vectorSearch': {
                    'index': CONFIG.mongodb_vector_index_name,
                    'path': 'vector',
                    'queryVector': query_vector,
                    'numCandidates': limit * CONFIG.mongodb_vector_num_candidates_multiplier,
                    'limit': limit,
                },
            },
            {
                '$project': {
                    '_id': 0,
                    'conceptId': 1,
                },
            },
        ]

        cursor = await collection.aggregate(pipeline)
        async for doc in cursor:
            concept_id = doc.get('conceptId')
            if concept_id is not None:
                yield concept_id

    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all vectors for a given prefix from the vector database.
        :param prefix: The vocabulary prefix to delete vectors for.
        """
        collection = self.db[prefix.value]
        await collection.update_many(
            {'vector': {'$exists': True}},
            {'$unset': {'vector': '', 'vectorId': ''}},
        )
