from uuid import uuid4
from typing import AsyncIterator
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from .vector_db import VectorDatabase


class QdrantVectorDatabase(VectorDatabase):
    """
    Qdrant vector database implementation.
    """

    _client: AsyncQdrantClient | None = None

    def __init__(self,
                 client: AsyncQdrantClient | None = None,
                 embedding_dimension: int = 768,
                 ):
        """
        Initialise the Qdrant vector database.
        :param client: Optional Qdrant client instance or None to use class variable
        :param embedding_dimension: Dimension of the embedding vectors, defaults to 768 (for BGE)
        """
        if client is not None:
            self._client = client

        self._embedding_dimension = embedding_dimension

    @property
    def client(self) -> AsyncQdrantClient:
        """
        Return the Qdrant client instance.
        :return: The Qdrant client
        """
        if self._client is None:
            raise ValueError(
                'Qdrant client is not set. Please set it using set_client method or pass it '
                'during initialization.'
            )

        return self._client

    @classmethod
    def set_client(cls, client: AsyncQdrantClient):
        """
        Set the Qdrant client for the class.
        :param client: The Qdrant client instance
        """
        cls._client = client

    async def close(self):
        """
        Close the Qdrant client connection.
        """
        if self._client is not None:
            await self._client.close()

    async def create_collection(self,
                                collection_name: str,
                                distance: Distance = Distance.COSINE,
                                ):
        """
        Create a Qdrant collection with the specified name and distance metric.
        :param collection_name: The name of the collection to create
        :param distance: The distance metric to use for the collection
        """
        await self.client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=self._embedding_dimension,
                distance=distance,
            )
        )

    async def delete_collection(self,
                                collection_name: str,
                                ):
        """
        Delete a Qdrant collection by name.
        :param collection_name: The name of the collection to delete
        """
        await self.client.delete_collection(collection_name=collection_name)

    async def insert_concepts(self,
                              concepts: list[Concept] | AsyncIterator[Concept],
                              prefix: ConceptPrefix,
                              ) -> dict[str, str]:
        """
        Insert concepts into the Qdrant collection.
        :param concepts: list of Concept instances to insert, or an async iterator of Concept instances
        :param prefix: The prefix of the concepts being inserted
        :return: A mapping of concept IDs to their assigned point IDs in Qdrant
        """
        if not concepts:
            return {}

        id_map = {}
        collection_name = prefix.value

        # Check if the collection exists
        collection_list = await self.client.get_collections()
        existing = [c.name for c in collection_list.collections]
        if collection_name not in existing:
            await self.create_collection(collection_name=collection_name)

        async for embedded_batch in self.embed_concepts(concepts):
            points = []
            for concept_id, vector in embedded_batch:
                point_id = str(uuid4())
                points.append(PointStruct(
                    id=point_id,
                    vector=vector,
                    payload={'conceptId': concept_id}
                ))

                id_map[concept_id] = point_id

            await self.client.upsert(
                collection_name=collection_name,
                points=points,
            )

        return id_map

    async def get_vectors_for_prefix_iter(self,
                                          prefix: ConceptPrefix,
                                          ) -> AsyncIterator[tuple[str, list[float]]]:
        """
        Get all vectors for a given prefix from the vector database as an async iterator.
        :param prefix: The vocabulary prefix to get vectors for.
        :return: An asynchronous iterator yielding tuples of concept IDs and their embedding vectors.
        """
        collection_name = prefix.value

        # Check if the collection exists
        collection_list = await self.client.get_collections()
        existing = [c.name for c in collection_list.collections]
        if collection_name not in existing:
            raise ValueError(f'Vocabulary prefix {prefix} does not exist in Qdrant.')

        offset = None
        limit = 100

        while True:
            points, new_offset = await self.client.scroll(
                collection_name=collection_name,
                with_vectors=True,
                limit=limit,
                offset=offset
            )

            if not points:
                break

            for point in points:
                concept_id = point.payload.get('conceptId')
                if concept_id is not None:
                    yield concept_id, point.vector

            if new_offset is None or new_offset == offset:
                break

            offset = new_offset

    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all vectors for a given prefix from the vector database.
        :param prefix: The vocabulary prefix to delete vectors for.
        """
        collection_name = prefix.value

        # Check if the collection exists
        collection_list = await self.client.get_collections()
        existing = [c.name for c in collection_list.collections]
        if collection_name not in existing:
            return

        await self.delete_collection(collection_name=collection_name)
