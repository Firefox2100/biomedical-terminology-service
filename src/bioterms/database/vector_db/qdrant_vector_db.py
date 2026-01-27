from uuid import uuid4
from typing import AsyncIterator
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
from qdrant_client.http.models import HnswConfigDiff
from qdrant_client.http.exceptions import UnexpectedResponse

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from bioterms.embedding import ConceptTransformer
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

    async def load_embeddings(self,
                              prefix: ConceptPrefix,
                              embeddings: AsyncIterator[tuple[str, str, list[float]]],
                              total_embeddings: int | None = None,
                              ) -> dict[str, str]:
        """
        Load precomputed embeddings into the Qdrant collection.
        :param prefix: The vocabulary prefix of the embeddings
        :param embeddings: An async iterator of tuples containing (concept_id, text, embedding_vector)
        :param total_embeddings: Optional total number of embeddings, used for progress tracking
        :return: A mapping of concept IDs to their assigned point IDs in Qdrant
        """
        # Disable HNSW indexing for faster bulk inserts
        collection_name = prefix.value
        await self.client.update_collection(
            collection_name=collection_name,
            hnsw_config=HnswConfigDiff(
                m=0,
            )
        )

        points = []
        id_map = {}
        async for concept_id, vector_id, vector in embeddings:
            for concept_id, vector in embedded_batch:
                points.append(PointStruct(
                    id=vector_id,
                    vector=vector,
                    payload={'conceptId': concept_id}
                ))

                id_map[concept_id] = point_id

            if len(points) > 1000:
                await self.client.upsert(
                    collection_name=collection_name,
                    points=points,
                )

                points = []

        if points:
            await self.client.upsert(
                collection_name=collection_name,
                points=points,
            )

        # Re-enable HNSW indexing after inserts
        await self.client.update_collection(
            collection_name=collection_name,
            hnsw_config=HnswConfigDiff(
                m=16,
                ef_construct=100,
            )
        )

        return id_map

    async def insert_concepts(self,
                              concepts: list[Concept] | AsyncIterator[Concept],
                              prefix: ConceptPrefix,
                              total_concepts: int | None = None,
                              ) -> dict[str, str]:
        """
        Insert concepts into the Qdrant collection.
        :param concepts: list of Concept instances to insert, or an async iterator of Concept instances
        :param prefix: The prefix of the concepts being inserted
        :param total_concepts: Optional total number of concepts, used for progress tracking
        :return: A mapping of concept IDs to their assigned point IDs in Qdrant
        """
        if not concepts:
            return {}

        collection_name = prefix.value

        # Check if the collection exists
        collection_list = await self.client.get_collections()
        existing = [c.name for c in collection_list.collections]
        if collection_name not in existing:
            await self.create_collection(collection_name=collection_name)

        if isinstance(concepts, list):
            total_concepts: int = len(concepts)

        transformer = ConceptTransformer()
        async def embedding_iter():
            async for embedded_batch in transformer.embed_concepts(concepts, total_concepts=total_concepts):
                for concept_id, vector in embedded_batch:
                    vector_id = str(uuid4())
                    yield concept_id, vector_id, vector

        id_map = await self.load_embeddings(
            prefix=prefix,
            embeddings=embedding_iter(),
            total_embeddings=total_concepts,
        )

        return id_map

    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of concept vectors for a given prefix in the vector database.
        :param prefix: The vocabulary prefix to count vectors for.
        :return: The number of vectors as an integer.
        """
        collection_name = prefix.value

        try:
            collection_info = await self.client.get_collection(
                collection_name=collection_name
            )

            return collection_info.points_count
        except UnexpectedResponse:
            return 0

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
        :return: A list of matching Concept instances.
        """
        collection_name = prefix.value
        query_vector = self._embed_strings(
            texts=[query],
        )[0]

        response = await self.client.query_points(
            collection_name=collection_name,
            query=query_vector,
            limit=limit,
            with_payload=True,
        )

        for p in response.points:
            concept_id = p.payload.get('conceptId')
            if concept_id is not None:
                yield concept_id

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
