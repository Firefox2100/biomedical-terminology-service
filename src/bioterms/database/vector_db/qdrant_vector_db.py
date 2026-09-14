from typing import AsyncIterator
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue, \
    PayloadSchemaType
from qdrant_client.http.models import HnswConfigDiff
from qdrant_client.http.exceptions import UnexpectedResponse

from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.embedding import TextTransformer
from .vector_db import VectorDatabase, EmbeddingItemVector


class QdrantVectorDatabase(VectorDatabase):
    """
    Qdrant vector database implementation. Each vocabulary prefix gets its own collection,
    with one point per embedding item (not per concept) -- a concept's label, each of its
    synonyms, and its definition are all separate points, distinguished by the payload's
    "kind" field so alias and definition items can be searched independently.
    """

    _client: AsyncQdrantClient | None = None

    def __init__(self,
                 client: AsyncQdrantClient | None = None,
                 ):
        """
        Initialise the Qdrant vector database.
        :param client: Optional Qdrant client instance or None to use class variable
        """
        if client is not None:
            self._client = client

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
        Create a Qdrant collection with the specified name and distance metric, sized to the
        currently configured embedding model's output dimension, and index the "kind" payload
        field so alias/definition searches can filter efficiently.
        :param collection_name: The name of the collection to create
        :param distance: The distance metric to use for the collection
        """
        dimension = TextTransformer().dimension

        await self.client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=dimension,
                distance=distance,
            )
        )
        await self.client.create_payload_index(
            collection_name=collection_name,
            field_name='kind',
            field_schema=PayloadSchemaType.KEYWORD,
        )

    async def delete_collection(self,
                                collection_name: str,
                                ):
        """
        Delete a Qdrant collection by name.
        :param collection_name: The name of the collection to delete
        """
        await self.client.delete_collection(collection_name=collection_name)

    async def load_embedding_items(self,
                                   prefix: ConceptPrefix,
                                   items: AsyncIterator[EmbeddingItemVector],
                                   total_items: int | None = None,
                                   ) -> int:
        """
        Load precomputed embedding items into the Qdrant collection.

        Items are flushed in batches, but a batch is only ever cut at a concept boundary -- a
        concept's items are never split across two flushes. This makes "this concept has a
        point in the collection" a reliable proxy for "this concept's items were fully
        written", which `get_embedded_concept_ids` (and `insert_concepts`'s resume support)
        depends on.
        :param prefix: The vocabulary prefix of the embedding items
        :param items: An async iterator of EmbeddingItemVector instances
        :param total_items: Optional total number of items, used for progress tracking (unused
            by this driver, kept for interface compatibility)
        :return: The number of embedding items written
        """
        collection_name = prefix.value

        collection_list = await self.client.get_collections()
        existing = [c.name for c in collection_list.collections]
        if collection_name not in existing:
            await self.create_collection(collection_name=collection_name)

        # Disable HNSW indexing for faster bulk inserts
        await self.client.update_collection(
            collection_name=collection_name,
            hnsw_config=HnswConfigDiff(
                m=0,
            )
        )

        points = []
        pending_concept_points = []
        pending_concept_id: str | None = None
        written = 0

        async for item in items:
            if item.concept_id != pending_concept_id:
                points.extend(pending_concept_points)
                pending_concept_points = []
                pending_concept_id = item.concept_id

                if len(points) > 1000:
                    await self.client.upsert(
                        collection_name=collection_name,
                        points=points,
                    )
                    points = []

            pending_concept_points.append(PointStruct(
                id=str(_stable_uuid(item.item_id)),
                vector=item.vector,
                payload={
                    'itemId': item.item_id,
                    'conceptId': item.concept_id,
                    'kind': item.kind.value,
                    'text': item.text,
                },
            ))
            written += 1

        points.extend(pending_concept_points)
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

        return written

    async def get_embedded_concept_ids(self,
                                       prefix: ConceptPrefix,
                                       ) -> set[str]:
        """
        Return the concept IDs that already have embedding items stored for a given prefix.
        :param prefix: The vocabulary prefix to check.
        :return: The set of concept IDs with at least one stored embedding item.
        """
        collection_name = prefix.value

        try:
            collection_list = await self.client.get_collections()
            existing = [c.name for c in collection_list.collections]
            if collection_name not in existing:
                return set()
        except UnexpectedResponse:
            return set()

        concept_ids: set[str] = set()
        next_offset = None

        while True:
            points, next_offset = await self.client.scroll(
                collection_name=collection_name,
                with_payload=['conceptId'],
                with_vectors=False,
                limit=1000,
                offset=next_offset,
            )
            for point in points:
                concept_id = point.payload.get('conceptId')
                if concept_id is not None:
                    concept_ids.add(concept_id)

            if next_offset is None:
                break

        return concept_ids

    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of embedding items for a given prefix in the vector database.
        :param prefix: The vocabulary prefix to count embedding items for.
        :return: The number of embedding items as an integer.
        """
        collection_name = prefix.value

        try:
            collection_info = await self.client.get_collection(
                collection_name=collection_name
            )

            return collection_info.points_count
        except UnexpectedResponse:
            return 0

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
        collection_name = prefix.value

        try:
            response = await self.client.query_points(
                collection_name=collection_name,
                query=query_vector,
                query_filter=Filter(
                    must=[FieldCondition(key='kind', match=MatchValue(value=kind.value))],
                ),
                limit=limit,
                with_payload=True,
            )
        except UnexpectedResponse:
            return

        for p in response.points:
            concept_id = p.payload.get('conceptId')
            if concept_id is not None:
                yield concept_id, p.payload.get('text', ''), p.score

    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all embedding items for a given prefix from the vector database.
        :param prefix: The vocabulary prefix to delete embedding items for.
        """
        collection_name = prefix.value

        # Check if the collection exists
        collection_list = await self.client.get_collections()
        existing = [c.name for c in collection_list.collections]
        if collection_name not in existing:
            return

        await self.delete_collection(collection_name=collection_name)


def _stable_uuid(item_id: str):
    """
    Deterministically derive a UUID from an embedding item id, so re-embedding the same
    concept/synonym/definition (e.g. after a partial restore) upserts the same Qdrant point
    instead of accumulating duplicates.
    :param item_id: The EmbeddingItem.item_id to derive a UUID from.
    :return: A UUID5 derived from `item_id`.
    """
    from uuid import uuid5, NAMESPACE_URL
    return uuid5(NAMESPACE_URL, item_id)
