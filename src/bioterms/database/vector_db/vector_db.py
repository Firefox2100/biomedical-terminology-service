"""
Abstract base class for vector databases.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import VectorDatabaseDriverType, ConceptPrefix
from bioterms.model.concept import Concept


class VectorDatabase(ABC):
    """
    Abstract base class for vector databases.
    """
    @abstractmethod
    async def close(self) -> None:
        """
        Close the vector database connection.
        """

    @abstractmethod
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

    @abstractmethod
    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of concept vectors for a given prefix in the vector database.
        :param prefix: The vocabulary prefix to count vectors for.
        :return: The number of vectors as an integer.
        """

    @abstractmethod
    def get_vectors_for_prefix_iter(self,
                                    prefix: ConceptPrefix,
                                    ) -> AsyncIterator[tuple[str, list[float]]]:
        """
        Get all vectors for a given prefix from the vector database as an async iterator.
        :param prefix: The vocabulary prefix to get vectors for.
        :return: An asynchronous iterator yielding tuples of concept IDs and their embedding vectors.
        """

    async def get_vectors_for_prefix(self,
                                     prefix: ConceptPrefix,
                                     ) -> dict[str, list[float]]:
        """
        Get all vectors for a given prefix from the vector database.
        :param prefix: The vocabulary prefix to get vectors for.
        :return: A dictionary mapping concept IDs to their embedding vectors.
        """
        vector_iter = self.get_vectors_for_prefix_iter(prefix)

        results: dict[str, list[float]] = {}
        async for concept_id, vector in vector_iter:
            results[concept_id] = vector

        return results

    @abstractmethod
    def search_concepts_iter(self,
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

    async def search_concepts(self,
                              query: str,
                              prefix: ConceptPrefix,
                              limit: int = 10,
                              ) -> list[str]:
        """
        Search for concepts matching the query within the specified vocabulary prefix.
        :param query: The search query string.
        :param prefix: The vocabulary prefix to search within.
        :param limit: The top number of concepts to return.
        :return: A list of matching Concept instances.
        """
        concept_ids = []
        concept_iter = self.search_concepts_iter(
            query=query,
            prefix=prefix,
            limit=limit,
        )

        async for concept_id in concept_iter:
            concept_ids.append(concept_id)

        return concept_ids

    @abstractmethod
    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all vectors for a given prefix from the vector database.
        :param prefix: The vocabulary prefix to delete vectors for.
        """


_active_vector_db: VectorDatabase | None = None


def get_active_vector_db() -> VectorDatabase:
    """
    Return the active vector database set by configuration
    :return: The active VectorDatabase instance
    """
    global _active_vector_db

    if _active_vector_db is not None:
        return _active_vector_db

    if CONFIG.vector_database_driver == VectorDatabaseDriverType.QDRANT:
        from qdrant_client import AsyncQdrantClient
        from .qdrant_vector_db import QdrantVectorDatabase

        qdrant_client = AsyncQdrantClient(
            location=CONFIG.qdrant_location,
        )
        QdrantVectorDatabase.set_client(qdrant_client)

        _active_vector_db = QdrantVectorDatabase()

        return _active_vector_db

    raise ValueError(f'Unsupported vector database driver: {CONFIG.vector_database_driver}')
