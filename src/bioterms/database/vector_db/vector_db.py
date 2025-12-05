from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from sentence_transformers import SentenceTransformer

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import VectorDatabaseDriverType, ConceptPrefix
from bioterms.etc.utils import get_transformer
from bioterms.model.concept import Concept


class VectorDatabase(ABC):
    """
    Abstract base class for vector databases.
    """
    @staticmethod
    async def embed_concepts(concepts: list[Concept] | AsyncIterator[Concept],
                             batch_size: int = 32,
                             transformer: SentenceTransformer = None,
                             ) -> AsyncIterator[list[tuple[str, list[float]]]]:
        """
        Embed concept texts using the configured SentenceTransformer model.
        :return: An iterator of chunks of tuples containing concept IDs and their embedding vectors
        """
        if transformer is None:
            transformer = get_transformer()

        def process_batch(b: list[Concept]) -> list[tuple[str, list[float]]]:
            texts = [c.canonical_text() for c in b]

            vectors = transformer.encode(
                sentences=texts,
                normalize_embeddings=True,
            )

            return [(c.concept_id, vectors[idx].tolist()) for idx, c in enumerate(b)]

        if isinstance(concepts, AsyncIterator):
            batch = []

            async for concept in concepts:
                batch.append(concept)

                if len(batch) >= batch_size:
                    yield process_batch(batch)
                    batch = []

            if batch:
                yield process_batch(batch)
        elif isinstance(concepts, list):
            for i in range(0, len(concepts), batch_size):
                batch = concepts[i:i + batch_size]

                yield process_batch(batch)
        else:
            raise TypeError('concepts must be a list or an AsyncIterator of Concept instances')

    @abstractmethod
    async def close(self) -> None:
        """
        Close the vector database connection.
        """

    @abstractmethod
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
