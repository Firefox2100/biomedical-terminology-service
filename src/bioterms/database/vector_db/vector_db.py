"""
Abstract base class for vector databases.
"""

import time
import threading
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from sentence_transformers import SentenceTransformer

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import VectorDatabaseDriverType, ConceptPrefix
from bioterms.etc.utils import get_transformer, aiter_progress
from bioterms.etc.metrics import EMBED_LOCK_WAIT, EMBED_DURATION, EMBED_TEXTS, EMBED_CHARS, \
    EMBED_ERRORS
from bioterms.model.concept import Concept


class VectorDatabase(ABC):
    """
    Abstract base class for vector databases.
    """
    _embed_lock = threading.Lock()

    @classmethod
    def _embed_strings(cls,
                       texts: list[str],
                       transformer: SentenceTransformer = None,
                       ) -> list[list[float]]:
        """
        Embed a list of strings using the provided SentenceTransformer.
        :param texts: The list of strings to embed
        :param transformer: The SentenceTransformer instance to use for embedding; if None,
            the default transformer will be used
        :return:
        """
        if transformer is None:
            managed = False
            transformer = get_transformer()
        else:
            managed = True

        model = transformer.model_card_data.base_model if transformer.model_card_data else 'custom'
        EMBED_TEXTS.labels(model=model).observe(len(texts))
        EMBED_CHARS.labels(model=model).observe(sum(len(t) for t in texts))

        def encode_with_metrics():
            enc_start = time.perf_counter()
            vs = transformer.encode(
                sentences=texts,
                normalize_embeddings=True,
            )
            enc_end = time.perf_counter()
            EMBED_DURATION.labels(model=model, result='ok').observe(enc_end - enc_start)
            return vs

        wait_start = time.perf_counter()
        try:
            if not managed:
                with cls._embed_lock:
                    wait_end = time.perf_counter()
                    EMBED_LOCK_WAIT.labels(model=model).observe(wait_end - wait_start)

                    vectors = encode_with_metrics()
            else:
                # Assume the provided transformer is thread-safe or already managed
                vectors = encode_with_metrics()
        except Exception as e:
            EMBED_DURATION.labels(model=model, result='error').observe(0.0)
            EMBED_ERRORS.labels(model=model, error_type=type(e).__name__).inc()
            raise

        return [vector.tolist() for vector in vectors]

    @classmethod
    async def embed_concepts(cls,
                             concepts: list[Concept] | AsyncIterator[Concept],
                             batch_size: int = 32,
                             transformer: SentenceTransformer = None,
                             total_concepts: int = None,
                             ) -> AsyncIterator[list[tuple[str, list[float]]]]:
        """
        Embed concept texts using the configured SentenceTransformer model.
        :param concepts: A list or async iterator of Concept instances to embed
        :param batch_size: Number of concepts to process in each batch
        :param transformer: Optional SentenceTransformer instance to use for embedding
        :param total_concepts: Optional total number of concepts, used for progress tracking
        :return: An iterator of chunks of tuples containing concept IDs and their embedding vectors
        """
        def process_batch(b: list[Concept]) -> list[tuple[str, list[float]]]:
            vectors = cls._embed_strings(
                texts=[c.canonical_text() for c in b],
                transformer=transformer,
            )

            return [(c.concept_id, vectors[idx]) for idx, c in enumerate(b)]

        if isinstance(concepts, AsyncIterator):
            batch = []

            async for concept in aiter_progress(
                concepts,
                description='Embedding concepts',
                total=total_concepts,
            ):
                batch.append(concept)

                if len(batch) >= batch_size:
                    yield process_batch(batch)
                    batch = []

            if batch:
                yield process_batch(batch)
        elif isinstance(concepts, list):
            while concepts:
                batch = concepts[:batch_size]
                del concepts[:batch_size]

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
