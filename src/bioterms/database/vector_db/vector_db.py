from abc import ABC, abstractmethod
from typing import AsyncIterator
from sentence_transformers import SentenceTransformer

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import VectorDatabaseDriverType
from bioterms.etc.utils import get_transformer
from bioterms.model.concept import Concept


class VectorDatabase(ABC):
    """
    Abstract base class for vector databases.
    """
    @staticmethod
    def _build_text_for_concept(concept: Concept) -> str:
        """
        Build the text representation for a concept to be embedded.
        :param concept: The Concept instance
        :return: The text representation of the concept
        """
        concept_str = ''

        if concept.label:
            concept_str += concept.label + ': '

        if concept.definition:
            concept_str += concept.definition + ' '

        if concept.synonyms:
            concept_str += '(' + ' '.join(concept.synonyms) + ')'

        return concept_str.strip(' :')

    @classmethod
    async def embed_concepts(cls,
                             concepts: list[Concept] | AsyncIterator[Concept],
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
            texts = [cls._build_text_for_concept(c) for c in b]

            vectors = transformer.encode(
                sentences=texts,
                batch_size=batch_size,
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
                              ) -> dict[str, str]:
        """
        Insert concepts into the Qdrant collection. All concepts must have the same prefix.
        :param concepts: list of Concept instances to insert, or an async iterator of Concept instances
        :return: A mapping of concept IDs to their assigned point IDs in Qdrant
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
