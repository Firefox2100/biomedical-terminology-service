"""
Abstract base class for vector databases.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections.abc import AsyncIterator

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import VectorDatabaseDriverType, ConceptPrefix, EmbeddingKind
from bioterms.model.concept import Concept


@dataclass(frozen=True)
class EmbeddingItemVector:
    """
    An embedding item paired with its computed vector -- the unit of storage and retrieval
    for `VectorDatabase`. See `Concept.embedding_items` for how items are derived from a
    concept (one per distinct label/synonym, plus one for the definition when present).
    """
    item_id: str
    concept_id: str
    kind: EmbeddingKind
    text: str
    vector: list[float]


class VectorDatabase(ABC):
    """
    Abstract base class for vector databases.

    Storage is item-level, not concept-level: a concept with a label, three synonyms, and a
    definition contributes five separate embedding items/vectors, each individually indexed
    and searchable, rather than one vector for a concatenation of all of them. `kind` (ALIAS
    vs DEFINITION) lets callers search each recall arm independently -- this is what the
    hybrid search fusion in `bioterms.search.hybrid` relies on to keep alias-embedding and
    definition-embedding recall as separate ranked lists before combining them.
    """

    @abstractmethod
    async def close(self) -> None:
        """
        Close the vector database connection.
        """

    @abstractmethod
    async def load_embedding_items(self,
                                   prefix: ConceptPrefix,
                                   items: AsyncIterator[EmbeddingItemVector],
                                   total_items: int | None = None,
                                   ) -> int:
        """
        Load precomputed embedding items into the vector database.
        :param prefix: The vocabulary prefix of the embedding items.
        :param items: An async iterator of EmbeddingItemVector instances.
        :param total_items: Optional total number of items, used for progress tracking.
        :return: The number of embedding items written.
        """

    async def insert_concepts(self,
                              concepts: list[Concept] | AsyncIterator[Concept],
                              prefix: ConceptPrefix,
                              total_concepts: int | None = None,
                              ) -> int:
        """
        Embed every concept's embedding items (see `Concept.embedding_items`) and write them
        into the vector database. This is a thin, driver-independent wrapper around
        `load_embedding_items` built on the base class, so concrete drivers only need to
        implement storage, not the embedding call.
        :param concepts: A list of Concept instances to insert, or an async iterator of them.
        :param prefix: The prefix of the concepts being inserted.
        :param total_concepts: Optional total number of concepts, used for progress tracking.
        :return: The number of embedding items written.
        """
        from bioterms.embedding import ConceptTransformer

        if isinstance(concepts, list) and not concepts:
            return 0

        transformer = ConceptTransformer()

        async def item_iter() -> AsyncIterator[EmbeddingItemVector]:
            async for embedded_batch in transformer.embed_concepts(concepts, total_concepts=total_concepts):
                for item, vector in embedded_batch:
                    yield EmbeddingItemVector(
                        item_id=item.item_id,
                        concept_id=item.concept_id,
                        kind=item.kind,
                        text=item.text,
                        vector=vector,
                    )

        return await self.load_embedding_items(prefix=prefix, items=item_iter())

    @abstractmethod
    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of embedding items stored for a given prefix in the vector database.
        Note this counts individual embedding items (aliases + definitions), not concepts --
        a single concept with several synonyms contributes several items.
        :param prefix: The vocabulary prefix to count embedding items for.
        :return: The number of embedding items as an integer.
        """

    @abstractmethod
    def search_items_iter(self,
                          query_vector: list[float],
                          prefix: ConceptPrefix,
                          kind: EmbeddingKind,
                          limit: int = 10,
                          ) -> AsyncIterator[tuple[str, str, float]]:
        """
        Search for embedding items of the given kind whose vector is closest to
        `query_vector`, within the specified vocabulary prefix.
        :param query_vector: The already-embedded query vector (embedding the query text is
            the caller's responsibility, so it is only embedded once regardless of how many
            kinds/prefixes it is searched against).
        :param prefix: The vocabulary prefix to search within.
        :param kind: Which embedding items to search (ALIAS or DEFINITION).
        :param limit: The top number of items to return.
        :return: An async iterator of (concept_id, item_text, score) tuples, best match first.
            Multiple items can belong to the same concept; callers that need one rank per
            concept are responsible for collapsing duplicates (see `bioterms.search.hybrid`).
        """

    async def search_items(self,
                           query_vector: list[float],
                           prefix: ConceptPrefix,
                           kind: EmbeddingKind,
                           limit: int = 10,
                           ) -> list[tuple[str, str, float]]:
        """
        Search for embedding items of the given kind whose vector is closest to
        `query_vector`, within the specified vocabulary prefix.
        :param query_vector: The already-embedded query vector.
        :param prefix: The vocabulary prefix to search within.
        :param kind: Which embedding items to search (ALIAS or DEFINITION).
        :param limit: The top number of items to return.
        :return: A list of (concept_id, item_text, score) tuples, best match first.
        """
        results: list[tuple[str, str, float]] = []

        async for concept_id, text, score in self.search_items_iter(
            query_vector=query_vector,
            prefix=prefix,
            kind=kind,
            limit=limit,
        ):
            results.append((concept_id, text, score))

        return results

    @abstractmethod
    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all embedding items for a given prefix from the vector database.
        :param prefix: The vocabulary prefix to delete embedding items for.
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

    if CONFIG.vector_database_driver == VectorDatabaseDriverType.MONGODB:
        from pymongo import AsyncMongoClient
        from .mongo_vector_db import MongoVectorDatabase

        mongo_client = AsyncMongoClient(
            host=CONFIG.mongodb_host,
            port=CONFIG.mongodb_port,
            username=CONFIG.mongodb_username,
            password=CONFIG.mongodb_password,
            authSource=CONFIG.mongodb_auth_source,
        )

        MongoVectorDatabase.set_client(mongo_client)

        _active_vector_db = MongoVectorDatabase()

        return _active_vector_db

    if CONFIG.vector_database_driver == VectorDatabaseDriverType.POSTGRESQL:
        from sqlalchemy.ext.asyncio import create_async_engine
        from .postgres_vector_db import PostgresVectorDatabase

        pg_engine = create_async_engine(CONFIG.postgres_vector_db_url)
        PostgresVectorDatabase.set_engine(pg_engine)

        _active_vector_db = PostgresVectorDatabase()

        return _active_vector_db

    raise ValueError(f'Unsupported vector database driver: {CONFIG.vector_database_driver}')
