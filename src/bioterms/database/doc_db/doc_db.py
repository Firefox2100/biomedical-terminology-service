import asyncio
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import AsyncIterator

from bioterms.etc.consts import CONFIG, LOGGER
from bioterms.etc.enums import DocDatabaseDriverType, ConceptPrefix
from bioterms.etc.metrics import DOCDB_OP_DURATION, DOCDB_OP_TTFI, DOCDB_OP_ERRORS, AUTOCOMPLETE_ITEMS
from bioterms.model.concept import Concept, ConceptUnion
from bioterms.model.user import UserRepository


@dataclass(frozen=True)
class SearchQuery:
    clean: str
    words: list[str]
    compact: str


def normalise_search_query(query: str) -> SearchQuery:
    """Normalise user text consistently across document database backends."""
    clean = re.sub(r'[()"\']', '', query.lower())
    words = [word for word in clean.split() if len(word) > 2]
    return SearchQuery(clean=clean, words=words, compact=re.sub(r'\s', '', clean))


class DocumentDatabase(ABC):
    """
    An interface for operating on the document database.

    This service uses two primary databases:

    - One graph database for relationships between terms.
    - One document database for term details and metadata.

    This database interface focuses on the document database operations.
    """

    @property
    @abstractmethod
    def users(self) -> UserRepository:
        """
        Get the user repository for managing admin users in the document database.
        :return: UserRepository instance.
        """

    @abstractmethod
    async def initialize(self):
        """
        Initialise the database driver/connection.
        """

    @abstractmethod
    async def close(self):
        """
        Close the database driver/connection.
        """

    @abstractmethod
    async def create_index(self,
                           prefix: ConceptPrefix,
                           field: str,
                           unique: bool = False,
                           overwrite: bool = False,
                           ):
        """
        Create an index on a specified field in the document database.
        :param prefix: The vocabulary prefix to create the index for.
        :param field: The field to index.
        :param unique: Whether the index should enforce uniqueness.
        :param overwrite: Whether to overwrite an existing index. If False, creating
            an index with the same field may raise an error, depending on the database
            implementation.
        :raises IndexCreationError: If there is an error creating the index.
        """

    @abstractmethod
    async def delete_index(self,
                           prefix: ConceptPrefix,
                           field: str,
                           ):
        """
        Delete an index on a specified field in the document database.
        :param prefix: The vocabulary prefix to delete the index for.
        :param field: The field to delete the index on.
        """

    @abstractmethod
    async def save_terms(self,
                         terms: list[Concept],
                         no_upsert: bool = False
                         ):
        """
        Save a list of terms into the document database.
        :param terms: A list of Concept instances to save.
        :param no_upsert: Force direct insert. The caller must ensure that there is no existing data that
            may be a duplicate, or it will fail from the unique index
        """

    @abstractmethod
    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of terms for a given prefix in the document database.
        :param prefix: The vocabulary prefix to count documents for.
        :return: The number of terms/documents
        """

    @abstractmethod
    def get_terms_iter(self,
                       prefix: ConceptPrefix,
                       limit: int = 0,
                       model_class: type[Concept] = Concept,
                       ) -> AsyncIterator[ConceptUnion]:
        """
        Get an asynchronous iterator over all items for a given prefix in the document database.
        :param prefix: The vocabulary prefix to get documents for.
        :param limit: The maximum number of documents to retrieve. If 0, retrieve all documents.
        :param model_class: The Concept subclass to instantiate for results.
        :return: An asynchronous iterator yielding Concept instances.
        """

    async def get_terms(self,
                        prefix: ConceptPrefix,
                        limit: int = 0,
                        model_class: type[Concept] = Concept,
                        ) -> list[ConceptUnion]:
        """
        Get all terms for a given prefix in the document database.
        :param prefix: The vocabulary prefix to get documents for.
        :param limit: The maximum number of documents to retrieve. If 0, retrieve all documents.
        :param model_class: The Concept subclass to instantiate for results.
        :return: A list of Concept instances.
        """
        it = self.get_terms_iter(
            prefix=prefix,
            limit=limit,
            model_class=model_class
        )

        results: list[ConceptUnion] = []
        async for concept in it:
            results.append(concept)

        return results

    @abstractmethod
    def get_terms_by_ids_iter(self,
                              prefix: ConceptPrefix,
                              concept_ids: list[str],
                              model_class: type[Concept] = Concept,
                              ) -> AsyncIterator[ConceptUnion]:
        """
        Get terms by their IDs for a given prefix in the document database as an async iterator.
        :param prefix: The vocabulary prefix to get documents for.
        :param concept_ids: A list of concept IDs to retrieve.
        :param model_class: The Concept subclass to instantiate for results.
        :return: An asynchronous iterator yielding Concept instances.
        """

    async def get_terms_by_ids(self,
                               prefix: ConceptPrefix,
                               concept_ids: list[str],
                               model_class: type[Concept] = Concept,
                               ) -> list[ConceptUnion]:
        """
        Get terms by their IDs for a given prefix in the document database.
        :param prefix: The vocabulary prefix to get documents for.
        :param concept_ids: A list of concept IDs to retrieve.
        :param model_class: The Concept subclass to instantiate for results.
        :return: A list of Concept instances.
        """
        it = self.get_terms_by_ids_iter(
            prefix=prefix,
            concept_ids=concept_ids,
            model_class=model_class
        )

        results: list[ConceptUnion] = []
        async for concept in it:
            results.append(concept)

        return results

    @abstractmethod
    async def delete_all_for_label(self,
                                   prefix: ConceptPrefix,
                                   ):
        """
        Delete all documents/records for a given label in the document database.
        :param prefix: The vocabulary prefix to delete documents for.
        """

    @abstractmethod
    def lexical_search_iter(self,
                            prefix: ConceptPrefix,
                            query: str,
                            limit: int = 10,
                            ) -> AsyncIterator[tuple[str, float]]:
        """
        Run a scored lexical/keyword search (BM25 or native full-text ranking where the
        backend supports it, an n-gram-overlap count otherwise) against a vocabulary's
        concept_id/label/synonyms text (the same text `auto_complete_iter` matches against --
        definitions are covered separately by the definition-embedding recall arm, not here),
        and return matching concept IDs ranked best-first. This is the lexical recall arm fed
        into the RRF fusion in
        `bioterms.search.hybrid` -- unlike `auto_complete_iter`, it is meant for relevance
        ranking of a full query rather than substring/prefix completion, so it does not
        require every query word to match.

        The returned score's absolute scale is backend-specific and not comparable across
        prefixes or drivers -- only its ordering within this one call matters, since RRF
        fuses recall lists by rank, not by score magnitude.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The top number of concepts to return.
        :return: An async iterator of (concept_id, score) tuples, best match first.
        """

    async def lexical_search(self,
                             prefix: ConceptPrefix,
                             query: str,
                             limit: int = 10,
                             ) -> list[tuple[str, float]]:
        """
        Run a scored lexical/keyword search and return matching concept IDs ranked best-first.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The top number of concepts to return.
        :return: A list of (concept_id, score) tuples, best match first.
        """
        results: list[tuple[str, float]] = []

        async for concept_id, score in self.lexical_search_iter(prefix=prefix, query=query, limit=limit):
            results.append((concept_id, score))

        return results

    _backend_name = 'unknown'

    @abstractmethod
    def _auto_complete_iter(self,
                            prefix: ConceptPrefix,
                            search_query: SearchQuery,
                            limit: int,
                            model_class: type[Concept],
                            ) -> AsyncIterator[ConceptUnion]:
        """Execute backend-specific autocomplete for a normalized query."""

    async def auto_complete_iter(self,
                                 prefix: ConceptPrefix,
                                 query: str,
                                 limit: int = None,
                                 model_class: type[Concept] = Concept,
                                 ) -> AsyncIterator[ConceptUnion]:
        """Stream autocomplete results and record backend-independent metrics."""
        search_query = normalise_search_query(query)
        if not search_query.words:
            return

        start = time.perf_counter()
        first_item_at = None
        items = 0
        result_label = 'ok'

        try:
            async for concept in self._auto_complete_iter(prefix, search_query, limit, model_class):
                if first_item_at is None:
                    first_item_at = time.perf_counter()
                items += 1
                yield concept
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as exc:
            result_label = 'error'
            DOCDB_OP_ERRORS.labels(
                backend=self._backend_name,
                op='auto_complete',
                prefix=prefix.value,
                error_type=type(exc).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            DOCDB_OP_DURATION.labels(
                backend=self._backend_name,
                op='auto_complete',
                prefix=prefix.value,
                result=result_label,
            ).observe(end - start)
            if first_item_at is not None:
                DOCDB_OP_TTFI.labels(
                    backend=self._backend_name,
                    op='auto_complete',
                    prefix=prefix.value,
                    result=result_label,
                ).observe(first_item_at - start)
            AUTOCOMPLETE_ITEMS.labels(prefix=str(prefix.value)).observe(items)

    async def auto_complete_search(self,
                                   prefix: ConceptPrefix,
                                   query: str,
                                   limit: int = None,
                                   model_class: type[Concept] = Concept,
                                   ) -> list[ConceptUnion]:
        """
        Run an auto-complete search query against the document database.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The maximum number of results to return. If None, return all matches.
        :param model_class: The Concept subclass to instantiate for results.
        :return: A list of Concept instances matching the auto-complete query.
        """
        it = self.auto_complete_iter(
            prefix=prefix,
            query=query,
            limit=limit,
            model_class=model_class
        )

        results: list[ConceptUnion] = []
        async for concept in it:
            results.append(concept)

        return results

    @abstractmethod
    async def get_random_term_ids(self,
                                  prefix: ConceptPrefix,
                                  count: int,
                                  ) -> list[str]:
        """
        Get a list of random term IDs for a given prefix from the document database.
        :param prefix: The vocabulary prefix to get random term IDs for.
        :param count: The number of random term IDs to retrieve.
        :return: A list of random term IDs.
        """


_active_doc_db: DocumentDatabase | None = None


async def get_active_doc_db() -> DocumentDatabase:
    """
    Get the active document database instance based on configuration.
    :return: The active DocumentDatabase instance.
    """
    global _active_doc_db

    if _active_doc_db is not None:
        return _active_doc_db

    if CONFIG.doc_database_driver == DocDatabaseDriverType.MONGO:
        from pymongo import AsyncMongoClient
        from .mongo_doc_db import MongoDocumentDatabase

        mongo_client = AsyncMongoClient(
            host=CONFIG.mongodb_host,
            port=CONFIG.mongodb_port,
            username=CONFIG.mongodb_username,
            password=CONFIG.mongodb_password,
            authSource=CONFIG.mongodb_auth_source,
            directConnection=CONFIG.mongodb_direct_connection,
        )
        await mongo_client.admin.command('ping')

        MongoDocumentDatabase.set_client(mongo_client)

        doc_db = MongoDocumentDatabase()
        await doc_db.initialize()
        _active_doc_db = doc_db
        LOGGER.info('Initialized document database backend: mongodb')

        return _active_doc_db

    if CONFIG.doc_database_driver == DocDatabaseDriverType.SQL:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine
        from .sql_doc_db import SqlDocumentDatabase

        # SQLite's async dialect uses StaticPool/NullPool, neither of which accepts
        # pool_size/max_overflow (create_engine raises TypeError if passed) -- those only
        # apply to QueuePool-backed dialects (PostgreSQL, MySQL).
        pool_kwargs = {} if CONFIG.sql_db_url.startswith('sqlite') else {
            'pool_size': CONFIG.sql_pool_size,
            'max_overflow': CONFIG.sql_max_overflow,
        }

        sql_engine = create_async_engine(
            CONFIG.sql_db_url,
            pool_pre_ping=True,
            future=True,
            **pool_kwargs,
        )

        async with sql_engine.connect() as conn:
            await conn.execute(text('SELECT 1'))

        doc_db = SqlDocumentDatabase(sql_engine, batch_size=CONFIG.sql_batch_size)
        await doc_db.initialize()
        _active_doc_db = doc_db
        LOGGER.info('Initialized document database backend: sql (%s)', sql_engine.dialect.name)

        return _active_doc_db

    if CONFIG.doc_database_driver == DocDatabaseDriverType.ELASTICSEARCH:
        from elasticsearch import AsyncElasticsearch
        from .elasticsearch_doc_db import ElasticsearchDocumentDatabase

        kwargs = {}
        if CONFIG.elasticsearch_api_key:
            kwargs['api_key'] = CONFIG.elasticsearch_api_key
        elif CONFIG.elasticsearch_username:
            kwargs['basic_auth'] = (
                CONFIG.elasticsearch_username, CONFIG.elasticsearch_password or '',
            )
        client = AsyncElasticsearch(CONFIG.elasticsearch_url, **kwargs)
        await client.info()
        doc_db = ElasticsearchDocumentDatabase(client)
        await doc_db.initialize()
        _active_doc_db = doc_db
        LOGGER.info('Initialized document database backend: elasticsearch')
        return _active_doc_db

    raise ValueError(
        f'Unsupported document database driver: {CONFIG.doc_database_driver}'
    )
