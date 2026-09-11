"""
MongoDB implementation of the DocumentDatabase interface.
"""
import asyncio
import re
import time
from uuid import UUID
from concurrent.futures import ProcessPoolExecutor
from typing import AsyncIterator, Optional
from bson import ObjectId
import pymongo
from pymongo import AsyncMongoClient, UpdateOne
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import OperationFailure
from pymongo.operations import SearchIndexModel

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.etc.utils import batch_iterable, iter_progress
from bioterms.etc.metrics import DOCDB_OP_DURATION, DOCDB_OP_TTFI, DOCDB_OP_ERRORS, \
    AUTOCOMPLETE_ITEMS
from bioterms.model.concept import Concept, ConceptUnion
from bioterms.model.user import UserApiKey, User, UserRepository
from .doc_db import DocumentDatabase
from .utils import generate_extra_data


class MongoUserRepository(UserRepository):
    """
    A MongoDB implementation of the UserRepository interface.
    """

    def __init__(self,
                 db: AsyncDatabase,
                 ):
        """
        Initialise the MongoUserRepository with a MongoDB database instance.
        :param db: The MongoDB database instance.
        """
        self._collection = db['users']

    async def get(self, username: str) -> User | None:
        """
        Retrieve a user by their username.
        :param username: The username of the user to retrieve.
        :return: User object or None if not found.
        """
        document = await self._collection.find_one({'username': username}, {'_id': 0})

        if document:
            return User.model_validate(document)

        return None

    async def filter(self) -> list[User]:
        """
        Get a list of all User entities.
        :return: A list of User instances.
        """
        cursor = self._collection.find({}, {'_id': 0})
        users = []
        async for document in cursor:
            users.append(User.model_validate(document))
        return users

    async def save(self, user: User):
        """
        Save a User entity to the database.
        :param user: An instance of User to be saved.
        """
        data = user.model_dump(exclude_none=True)
        await self._collection.update_one(
            {'username': user.username},
            {'$set': data},
            upsert=True
        )

    async def update(self, user: User):
        """
        Update an existing User entity in the database.
        :param user: An instance of User to be updated.
        """
        data = user.model_dump(exclude_none=True)
        await self._collection.update_one(
            {'username': user.username},
            {'$set': data}
        )

    async def delete(self, username: str):
        """
        Delete a User entity from the database.
        :param username: The username of the user to be deleted.
        """
        await self._collection.delete_one({'username': username})

    async def save_api_key(self,
                           username: str,
                           api_key: UserApiKey,
                           ):
        """
        Save an API key for a user.
        :param username: The username of the user to associate the API key with.
        :param api_key: The UserApiKey instance to be saved.
        """
        data = api_key.model_dump()

        await self._collection.update_one(
            {'username': username},
            {'$push': {'apiKeys': data}}
        )

    async def delete_api_key(self,
                             username: str,
                             key_id: UUID,
                             ):
        """
        Delete an API key for a user.
        :param username: The username of the user to disassociate the API key from.
        :param key_id: The UUID of the API key to be deleted.
        """
        await self._collection.update_one(
            {'username': username},
            {'$pull': {'apiKeys': {'keyId': str(key_id)}}}
        )

    async def get_user_by_api_key(self,
                                  key_hash: str,
                                  ) -> User | None:
        """
        Retrieve a user by their API key hash.
        :param key_hash: The HMAC-SHA-256 hashed value of the API key.
        :return: User object or None if not found.
        """
        document = await self._collection.find_one(
            {'apiKeys.keyHash': key_hash},
            {'_id': 0}
        )

        if document:
            return User.model_validate(document)

        return None


class MongoDocumentDatabase(DocumentDatabase):
    """
    A MongoDB implementation of the DocumentDatabase interface.

    Auto-complete substring search prefers a native Atlas Search/mongot `$search` autocomplete
    index (an nGram-tokenised index, mirroring `Concept.n_grams()`'s own 3-20 character range,
    built directly on the "conceptId"/"label"/"synonyms" document fields) over the legacy
    "nGrams" array field approach, when the connected deployment actually has Atlas
    Search/mongot support -- this is not guaranteed just because the document database driver
    is "mongo" (unlike BTS_VECTOR_DATABASE_DRIVER=mongodb, which is an explicit opt-in that
    implies mongot is present); plain community MongoDB without the `mongodb-search` compose
    profile does not have it. Support is probed once per instance (see
    `_supports_native_text_search`) and cached for the instance's lifetime, so a deployment
    that gains/loses Search support needs a service restart to be picked up.
    """

    _TEXT_INDEX_MIN_GRAMS = 3
    _TEXT_INDEX_MAX_GRAMS = 20

    _client: AsyncMongoClient = None

    def __init__(self,
                 client: AsyncMongoClient = None,
                 ):
        """
        Initialise the MongoDatabase with an AsyncMongoClient instance.
        :param client: AsyncMongoClient instance.
        """
        if client is not None:
            self._client = client

        self._native_search_supported: Optional[bool] = None

    @property
    def db(self):
        """
        Get the MongoDB database instance.
        :return: The MongoDB database instance.
        """
        if self._client is None:
            raise ValueError('MongoDB client is not set. Call set_client() first.')
        return self._client[CONFIG.mongodb_db_name]

    @classmethod
    def set_client(cls,
                   client: AsyncMongoClient,
                   ):
        """
        Set the MongoDB client for the database.
        :param client: AsyncMongoClient instance.
        """
        cls._client = client

    async def initialize(self):
        """
        Initialise the database driver/connection.

        MongoDB does not need explicit initialisation on the schemas
        """

    async def close(self) -> None:
        """
        Close the MongoDB connection.
        """
        if self._client is not None:
            await self._client.close()
        else:
            raise ValueError('MongoDB client is not set. Cannot close connection.')

    @property
    def users(self) -> MongoUserRepository:
        """
        Get the user repository for managing admin users in the document database.
        :return: UserRepository instance.
        """
        return MongoUserRepository(self.db)

    async def _supports_native_text_search(self,
                                           collection,
                                           ) -> bool:
        """
        Detect (once, cached for this instance's lifetime) whether the connected MongoDB
        deployment has Atlas Search/mongot support, needed for a native `$search` autocomplete
        index. Falls back to the legacy "nGrams" field approach when it does not.
        :param collection: Any collection to probe `list_search_indexes` against -- the
            capability is deployment-wide, not per-collection.
        :return: True if Atlas Search/mongot is available.
        """
        if self._native_search_supported is not None:
            return self._native_search_supported

        try:
            cursor = await collection.list_search_indexes()
            async for _ in cursor:
                pass
            self._native_search_supported = True
        except OperationFailure:
            self._native_search_supported = False

        return self._native_search_supported

    async def _ensure_text_index(self,
                                 collection,
                                 ):
        """
        Ensure a `$search` autocomplete index exists on "conceptId"/"label"/"synonyms",
        creating it if necessary. Newly created indexes are built asynchronously by mongot, so
        they may not be immediately queryable (mirrors `MongoVectorDatabase._ensure_vector_index`).

        This always checks the server rather than caching the result, since vocabulary reloads
        drop and recreate the underlying collection (and, with it, any search index), and this
        instance's lifetime can span multiple such reloads.
        :param collection: The collection to ensure the index for.
        """
        index_name = CONFIG.mongodb_text_index_name

        existing_indexes = await collection.list_search_indexes(name=index_name)
        if not [idx async for idx in existing_indexes]:
            field_mapping = {
                'type': 'autocomplete',
                'tokenization': 'nGram',
                'minGrams': self._TEXT_INDEX_MIN_GRAMS,
                'maxGrams': self._TEXT_INDEX_MAX_GRAMS,
                'foldDiacritics': False,
            }
            await collection.create_search_index(
                SearchIndexModel(
                    definition={
                        'mappings': {
                            'dynamic': False,
                            'fields': {
                                'conceptId': field_mapping,
                                'label': field_mapping,
                                'synonyms': field_mapping,
                            },
                        },
                    },
                    name=index_name,
                    type='search',
                )
            )

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
        collection = self.db[str(prefix.value)]
        index_name = f'{field}_index'

        # Ensure the collection exists
        collections = await self.db.list_collection_names()
        if prefix.value not in collections:
            await self.db.create_collection(str(prefix.value))

        # Ensure the auto-complete search index exists: a native `$search` autocomplete index
        # when this deployment supports it, otherwise the legacy "nGrams" field index.
        if await self._supports_native_text_search(collection):
            await self._ensure_text_index(collection)
        else:
            await collection.create_index('nGrams', name='nGrams_index')

        try:
            await collection.create_index(
                field,
                unique=unique,
                name=index_name,
            )
        except OperationFailure as e:
            if e.code in (85, 86) and overwrite:
                # Index already exists, drop and recreate

                await collection.drop_index(index_name)
                await collection.create_index(
                    field,
                    unique=unique,
                    name=index_name,
                )
            else:
                raise IndexCreationError(
                    f'Failed to create index on {prefix.value}.{field}: {e}'
                ) from e

    async def delete_index(self,
                           prefix: ConceptPrefix,
                           field: str,
                           ):
        """
        Delete an index on a specified field in the document database.
        :param prefix: The vocabulary prefix to delete the index for.
        :param field: The field to delete the index on.
        """
        await self.db[str(prefix.value)].drop_index(f'{field}_index')

    @staticmethod
    async def _insert_new_terms_batch(collection,
                                      batch: list[Concept],
                                      extra_data: Optional[list] = None,
                                      ):
        """
        Insert a batch of concepts as new documents, without checking for existing duplicates.
        :param collection: The MongoDB collection to insert into.
        :param batch: The batch of Concept instances to insert.
        :param extra_data: The (concept_id, ngrams, search_text) tuples generated for this
            batch, or None when a native search index is in use and no nGrams/searchText need
            to be computed/stored at all.
        """
        new_docs = {
            c.concept_id: c.model_dump(exclude_none=True)
            for c in batch
        }

        if extra_data:
            for concept_id, ngrams, search_text in extra_data:
                if concept_id in new_docs:
                    new_docs[concept_id]['nGrams'] = ngrams
                    new_docs[concept_id]['searchText'] = search_text

        if new_docs:
            await collection.insert_many(new_docs.values())

    @staticmethod
    async def _upsert_terms_batch(collection,
                                  batch: list[Concept],
                                  extra_data: Optional[list],
                                  existing_concept_ids: set[str],
                                  ):
        """
        Insert new concepts and update existing ones in a single batch.
        :param collection: The MongoDB collection to write to.
        :param batch: The batch of Concept instances to save.
        :param extra_data: The (concept_id, ngrams, search_text) tuples generated for this
            batch, or None when a native search index is in use and no nGrams/searchText need
            to be computed/stored at all -- any stale nGrams/searchText field left over on an
            already-existing document (e.g. from before native search was available) is
            unset in that case, so documents don't keep the old side data forever between
            full vocabulary reloads.
        :param existing_concept_ids: The set of concept IDs already present in the collection.
        """
        existing_docs: dict[str, dict] = {}
        new_docs: dict[str, dict] = {}

        for c in batch:
            c_doc = c.model_dump(exclude_none=True)
            if c.concept_id in existing_concept_ids:
                # Remove the immutable fields for faster updates
                c_doc.pop('conceptId', None)
                c_doc.pop('prefix', None)
                existing_docs[c.concept_id] = c_doc
            else:
                new_docs[c.concept_id] = c_doc

        if extra_data:
            for concept_id, ngrams, search_text in extra_data:
                if concept_id in existing_docs:
                    existing_docs[concept_id]['nGrams'] = ngrams
                    existing_docs[concept_id]['searchText'] = search_text
                elif concept_id in new_docs:
                    new_docs[concept_id]['nGrams'] = ngrams
                    new_docs[concept_id]['searchText'] = search_text

        if new_docs:
            await collection.insert_many(new_docs.values())

        if existing_docs:
            update_extra = {} if extra_data else {'$unset': {'nGrams': '', 'searchText': ''}}
            operations = [
                UpdateOne(
                    {'conceptId': concept_id},
                    {'$set': doc, **update_extra}
                ) for concept_id, doc in existing_docs.items()
            ]
            await collection.bulk_write(operations)

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
        collection = self.db[f'{terms[0].prefix.value}']
        existing_concept_ids: set[str] = set()

        if not no_upsert:
            result = collection.find({}, {'conceptId': 1})
            async for doc in result:
                existing_concept_ids.add(doc['conceptId'])

        native = await self._supports_native_text_search(collection)

        with pymongo.timeout(None):
            if native:
                # No nGrams/searchText to compute or store: the native `$search` autocomplete
                # index is built directly from "conceptId"/"label"/"synonyms", which are
                # already part of the document. Skipping `generate_extra_data` here also
                # avoids spinning up a ProcessPoolExecutor for work that is no longer needed.
                for batch in batch_iterable(terms):
                    if no_upsert:
                        await self._insert_new_terms_batch(collection, batch)
                    else:
                        await self._upsert_terms_batch(collection, batch, None, existing_concept_ids)
            else:
                with ProcessPoolExecutor(
                    max_workers=CONFIG.process_limit,
                ) as executor:
                    for batch in batch_iterable(terms):
                        extra_data = await generate_extra_data(
                            concepts=batch,
                            executor=executor,
                        )

                        if no_upsert:
                            await self._insert_new_terms_batch(collection, batch, extra_data)
                        else:
                            await self._upsert_terms_batch(collection, batch, extra_data, existing_concept_ids)

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of terms for a given prefix in the document database.
        :param prefix: The vocabulary prefix to count documents for.
        :return: The number of terms/documents
        """
        collection = self.db[str(prefix.value)]
        count = await collection.count_documents({})
        return count

    async def _iter_page_docs(self,
                              collection,
                              query: dict,
                              page_size: int,
                              ) -> AsyncIterator[dict]:
        """
        Yield the raw documents for a single page of a paginated collection scan.
        :param collection: The MongoDB collection to query.
        :param query: The query filter, already scoped to the current pagination cursor.
        :param page_size: The maximum number of documents to fetch in this page.
        :return: An async iterator of raw MongoDB documents.
        """
        async with self._client.start_session() as session:
            cursor = collection.find(
                query,
                {'nGrams': 0, 'searchText': 0},
                session=session,
                no_cursor_timeout=True,
            ).sort('_id', 1).limit(page_size)

            try:
                async for doc in cursor:
                    yield doc
            finally:
                await cursor.close()

    async def get_terms_iter(self,
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
        collection = self.db[str(prefix.value)]

        page_size = 5000
        remaining = limit if limit and limit > 0 else None
        last_id: ObjectId | None = None

        while True:
            if remaining is not None and remaining <= 0:
                return

            this_page = page_size if remaining is None else min(page_size, remaining)
            query = {} if last_id is None else {'_id': {'$gt': last_id}}

            yielded_any = False
            async for doc in self._iter_page_docs(collection, query, this_page):
                yielded_any = True
                last_id = doc['_id']
                doc.pop('_id', None)
                yield model_class.model_validate(doc)

                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            if not yielded_any:
                return

    async def get_terms_by_ids_iter(self,
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
        start = time.perf_counter()
        first_item_at = None
        result_label = 'ok'

        try:
            collection = self.db[str(prefix.value)]
            cursor = collection.find(
                {'conceptId': {'$in': concept_ids}},
                {'_id': 0, 'nGrams': 0, 'searchText': 0}
            )

            async for doc in cursor:
                if first_item_at is None:
                    first_item_at = time.perf_counter()
                yield model_class.model_validate(doc)
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            DOCDB_OP_ERRORS.labels(
                backend='mongo',
                op='get_terms_by_ids',
                prefix=prefix.value,
                error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            DOCDB_OP_DURATION.labels(
                backend='mongo',
                op='get_terms_by_ids',
                prefix=prefix.value,
                result=result_label,
            ).observe(end - start)

            if first_item_at is not None:
                DOCDB_OP_TTFI.labels(
                    backend='mongo',
                    op='get_terms_by_ids',
                    prefix=prefix.value,
                    result=result_label,
                ).observe(first_item_at - start)

    async def delete_all_for_label(self,
                                   prefix: ConceptPrefix,
                                   ):
        """
        Delete all documents/records for a given label in the document database.
        :param prefix: The vocabulary prefix to delete documents for.
        """
        # Drop the collection directly to avoid index and performance issues
        await self.db.drop_collection(str(prefix.value))

        # Recreate the collection to ensure it exists
        await self.db.create_collection(str(prefix.value))

    async def update_vector_mapping(self,
                                    prefix: ConceptPrefix,
                                    mapping: dict[str, str],
                                    ):
        """
        Update the vector mapping for concepts in the document database.
        :param prefix: The vocabulary prefix to update the vector mapping for.
        :param mapping: A dictionary mapping concept IDs to vector IDs.
        """
        collection = self.db[str(prefix.value)]

        # Batch update with default overwrite behaviour
        operations = []
        for concept_id, vector_id in iter_progress(
            mapping.items(),
            description='Updating vector mappings',
            total=len(mapping),
        ):
            operations.append(
                UpdateOne(
                    {'conceptId': concept_id},
                    {'$set': {'vectorId': vector_id}}
                )
            )

            if len(operations) >= 1000:
                await collection.bulk_write(operations)
                operations = []

        if operations:
            await collection.bulk_write(operations)

    @staticmethod
    def _build_legacy_auto_complete_pipeline(n_gram_query: list[str],
                                             score_query: str,
                                             limit: int | None,
                                             ) -> list[dict]:
        """
        Build the fallback aggregation pipeline matching against the pre-generated "nGrams"
        field, used when this deployment has no Atlas Search/mongot support.
        :param n_gram_query: The lowercased, whitespace-split query words (each len > 2).
        :param score_query: The whitespace-stripped lowercased full query.
        :param limit: The maximum number of results to return, or None for no limit.
        :return: The aggregation pipeline.
        """
        pipeline: list[dict] = [
            # Match on the n-gram
            {
                '$match': {
                    'nGrams': {
                        '$all': n_gram_query
                    }
                }
            },
            # Calculate the scores
            {
                '$addFields': {
                    'score': {
                        '$indexOfBytes': ['$searchText', score_query]
                    },
                    'labelLength': {
                        '$cond': {
                            'if': {'$gt': [{'$type': '$label'}, 'null']},
                            'then': {'$strLenCP': '$label'},
                            'else': 999,
                        }
                    }
                }
            },
            # Rank based on the scores
            {
                '$sort': {
                    'score': 1,
                    'labelLength': 1,
                    'termId': 1,
                },
            },
            # Remove the intermediate fields
            {
                '$project': {
                    'score': 0,
                    'labelLength': 0,
                    '_id': 0,
                },
            },
        ]

        if limit is not None:
            pipeline.append({'$limit': limit})

        pipeline.append({
            '$project': {
                'nGrams': 0,
                'searchText': 0,
            },
        })

        return pipeline

    def _build_native_auto_complete_pipeline(self,
                                             n_gram_query: list[str],
                                             limit: int | None,
                                             ) -> list[dict]:
        """
        Build the `$search` aggregation pipeline used when this deployment has Atlas
        Search/mongot support: each query word must autocomplete-match somewhere in
        "conceptId"/"label"/"synonyms" (AND across words, OR across fields per word), ranked
        by mongot's own relevance score, then by shorter label first, then by concept ID for
        determinism. Unlike the legacy path, no position-based score is computed by hand.
        :param n_gram_query: The lowercased, whitespace-split query words (each len > 2).
        :param limit: The maximum number of results to return, or None for no limit.
        :return: The aggregation pipeline.
        """
        pipeline: list[dict] = [
            {
                '$search': {
                    'index': CONFIG.mongodb_text_index_name,
                    'compound': {
                        'must': [
                            {
                                'autocomplete': {
                                    'query': word,
                                    'path': ['conceptId', 'label', 'synonyms'],
                                }
                            }
                            for word in n_gram_query
                        ],
                    },
                },
            },
            {
                '$addFields': {
                    'searchScore': {'$meta': 'searchScore'},
                    'labelLength': {
                        '$cond': {
                            'if': {'$gt': [{'$type': '$label'}, 'null']},
                            'then': {'$strLenCP': '$label'},
                            'else': 999,
                        }
                    }
                }
            },
            {
                '$sort': {
                    'searchScore': -1,
                    'labelLength': 1,
                    'conceptId': 1,
                },
            },
            {
                '$project': {
                    'searchScore': 0,
                    'labelLength': 0,
                    '_id': 0,
                    # Defensive: only relevant for documents left over from before native
                    # search was adopted (or its capability re-detected) that haven't gone
                    # through a full vocabulary reload since -- new writes in native mode never
                    # set these fields to begin with.
                    'nGrams': 0,
                    'searchText': 0,
                },
            },
        ]

        if limit is not None:
            pipeline.append({'$limit': limit})

        return pipeline

    async def auto_complete_iter(self,
                                 prefix: ConceptPrefix,
                                 query: str,
                                 limit: int = None,
                                 model_class: type[Concept] = Concept,
                                 ) -> AsyncIterator[ConceptUnion]:
        """
        Run an auto-complete search query against the document database and return an async iterator.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The maximum number of results to return. If None, return all matches.
        :param model_class: The Concept subclass to instantiate for results.
        :return: An asynchronous iterator yielding Concept instances matching the auto-complete query.
        """
        start = time.perf_counter()
        first_item_at = None
        items = 0
        result_label = 'ok'

        clean_query = re.sub(r'[()"\']', '', query.lower())

        # N-gram query is used to match the pre-generated n-grams (legacy path) or as the
        # autocomplete operator's search terms (native path), while score query is only used
        # to rank the already matched documents in the legacy path.
        n_gram_query = [word for word in clean_query.split() if len(word) > 2]
        score_query = re.sub(r'\s', '', clean_query)

        collection = self.db[str(prefix.value)]
        native = await self._supports_native_text_search(collection)

        if native:
            await self._ensure_text_index(collection)
            pipeline = self._build_native_auto_complete_pipeline(n_gram_query, limit)
        else:
            pipeline = self._build_legacy_auto_complete_pipeline(n_gram_query, score_query, limit)

        try:
            cursor = await collection.aggregate(pipeline)

            async for doc in cursor:
                if first_item_at is None:
                    first_item_at = time.perf_counter()
                items += 1
                yield model_class.model_validate(doc)
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            DOCDB_OP_ERRORS.labels(
                backend='mongo',
                op='auto_complete',
                prefix=prefix.value,
                error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            DOCDB_OP_DURATION.labels(
                backend='mongo',
                op='auto_complete',
                prefix=prefix.value,
                result=result_label,
            ).observe(end - start)

            if first_item_at is not None:
                DOCDB_OP_TTFI.labels(
                    backend='mongo',
                    op='auto_complete',
                    prefix=prefix.value,
                    result=result_label,
                ).observe(first_item_at - start)

            AUTOCOMPLETE_ITEMS.labels(prefix=str(prefix.value)).observe(items)

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
        collection = self.db[str(prefix.value)]
        pipeline = [
            {'$sample': {'size': count}},
            {'$project': {'conceptId': 1}},
        ]

        term_ids = []
        cursor = await collection.aggregate(pipeline)
        async for doc in cursor:
            term_ids.append(doc['conceptId'])

        return term_ids
