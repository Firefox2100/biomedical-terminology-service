import re
from uuid import UUID
from typing import AsyncIterator
from pymongo import AsyncMongoClient, UpdateOne
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import OperationFailure

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.model.concept import Concept
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


class MongoDocumentDatabase(DocumentDatabase):
    """
    A MongoDB implementation of the DocumentDatabase interface.
    """

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

        # Always create the default nGram index if they don't exist
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
                raise IndexCreationError(f'Failed to create index on {prefix.value}.{field}: {e}') from e

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

    async def save_terms(self,
                         terms: list[Concept],
                         ):
        """
        Save a list of terms into the mongodb.
        :param terms: A list of Concept instances to save.
        """
        extra_data = await generate_extra_data(terms)

        documents: dict[ConceptPrefix, list[dict]] = {}
        for concept, (term_id, ngrams, search_text) in zip(terms, extra_data):
            doc = concept.model_dump(exclude_none=True)
            doc['nGrams'] = ngrams
            doc['searchText'] = search_text

            if concept.prefix not in documents:
                documents[concept.prefix] = []

            documents[concept.prefix].append(doc)

        for prefix, concept_docs in documents.items():
            collection = self.db[str(prefix.value)]

            await collection.insert_many(concept_docs, ordered=False)

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

    async def get_terms_iter(self,
                             prefix: ConceptPrefix,
                             limit: int = 0,
                             ) -> AsyncIterator[Concept]:
        """
        Get an asynchronous iterator over all items for a given prefix in the document database.
        :param prefix: The vocabulary prefix to get documents for.
        :param limit: The maximum number of documents to retrieve. If 0, retrieve all documents.
        :return: An asynchronous iterator yielding Concept instances.
        """
        collection = self.db[str(prefix.value)]
        cursor = collection.find(
            {},
            {'_id': 0, 'nGrams': 0, 'searchText': 0}
        ).limit(limit if limit > 0 else 0)

        async for doc in cursor:
            yield Concept.model_validate(doc)

    async def get_terms_by_ids_iter(self,
                                    prefix: ConceptPrefix,
                                    concept_ids: list[str],
                                    ) -> AsyncIterator[Concept]:
        """
        Get terms by their IDs for a given prefix in the document database as an async iterator.
        :param prefix: The vocabulary prefix to get documents for.
        :param concept_ids: A list of concept IDs to retrieve.
        :return: An asynchronous iterator yielding Concept instances.
        """
        collection = self.db[str(prefix.value)]
        cursor = collection.find(
            {'conceptId': {'$in': concept_ids}},
            {'_id': 0, 'nGrams': 0, 'searchText': 0}
        )

        async for doc in cursor:
            yield Concept.model_validate(doc)

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
        for concept_id, vector_id in mapping.items():
            operations.append(
                UpdateOne(
                    {'conceptId': concept_id},
                    {'$set': {'vectorId': vector_id}}
                )
            )

        if operations:
            await collection.bulk_write(operations)

    async def auto_complete_iter(self,
                                 prefix: ConceptPrefix,
                                 query: str,
                                 limit: int = None,
                                 ) -> AsyncIterator[Concept]:
        """
        Run an auto-complete search query against the document database and return an async iterator.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The maximum number of results to return. If None, return all matches.
        :return: An asynchronous iterator yielding Concept instances matching the auto-complete query.
        """
        clean_query = re.sub(r'[()"\']', '', query.lower())

        # N-gram query is used to match the pre-generated n-grams, while
        # score query is only used to rank the already matched documents
        n_gram_query = [word for word in clean_query.split() if len(word) > 2]
        score_query = re.sub(r'\s', '', clean_query)

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
            pipeline.append(
                {
                    '$limit': limit,
                }
            )

        final_projection = {
            '$project': {
                'nGrams': 0,
                'searchText': 0,
            },
        }

        pipeline.append(final_projection)
        collection = self.db[str(prefix.value)]

        cursor = await collection.aggregate(pipeline)

        async for doc in cursor:
            yield Concept.model_validate(doc)
