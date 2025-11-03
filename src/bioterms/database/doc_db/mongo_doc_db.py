import asyncio
import re
from pymongo import AsyncMongoClient
from pymongo.errors import OperationFailure

from bioterms.etc.consts import CONFIG, EXECUTOR
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.model.concept import Concept
from .doc_db import DocumentDatabase


def _generate_extra_data_for_term(concept: Concept) -> tuple[str, list[str], str]:
    """
    Generate extra data for search indexing from a Concept object.
    :param concept: A Concept object
    :return: A tuple containing the term's ID, n-grams, and search text.
    """
    ngrams = concept.n_grams()
    search_text = concept.search_text()

    return concept.concept_id, ngrams, search_text


async def _generate_extra_data(concepts: list[Concept]) -> list[tuple[str, list[str], str]]:
    """
    Generate extra data for search indexing from a list of Concept objects.
    :param concepts: A list of Concept objects
    :return: A tuple containing the term's ID, n-grams, and search text.
    """
    loop = asyncio.get_running_loop()

    futures = [
        loop.run_in_executor(EXECUTOR, _generate_extra_data_for_term, concept)
        for concept in concepts
    ]

    return await asyncio.gather(*futures)


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
        collection = self.db[prefix.value]
        index_name = f'{field}_index'

        # Ensure the collection exists
        collections = await self.db.list_collection_names()
        if prefix.value not in collections:
            await self.db.create_collection(prefix.value)

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
        await self.db[prefix.value].drop_index(f'{field}_index')

    async def save_terms(self,
                         terms: list[Concept],
                         ):
        """
        Save a list of terms into the mongodb.
        :param terms: A list of Concept instances to save.
        """
        extra_data = await _generate_extra_data(terms)

        documents: dict[ConceptPrefix, list[dict]] = {}
        for concept, (term_id, ngrams, search_text) in zip(terms, extra_data):
            doc = concept.model_dump()
            doc['nGrams'] = ngrams
            doc['searchText'] = search_text

            if concept.prefix not in documents:
                documents[concept.prefix] = []

            documents[concept.prefix].append(doc)

        for prefix, concept_docs in documents.items():
            collection = self.db[prefix.value]

            await collection.insert_many(concept_docs, ordered=False)

    async def delete_all_for_label(self,
                                   prefix: ConceptPrefix,
                                   ):
        """
        Delete all documents/records for a given label in the document database.
        :param prefix: The vocabulary prefix to delete documents for.
        """
        # Drop the collection directly to avoid index and performance issues
        await self.db.drop_collection(prefix.value)

        # Recreate the collection to ensure it exists
        await self.db.create_collection(prefix.value)

    async def auto_complete_search(self,
                                   prefix: ConceptPrefix,
                                   query: str,
                                   limit: int = None,
                                   ) -> list[Concept]:
        """
        Run an auto-complete search query against the document database.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The maximum number of results to return. If None, return all matches.
        :return: A list of Concept instances matching the auto-complete query.
        """
        clean_query = re.sub(r'[()"\']', '', query.lower())

        # N-gram query is used to match the pre-generated n-grams, while
        # score query is only used to rank the already matched documents
        n_gram_query = [word for word in clean_query.split() if len(word) > 2]
        score_query = re.sub(r'\s', '', clean_query)

        pipeline = [
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
        collection = self.db[prefix.value]

        cursor = await collection.aggregate(pipeline)
        results = await cursor.to_list(length=limit)

        return [Concept.model_validate(doc) for doc in results]
