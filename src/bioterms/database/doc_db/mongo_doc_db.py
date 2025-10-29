import asyncio
from pymongo import AsyncMongoClient

from bioterms.etc.consts import CONFIG, EXECUTOR
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

    async def save_terms(self,
                         label: str,
                         terms: list[Concept],
                         ):
        """
        Save a list of terms into the mongodb.
        :param label: A label to save the terms under. It Will be used as a collection name.
        :param terms: A list of Concept instances to save.
        """
        collection = self.db[label]

        extra_data = await _generate_extra_data(terms)

        documents = []
        for concept, (term_id, ngrams, search_text) in zip(terms, extra_data):
            doc = concept.model_dump()
            doc['nGrams'] = ngrams
            doc['searchText'] = search_text
            documents.append(doc)

        await collection.insert_many(documents, ordered=False)
