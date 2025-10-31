from abc import ABC, abstractmethod

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import DocDatabaseDriverType, ConceptPrefix
from bioterms.model.concept import Concept


class DocumentDatabase(ABC):
    """
    An interface for operating on the document database.

    This service uses two primary databases:

    - One graph database for relationships between terms.
    - One document database for term details and metadata.

    This database interface focuses on the document database operations.
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
                         ):
        """
        Save a list of terms into the document database.
        :param terms: A list of Concept instances to save.
        """

    @abstractmethod
    async def delete_all_for_label(self,
                                   prefix: ConceptPrefix,
                                   ):
        """
        Delete all documents/records for a given label in the document database.
        :param prefix: The vocabulary prefix to delete documents for.
        """

    @abstractmethod
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


_active_doc_db: DocumentDatabase | None = None


def get_active_doc_db() -> DocumentDatabase:
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
        )

        MongoDocumentDatabase.set_client(mongo_client)

        _active_doc_db = MongoDocumentDatabase(mongo_client)

        return _active_doc_db

    raise ValueError(
        f'Unsupported document database driver: {CONFIG.doc_database_driver}'
    )
