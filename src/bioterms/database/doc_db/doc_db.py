from abc import ABC, abstractmethod

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
    async def save_terms(self,
                         label: str,
                         terms: list[Concept],
                         ):
        """
        Save a list of terms into the document database.
        :param label: A label to save the terms under. It Will be used as a collection or table name.
        :param terms: A list of Concept instances to save.
        """


_active_doc_db: DocumentDatabase | None = None


def get_active_doc_db() -> DocumentDatabase:
    """
    Get the active document database instance based on configuration.
    :return: The active DocumentDatabase instance.
    """
