from abc import ABC, abstractmethod
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import GraphDatabaseDriverType, ConceptPrefix
from bioterms.model.concept import Concept


class GraphDatabase(ABC):
    """
    An interface for operating on the graph database.

    This service uses two primary databases:

    - One graph database for relationships between terms.
    - One document database for term details and metadata.

    This database interface focuses on the graph database operations.
    """

    @abstractmethod
    async def close(self):
        """
        Close the database driver/connection.
        """

    @abstractmethod
    async def save_vocabulary_graph(self,
                                    concepts: list[Concept],
                                    graph: nx.DiGraph,
                                    ):
        """
        Save the vocabulary graph to the graph database.
        :param concepts: The list of concepts to save. This list is passed in to
            allow for any necessary term metadata to be accessed during graph saving.
        :param graph: The vocabulary graph to save.
        """

    @abstractmethod
    async def delete_vocabulary_graph(self,
                                      prefix: ConceptPrefix,
                                      ):
        """
        Delete the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to delete.
        """

    @abstractmethod
    async def create_index(self):
        """
        Create indexes in the graph database.
        """


_active_graph_db: GraphDatabase | None = None


def get_active_graph_db() -> GraphDatabase:
    """
    Get the active graph database instance based on the configuration.
    :return: The active GraphDatabase instance.
    """
    global _active_graph_db

    if _active_graph_db is not None:
        return _active_graph_db

    if CONFIG.graph_database_driver == GraphDatabaseDriverType.NEO4J:
        from neo4j import AsyncGraphDatabase
        from .neo4j_graph_db import Neo4jGraphDatabase

        neo4j_client = AsyncGraphDatabase.driver(
            uri=CONFIG.neo4j_uri,
            auth=(CONFIG.neo4j_username, CONFIG.neo4j_password),
            database=CONFIG.neo4j_db_name,
        )

        Neo4jGraphDatabase.set_client(neo4j_client)

        _active_graph_db = Neo4jGraphDatabase()
        return _active_graph_db

    raise ValueError(
        f'Unsupported graph database driver type: {CONFIG.graph_db.driver_type}'
    )
