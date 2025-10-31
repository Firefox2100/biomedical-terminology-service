import asyncio
from typing import LiteralString, cast
import networkx as nx
from neo4j import AsyncDriver, AsyncSession
from neo4j.exceptions import TransientError

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from .graph_db import GraphDatabase


class Neo4jGraphDatabase(GraphDatabase):
    """
    An implementation of the GraphDatabase interface for Neo4j.
    """

    _client: AsyncDriver = None

    def __init__(self,
                 client: AsyncDriver = None,
                 ):
        """
        Initialise the Neo4jGraphDatabase with an AsyncDriver instance.
        :param client: AsyncDriver instance.
        """
        if client is not None:
            self._client = client

    @staticmethod
    async def _execute_query_with_retry(query: LiteralString,
                                        session: AsyncSession,
                                        parameters: dict = None,
                                        backoff_retries: int = 3,
                                        ):
        """
        Execute a Cypher query with retry logic.

        This is in case there are deadlocks or transient errors in the database, which can
        be safely retried.
        :param query: The Cypher query to execute.
        :param parameters: The parameters for the query.
        :param backoff_retries: The number of retries to attempt on failure.
        :return: The result of the query.
        """
        attempt = 0
        backoff_time = 1

        while attempt < backoff_retries:
            try:
                result = await session.run(
                    query,
                    **(parameters or {}),
                )
                return result
            except TransientError as e:
                if attempt < backoff_retries - 1:
                    attempt += 1
                    await asyncio.sleep(backoff_time)
                    backoff_time *= 2
                else:
                    raise e

        raise RuntimeError('Exceeded maximum retry attempts for query execution.')

    @classmethod
    def set_client(cls,
                   client: AsyncDriver,
                   ):
        """
        Set the Neo4J client for the database.
        :param client: AsyncDriver instance.
        """
        cls._client = client

    async def close(self) -> None:
        """
        Close the Neo4J connection.
        """
        if self._client is not None:
            await self._client.close()
        else:
            raise ValueError('Neo4J client is not set. Cannot close connection.')

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
        edges = [
            (str(source), str(target), data.get('label'))
            for source, target, data in graph.edges(data=True)
        ]

        async with self._client.session() as session:
            # Insert the concepts first before adding edges
            await self._execute_query_with_retry(
                query="""
                UNWIND $concepts AS concept
                WITH concept, coalesce(concept.conceptTypes, []) AS types
                MERGE (n:Concept {id: concept.conceptId, prefix: concept.prefix})
                
                WITH n, [t IN types WHERE t IS NOT NULL AND trim(t) <> ""] AS labels
                CALL apoc.create.addLabels(n, labels) YIELD node
                RETURN count(node) AS upserted
                """,
                session=session,
                parameters={
                    'concepts': [concept.model_dump() for concept in concepts],
                },
            )

            # Insert the edges
            await self._execute_query_with_retry(
                query="""
                UNWIND $edges AS edge
                MERGE (source:Concept {id: edge[0]})
                MERGE (target:Concept {id: edge[1]})
                WITH source, target, edge, coalesce(edge[2], 'related_to') as rel_label
                CALL apoc.merge.relationship(source, rel_label, {}, {}, target) YIELD rel
                RETURN count(rel) AS created
                """,
                session=session,
                parameters={'edges': edges},
            )

    async def delete_vocabulary_graph(self,
                                      prefix: ConceptPrefix,
                                      ):
        """
        Delete the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to delete.
        """
        async with self._client.session() as session:
            await self._execute_query_with_retry(
                query="""
                MATCH (n:Concept {prefix: $prefix})
                DETACH DELETE n
                """,
                session=session,
                parameters={'prefix': prefix.value},
            )

            # Delete the indexes associated with this prefix
            # All indexes would be in the format of {prefix}_*
            await self._execute_query_with_retry(
                query=cast(
                    LiteralString,
                    f"""
                    CALL db.indexes() YIELD name, entityType, labelsOrTypes, properties
                    WHERE labelsOrTypes = ['Concept'] AND properties[0] STARTS WITH '{prefix.value}_'
                    CALL apoc.schema.assert({{}}, {{name: name}}) YIELD label, key, action
                    RETURN count(*)
                    """,
                ),
                session=session,
            )

    async def create_index(self):
        """
        Create indexes in Neo4J
        """
        async with self._client.session() as session:
            await self._execute_query_with_retry(
                query="""
                CREATE CONSTRAINT concept_id_unique IF NOT EXISTS
                FOR (n:Concept)
                REQUIRE n.id IS UNIQUE
                """,
                session=session,
            )
            await self._execute_query_with_retry(
                query="""
                CREATE INDEX concept_prefix_index IF NOT EXISTS
                FOR (n:Concept)
                ON (n.prefix)
                """,
                session=session,
            )
