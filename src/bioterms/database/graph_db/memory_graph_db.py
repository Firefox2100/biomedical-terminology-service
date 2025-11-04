import asyncio
from typing import LiteralString
import networkx as nx

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from .graph_db import GraphDatabase


class MemoryGraphDatabase(GraphDatabase):
    """
    An implementation of the GraphDatabase interface using an in-memory graph.
    """

    _graphs: dict[ConceptPrefix, nx.DiGraph] = {}

    async def close(self) -> None:
        """
        Remove the in-memory graphs.
        """
        self._graphs.clear()

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
        if not concepts:
            return

        prefix = concepts[0].prefix
        self._graphs[prefix] = graph

    async def delete_vocabulary_graph(self,
                                      prefix: ConceptPrefix,
                                      ):
        """
        Delete the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to delete.
        """
        if prefix in self._graphs:
            del self._graphs[prefix]

    async def create_index(self):
        """
        In-memory graph does not require indexes.
        """

    async def expand_terms(self,
                           prefix: ConceptPrefix,
                           concept_ids: list[str],
                           max_depth: int | None = None,
                           ) -> dict[str, set[str]]:
        """
        Expand the given terms to retrieve their descendants up to the specified depth.

        This would only work on ontologies, because it relies on the IS_A relationships.
        Expanding a non-ontology or an ontology that does not have hierarchical relationships
        would return an empty set for each term.
        :param prefix: The prefix of the concepts to expand.
        :param concept_ids: The list of concept IDs to expand.
        :param max_depth: The maximum depth to expand. If None, expand to all depths.
        :return: A dictionary mapping each concept ID to a set of its descendant concept IDs.
        """
