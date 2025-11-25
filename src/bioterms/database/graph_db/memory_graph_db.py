from typing import AsyncIterator
import networkx as nx

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from bioterms.model.annotation import Annotation
from bioterms.model.expanded_term import ExpandedTerm
from .graph_db import GraphDatabase


class MemoryGraphDatabase(GraphDatabase):
    """
    An implementation of the GraphDatabase interface using an in-memory graph.
    """

    _graphs: dict[ConceptPrefix, nx.DiGraph] = {}
    _annotation_graphs: dict[ConceptPrefix, dict[ConceptPrefix, nx.DiGraph]] = {}

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

    async def get_vocabulary_graph(self,
                                   prefix: ConceptPrefix,
                                   ) -> nx.DiGraph:
        """
        Retrieve the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to retrieve.
        :return: The vocabulary graph.
        """
        return self._graphs.get(prefix, nx.DiGraph())

    async def delete_vocabulary_graph(self,
                                      prefix: ConceptPrefix,
                                      ):
        """
        Delete the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to delete.
        """
        if prefix in self._graphs:
            del self._graphs[prefix]

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of nodes for a given prefix in the graph database.
        :param prefix: The vocabulary prefix to count nodes for.
        :return: The number of nodes with the given prefix.
        """
        graph = self._graphs.get(prefix)
        if graph is None:
            return 0

        return graph.number_of_nodes()

    async def count_internal_relationships(self,
                                           prefix: ConceptPrefix,
                                           ) -> int:
        """
        Count the number of internal relationships within a vocabulary in the graph database.
        :param prefix: The vocabulary prefix to count relationships for
        :return: The number of internal relationships within the vocabulary.
        """
        graph = self._graphs.get(prefix)

        if graph is None:
            return 0

        return graph.number_of_edges()

    async def save_annotations(self,
                               annotations: list[Annotation],
                               ):
        """
        Save a list of annotations into the graph database.
        :param annotations: A list of Annotation instances to save.
        """
        annotation_graph = nx.DiGraph()

        for annotation in annotations:
            source_node = f'{annotation.prefix_from}:{annotation.prefix_to}'
            dest_node = f'{annotation.prefix_to}:{annotation.concept_id_to}'
            annotation_graph.add_edge(
                source_node,
                dest_node,
                label=annotation.relationship_type,
                **(annotation.properties or {})
            )

        self._annotation_graphs.setdefault(
            annotations[0].prefix_from, {}
        )[annotations[0].prefix_to] = annotation_graph

    async def delete_annotations(self,
                                 prefix_1: ConceptPrefix,
                                 prefix_2: ConceptPrefix,
                                 ):
        """
        Delete annotations between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        """
        if prefix_1 in self._annotation_graphs:
            if prefix_2 in self._annotation_graphs[prefix_1]:
                del self._annotation_graphs[prefix_1][prefix_2]

        if prefix_2 in self._annotation_graphs:
            if prefix_1 in self._annotation_graphs[prefix_2]:
                del self._annotation_graphs[prefix_2][prefix_1]

    async def count_annotations(self,
                                prefix_1: ConceptPrefix,
                                prefix_2: ConceptPrefix,
                                ) -> int:
        """
        Count the number of annotations between two vocabularies in the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        :return: The number of annotations between the two vocabularies.
        """
        annotation_graph = self._annotation_graphs.get(prefix_1, {}).get(prefix_2)

        if annotation_graph is None:
            annotation_graph = self._annotation_graphs.get(prefix_2, {}).get(prefix_1)

        if annotation_graph is None:
            return 0

        return annotation_graph.number_of_edges()

    async def create_index(self):
        """
        In-memory graph does not require indexes.
        """

    async def expand_terms_iter(self,
                                prefix: ConceptPrefix,
                                concept_ids: list[str],
                                max_depth: int | None = None,
                                ) -> AsyncIterator[ExpandedTerm]:
        """
        Expand the given terms to retrieve their descendants up to the specified depth, and return
        an asynchronous iterator over the results.

        This would only work on ontologies, because it relies on the IS_A relationships.
        Expanding a non-ontology or an ontology that does not have hierarchical relationships
        would return an empty set for each term.
        :param prefix: The prefix of the concepts to expand.
        :param concept_ids: The list of concept IDs to expand.
        :param max_depth: The maximum depth to expand. If None, expand to all depths.
        :return: An asynchronous iterator yielding ExpandedTerm instances.
        """
