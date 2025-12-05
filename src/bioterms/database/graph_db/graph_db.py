from abc import ABC, abstractmethod
from typing import AsyncIterator
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import GraphDatabaseDriverType, ConceptPrefix, SimilarityMethod
from bioterms.model.concept import Concept
from bioterms.model.annotation import Annotation
from bioterms.model.expanded_term import ExpandedTerm
from bioterms.model.similar_term import SimilarTerm
from bioterms.model.translated_term import TranslatedTerm


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
    async def get_vocabulary_graph(self,
                                   prefix: ConceptPrefix,
                                   ) -> nx.DiGraph:
        """
        Retrieve the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to retrieve.
        :return: The vocabulary graph.
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
    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of nodes for a given prefix in the graph database.
        :param prefix: The vocabulary prefix to count nodes for.
        :return: The number of nodes with the given prefix.
        """

    @abstractmethod
    async def count_internal_relationships(self,
                                           prefix: ConceptPrefix,
                                           ) -> int:
        """
        Count the number of internal relationships within a vocabulary in the graph database.
        :param prefix: The vocabulary prefix to count relationships for
        :return: The number of internal relationships within the vocabulary.
        """

    @abstractmethod
    async def count_similarity_relationships(self,
                                             prefix_from: ConceptPrefix,
                                             prefix_to: ConceptPrefix,
                                             configurations: list[tuple[SimilarityMethod, ConceptPrefix | None]],
                                             ) -> list[tuple[SimilarityMethod, ConceptPrefix | None, int]]:
        """
        Count the number of similarity relationships between two vocabularies in the graph database,
        for each similarity method and corpus configuration.
        :param prefix_from: The source vocabulary prefix.
        :param prefix_to: The target vocabulary prefix.
        :param configurations: A list of tuples containing similarity methods and corpus prefixes
            (or None for intrinsic similarity).
        :return: A list of tuples containing the similarity method, corpus prefix,
            and the number of similarity relationships.
        """

    @abstractmethod
    async def save_annotations(self,
                               annotations: list[Annotation],
                               ):
        """
        Save a list of annotations into the graph database.
        :param annotations: A list of Annotation instances to save.
        """

    @abstractmethod
    async def get_annotation_graph(self,
                                   prefix_1: ConceptPrefix,
                                   prefix_2: ConceptPrefix,
                                   ) -> nx.DiGraph:
        """
        Retrieve the annotation graph between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        :return: The annotation graph between the two vocabularies.
        """

    @abstractmethod
    async def delete_annotations(self,
                                 prefix_1: ConceptPrefix,
                                 prefix_2: ConceptPrefix,
                                 ):
        """
        Delete annotations between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        """

    @abstractmethod
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

    @abstractmethod
    async def save_similarity_scores(self,
                                     prefix_from: ConceptPrefix,
                                     prefix_to: ConceptPrefix,
                                     similarity_scores: list[tuple[str, str, float]],
                                     similarity_method: SimilarityMethod,
                                     corpus_prefix: ConceptPrefix | None = None,
                                     ):
        """
        Save similarity scores between two vocabularies into the graph database.
        :param prefix_from: The source vocabulary prefix. Correspond to 'concept_from' in similarity_df.
        :param prefix_to: The target vocabulary prefix. Correspond to 'concept_to' in similarity_df.
        :param similarity_scores: A list of tuple containing similarity scores. In the format of:
            | concept_from | concept_to | similarity |
        :param similarity_method: The similarity method used to generate the scores. Stored as
            property name on the relationship.
        :param corpus_prefix: The corpus vocabulary prefix, if applicable.
        """

    @abstractmethod
    async def create_index(self):
        """
        Create indexes in the graph database.
        """

    @abstractmethod
    def expand_terms_iter(self,
                          prefix: ConceptPrefix,
                          concept_ids: list[str],
                          max_depth: int | None = None,
                          limit: int | None = None,
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
        :param limit: The maximum number of descendants to return for each term. If None, return all.
        :return: An asynchronous iterator yielding ExpandedTerm instances.
        """

    async def expand_terms(self,
                           prefix: ConceptPrefix,
                           concept_ids: list[str],
                           max_depth: int | None = None,
                           limit: int | None = None,
                           ) -> list[ExpandedTerm]:
        """
        Expand the given terms to retrieve their descendants up to the specified depth.

        This would only work on ontologies, because it relies on the IS_A relationships.
        Expanding a non-ontology or an ontology that does not have hierarchical relationships
        would return an empty set for each term.
        :param prefix: The prefix of the concepts to expand.
        :param concept_ids: The list of concept IDs to expand.
        :param max_depth: The maximum depth to expand. If None, expand to all depths.
        :param limit: The maximum number of descendants to return for each term. If None, return all.
        :return: A list of ExpandedTerm instances.
        """
        expand_iter = self.expand_terms_iter(
            prefix=prefix,
            concept_ids=concept_ids,
            max_depth=max_depth,
            limit=limit,
        )

        results: list[ExpandedTerm] = []
        async for expanded_term in expand_iter:
            results.append(expanded_term)

        return results

    @abstractmethod
    def get_similar_terms_iter(self,
                               prefix: ConceptPrefix,
                               concept_ids: list[str],
                               threshold: float = 1.0,
                               same_prefix: bool = True,
                               corpus_prefix: ConceptPrefix | None = None,
                               method: SimilarityMethod | None = None,
                               limit: int | None = None,
                               ) -> AsyncIterator[SimilarTerm]:
        """
        Get similar terms for the given concept IDs as an asynchronous iterator.
        :param prefix: The prefix of the concepts to find similar terms for.
        :param concept_ids: The list of concept IDs to find similar terms for.
        :param threshold: The similarity threshold to filter similar terms.
        :param same_prefix: Whether to only consider similar terms within the same prefix.
        :param corpus_prefix: The corpus prefix that was used to calculate the similarity score,
            if applicable.
        :param method: The similarity method to use.
        :param limit: The maximum number of similar terms to return for each concept ID.
        :return: An asynchronous iterator yielding SimilarTerm instances.
        """

    async def get_similar_terms(self,
                                prefix: ConceptPrefix,
                                concept_ids: list[str],
                                threshold: float = 1.0,
                                same_prefix: bool = True,
                                method: SimilarityMethod | None = None,
                                limit: int | None = None,
                                ) -> list[SimilarTerm]:
        """
        Get similar terms for the given concept IDs.
        :param prefix: The prefix of the concepts to find similar terms for.
        :param concept_ids: The list of concept IDs to find similar terms for.
        :param threshold: The similarity threshold to filter similar terms.
        :param same_prefix: Whether to only consider similar terms within the same prefix.
        :param method: The similarity method to use.
        :param limit: The maximum number of similar terms to return for each concept ID.
        :return: A list of SimilarTerm instances.
        """
        similar_iter = self.get_similar_terms_iter(
            prefix=prefix,
            concept_ids=concept_ids,
            threshold=threshold,
            same_prefix=same_prefix,
            method=method,
            limit=limit,
        )

        results: list[SimilarTerm] = []
        async for similar_term in similar_iter:
            results.append(similar_term)

        return results

    @abstractmethod
    def translate_terms_iter(self,
                             original_ids: list[str],
                             original_prefix: ConceptPrefix,
                             constraint_ids: dict[ConceptPrefix, set[str]],
                             threshold: float = 1.0,
                             limit: int | None = None,
                             ) -> AsyncIterator[TranslatedTerm]:
        """
        Translate terms to a subset of the constraint vocabulary as an asynchronous iterator,
        based on the similarity scores.
        :param original_ids: The list of original concept IDs to translate.
        :param original_prefix: The prefix of the original concepts.
        :param constraint_ids: A dictionary mapping constraint vocabulary prefixes to sets of concept IDs.
        :param threshold: The similarity threshold to filter translations.
        :param limit: The maximum number of translations to return for each original concept ID.
        :return: An asynchronous iterator yielding TranslatedTerm instances.
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

    if CONFIG.graph_database_driver == GraphDatabaseDriverType.MEMORY:
        from .memory_graph_db import MemoryGraphDatabase

        _active_graph_db = MemoryGraphDatabase()
        return _active_graph_db

    raise ValueError(
        f'Unsupported graph database driver type: {CONFIG.graph_db.driver_type}'
    )
