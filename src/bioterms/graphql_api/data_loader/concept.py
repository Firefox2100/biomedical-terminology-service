"""
Data loaders for concept-related data fetching.
"""

from typing import Optional
from aiodataloader import DataLoader

from bioterms.etc.enums import ConceptPrefix, AnnotationType, ConceptRelationshipType
from bioterms.database import DocumentDatabase, GraphDatabase
from bioterms.vocabulary import get_vocabulary_config


class ConceptLoaderById(DataLoader[str, Optional[dict]]):
    """
    Data loader to fetch concepts by their IDs.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 doc_db: DocumentDatabase,
                 config: dict,
                 ):
        """
        Initialize the ConceptLoaderById.
        :param prefix: The vocabulary prefix.
        :param doc_db: The document database instance.
        :param config: The vocabulary configuration dictionary.
        """
        super().__init__()

        self._prefix = prefix
        self._doc_db = doc_db
        self._config = config

    async def batch_load_fn(self,
                            concept_ids: list[str]
                            ) -> list[Optional[dict]]:
        """
        Batch load function to fetch concepts by their IDs.
        :param concept_ids: List of concept IDs to fetch.
        :return: List of concept data dictionaries or None if not found.
        """
        concepts = await self._doc_db.get_terms_by_ids(
            prefix=self._prefix,
            concept_ids=concept_ids,
            model_class=self._config['conceptClass'],
        )

        concept_map = {concept.concept_id: concept.model_dump() for concept in concepts}
        sorted_concepts = [concept_map.get(concept_id) for concept_id in concept_ids]

        return sorted_concepts


class ConceptLoaderByParent(DataLoader[str, list[str]]):
    """
    Data loader to fetch child concepts given parent concept IDs.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoaderByParent.
        :param prefix: The vocabulary prefix.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            parent_ids: list[str]
                            ) -> list[list[str]]:
        """
        Batch load function to fetch child concepts for given parent IDs.
        :param parent_ids: List of parent concept IDs.
        :return: List of lists of child concept IDs.
        """
        expansion_results = await self._graph_db.expand_terms(
            prefix=self._prefix,
            concept_ids=parent_ids,
            max_depth=1,
        )

        result_map = {result.concept_id: result.related_concepts for result in expansion_results}
        sorted_results = [result_map.get(parent_id, []) for parent_id in parent_ids]

        return sorted_results


class ConceptLoaderByChild(DataLoader[str, list[str]]):
    """
    Data loader to fetch parent concepts given child concept IDs.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoaderByChild.
        :param prefix: The vocabulary prefix.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            child_ids: list[str]
                            ) -> list[list[str]]:
        """
        Batch load function to fetch parent concepts for given child IDs.
        :param child_ids: List of child concept IDs.
        :return: List of lists of parent concept IDs.
        """
        expansion_results = await self._graph_db.trace_ancestors(
            prefix=self._prefix,
            concept_ids=child_ids,
            max_depth=1,
        )

        result_map = {result.concept_id: result.related_concepts for result in expansion_results}
        sorted_results = [result_map.get(child_id, []) for child_id in child_ids]

        return sorted_results


class ConceptLoaderByReplacement(DataLoader[str, list[str]]):
    """
    Data loader to fetch replacing concepts given concept IDs.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoaderByReplacement.
        :param prefix: The vocabulary prefix.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            concept_ids: list[str],
                            ) -> list[list[str]]:
        """
        Batch load function to fetch replacing concepts for given concept IDs.
        :param concept_ids: List of concept IDs.
        :return: List of lists of replacing concept IDs.
        """
        replacements = await self._graph_db.get_replacing_terms(
            prefix=self._prefix,
            concept_ids=concept_ids,
        )

        replacement_map = {item.concept_id: item.related_concepts for item in replacements}
        sorted_replacements = [replacement_map.get(concept_id, []) for concept_id in concept_ids]

        return sorted_replacements


class ConceptLoaderByReplaced(DataLoader[str, list[str]]):
    """
    Data loader to fetch replaced concepts given concept IDs.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoaderByReplaced.
        :param prefix: The vocabulary prefix.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            concept_ids: list[str],
                            ) -> list[list[str]]:
        """
        Batch load function to fetch replaced concepts for given concept IDs.
        :param concept_ids: List of concept IDs.
        :return: List of lists of replaced concept IDs.
        """
        replaced_terms = await self._graph_db.get_replaced_terms(
            prefix=self._prefix,
            concept_ids=concept_ids,
        )

        replaced_map = {item.concept_id: item.related_concepts for item in replaced_terms}
        sorted_replaced = [replaced_map.get(concept_id, []) for concept_id in concept_ids]

        return sorted_replaced


class ConceptLoaderBySimilarity(DataLoader[tuple[str, float], list[tuple[str, float]]]):
    """
    Data loader to fetch similar concepts given concept ID and similarity threshold.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoaderBySimilarity.
        :param prefix: The vocabulary prefix.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            queries: list[tuple[str, float]],
                            ) -> list[list[tuple[str, float]]]:
        """
        Batch load function to fetch similar concepts for given queries.
        :param queries: List of tuples containing concept ID and similarity threshold.
        :return: List of lists of tuples containing similar concept IDs and their similarity scores.
        """
        similar_concepts = await self._graph_db.get_similar_terms_aggregate(
            prefix=self._prefix,
            similarity_queries=queries,
        )

        similar_map = {item.concept_id: item.similar_concepts for item in similar_concepts}
        sorted_similar = [similar_map.get(query[0], []) for query in queries]

        return sorted_similar


class ConceptLoaderByPathsTo(DataLoader[
                                 tuple[str, str, str, str, str, int],
                                 list[tuple[int, list[tuple[str, str]]]]
                             ]):
    """
    Data loader to fetch paths to target concepts given source concept IDs.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoaderByPathsTo.
        :param prefix: The vocabulary prefix.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    @staticmethod
    def _str_to_relationship(rel) -> AnnotationType | ConceptRelationshipType:
        try:
            return ConceptRelationshipType(rel)
        except ValueError:
            return AnnotationType(rel)

    async def batch_load_fn(self,
                            queries: list[tuple[str, str, str, str, str, int]],
                            ) -> list[list[tuple[int, list[tuple[str, str]]]]]:
        """
        Batch load function to fetch paths to target concepts for given queries.
        :param queries: List of tuples containing source concept ID, target prefix, target concept ID,
                relationship type, direction, and max depth.
        :return: List of lists of tuples containing path length and list of (prefix, concept IDs) tuples
            in the path.
        """
        converted_queries = []
        for query in queries:
            source_id, target_prefix, target_id, relationship, direction, max_depth = query

            converted_queries.append((
                self._prefix,
                source_id,
                ConceptPrefix(target_prefix),
                target_id,
                self._str_to_relationship(relationship),
                None if direction == 'undirected' else (direction == 'forward'),
                max_depth,
            ))

        paths = await self._graph_db.trace_term_aggregate(
            trace_queries=converted_queries,
        )

        # Convert the paths to the expected output format
        paths_map = {}
        for p in paths:
            if p.start_concept_id not in paths_map:
                paths_map[p.start_concept_id] = []

            paths_map[p.start_concept_id].append((
                p.length,
                [(n.prefix.value, n.concept_id) for n in p.nodes]
            ))

        sorted_paths = [paths_map.get(query[0], []) for query in queries]
        return sorted_paths


class ConceptLoaderByAnnotatedConcepts(DataLoader[str, list[str]]):
    """
    Data loader to fetch mapped concepts from source prefix to target prefix.
    """

    def __init__(self,
                 source_prefix: ConceptPrefix,
                 target_prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoaderByAnnotatedConcepts.
        :param source_prefix: The source vocabulary prefix.
        :param target_prefix: The target vocabulary prefix.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._source_prefix = source_prefix
        self._target_prefix = target_prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            concept_ids: list[str],
                            ) -> list[list[str]]:
        """
        Batch load function to fetch mapped concepts for given concept IDs.
        :param concept_ids: List of concept IDs.
        :return: List of lists of mapped concept IDs.
        """
        mapped_concepts = await self._graph_db.map_terms(
            prefix=self._source_prefix,
            target_prefix=self._target_prefix,
            concept_ids=concept_ids,
        )

        mapped_map = {item.concept_id: item.related_concepts for item in mapped_concepts}
        sorted_mapped = [mapped_map.get(concept_id, []) for concept_id in concept_ids]

        return sorted_mapped


class ConceptLoader:
    """
    Concept data loader providing various methods to load concept-related data.
    """

    def __init__(self,
                 prefix: ConceptPrefix,
                 doc_db: DocumentDatabase,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the ConceptLoader.
        :param prefix: The vocabulary prefix.
        :param doc_db: The document database instance.
        :param graph_db: The graph database instance.
        """
        self._prefix = prefix
        self._doc_db = doc_db
        self._graph_db = graph_db
        self._config = get_vocabulary_config(prefix)

        self._id_loader = None
        self._children_loader = None
        self._parents_loader = None
        self._replaced_loader = None
        self._replacement_loader = None
        self._similarity_loader = None
        self._mapping_loaders = {}

    @property
    def id(self) -> ConceptLoaderById:
        """
        Get the ConceptLoaderById instance.
        :return: The ConceptLoaderById instance.
        """
        if self._id_loader is None:
            self._id_loader = ConceptLoaderById(
                prefix=self._prefix,
                doc_db=self._doc_db,
                config=self._config,
            )

        return self._id_loader

    @property
    def children(self) -> ConceptLoaderByParent:
        """
        Get the ConceptLoaderByParent instance.
        :return: The ConceptLoaderByParent instance.
        """
        if self._children_loader is None:
            self._children_loader = ConceptLoaderByParent(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._children_loader

    @property
    def parents(self) -> ConceptLoaderByChild:
        """
        Get the ConceptLoaderByChild instance.
        :return: The ConceptLoaderByChild instance.
        """
        if self._parents_loader is None:
            self._parents_loader = ConceptLoaderByChild(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._parents_loader

    @property
    def replaced(self) -> ConceptLoaderByReplaced:
        """
        Get the ConceptLoaderByReplaced instance.
        :return: The ConceptLoaderByReplaced instance.
        """
        if self._replaced_loader is None:
            self._replaced_loader = ConceptLoaderByReplaced(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._replaced_loader

    @property
    def replacement(self) -> ConceptLoaderByReplacement:
        """
        Get the ConceptLoaderByReplacement instance.
        :return: The ConceptLoaderByReplacement instance.
        """
        if self._replacement_loader is None:
            self._replacement_loader = ConceptLoaderByReplacement(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._replacement_loader

    @property
    def similar(self) -> ConceptLoaderBySimilarity:
        """
        Get the ConceptLoaderBySimilarity instance.
        :return: The ConceptLoaderBySimilarity instance.
        """
        if self._similarity_loader is None:
            self._similarity_loader = ConceptLoaderBySimilarity(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._similarity_loader

    @property
    def paths_to(self) -> ConceptLoaderByPathsTo:
        """
        Get the ConceptLoaderByPathsTo instance.
        :return: The ConceptLoaderByPathsTo instance.
        """
        return ConceptLoaderByPathsTo(
            prefix=self._prefix,
            graph_db=self._graph_db,
        )

    def get_mapping_loader(self,
                           target_prefix: ConceptPrefix,
                           ) -> ConceptLoaderByAnnotatedConcepts:
        """
        Get the ConceptLoaderByAnnotatedConcepts for the specified target prefix.
        :param target_prefix: The target vocabulary prefix.
        :return: The ConceptLoaderByAnnotatedConcepts instance.
        """
        if target_prefix not in self._mapping_loaders:
            self._mapping_loaders[target_prefix] = ConceptLoaderByAnnotatedConcepts(
                source_prefix=self._prefix,
                target_prefix=target_prefix,
                graph_db=self._graph_db,
            )

        return self._mapping_loaders[target_prefix]
