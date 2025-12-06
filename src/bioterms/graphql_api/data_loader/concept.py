from typing import Optional
from aiodataloader import DataLoader

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase


class ConceptLoaderById(DataLoader[str, Optional[dict]]):
    def __init__(self,
                 prefix: ConceptPrefix,
                 doc_db: DocumentDatabase,
                 ):
        super().__init__()

        self._prefix = prefix
        self._doc_db = doc_db

    async def batch_load_fn(self,
                            concept_ids: list[str]
                            ) -> list[Optional[dict]]:
        concepts = await self._doc_db.get_terms_by_ids(
            prefix=self._prefix,
            concept_ids=concept_ids,
        )

        concept_map = {concept.concept_id: concept.model_dump() for concept in concepts}
        sorted_concepts = [concept_map.get(concept_id) for concept_id in concept_ids]

        return sorted_concepts


class ConceptLoaderByParent(DataLoader[str, list[str]]):
    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            parent_ids: list[str]
                            ) -> list[list[str]]:
        expansion_results = await self._graph_db.expand_terms(
            prefix=self._prefix,
            concept_ids=parent_ids,
            max_depth=1,
        )

        result_map = {result.concept_id: result.related_concepts for result in expansion_results}
        sorted_results = [result_map.get(parent_id, []) for parent_id in parent_ids]

        return sorted_results


class ConceptLoaderByChild(DataLoader[str, list[str]]):
    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            child_ids: list[str]
                            ) -> list[list[str]]:
        expansion_results = await self._graph_db.trace_ancestors(
            prefix=self._prefix,
            concept_ids=child_ids,
            max_depth=1,
        )

        result_map = {result.concept_id: result.related_concepts for result in expansion_results}
        sorted_results = [result_map.get(child_id, []) for child_id in child_ids]

        return sorted_results


class ConceptLoaderByReplacement(DataLoader[str, list[str]]):
    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            concept_ids: list[str],
                            ) -> list[list[str]]:
        replacements = await self._graph_db.get_replacing_terms(
            prefix=self._prefix,
            concept_ids=concept_ids,
        )

        replacement_map = {item.concept_id: item.related_concepts for item in replacements}
        sorted_replacements = [replacement_map.get(concept_id, []) for concept_id in concept_ids]

        return sorted_replacements


class ConceptLoaderByReplaced(DataLoader[str, list[str]]):
    def __init__(self,
                 prefix: ConceptPrefix,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._prefix = prefix
        self._graph_db = graph_db

    async def batch_load_fn(self,
                            concept_ids: list[str],
                            ) -> list[list[str]]:
        replaced_terms = await self._graph_db.get_replaced_terms(
            prefix=self._prefix,
            concept_ids=concept_ids,
        )

        replaced_map = {item.concept_id: item.related_concepts for item in replaced_terms}
        sorted_replaced = [replaced_map.get(concept_id, []) for concept_id in concept_ids]

        return sorted_replaced


class ConceptLoader:
    def __init__(self,
                 prefix: ConceptPrefix,
                 doc_db: DocumentDatabase,
                 graph_db: GraphDatabase,
                 ):
        self._prefix = prefix
        self._doc_db = doc_db
        self._graph_db = graph_db

        self._id_loader = None
        self._children_loader = None
        self._parents_loader = None
        self._replaced_loader = None
        self._replacement_loader = None

    @property
    def id(self) -> ConceptLoaderById:
        if self._id_loader is None:
            self._id_loader = ConceptLoaderById(
                prefix=self._prefix,
                doc_db=self._doc_db,
            )

        return self._id_loader

    @property
    def children(self) -> ConceptLoaderByParent:
        if self._children_loader is None:
            self._children_loader = ConceptLoaderByParent(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._children_loader

    @property
    def parents(self) -> ConceptLoaderByChild:
        if self._parents_loader is None:
            self._parents_loader = ConceptLoaderByChild(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._parents_loader

    @property
    def replaced(self) -> ConceptLoaderByReplaced:
        if self._replaced_loader is None:
            self._replaced_loader = ConceptLoaderByReplaced(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._replaced_loader

    @property
    def replacement(self) -> ConceptLoaderByReplacement:
        if self._replacement_loader is None:
            self._replacement_loader = ConceptLoaderByReplacement(
                prefix=self._prefix,
                graph_db=self._graph_db,
            )

        return self._replacement_loader
