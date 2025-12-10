from typing import Optional
from aiodataloader import DataLoader

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase


class ReactomePathwayLoaderByParent(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            parent_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_sub_pathways(parent_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(parent_id, []) for parent_id in parent_ids]

        return sorted_results


class ReactomePathwayLoaderByChild(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            child_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_super_pathways(child_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(child_id, []) for child_id in child_ids]

        return sorted_results


class ReactomeReactionLoaderByPreceding(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            preceding_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_subsequent_reactions(preceding_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(preceding_id, []) for preceding_id in preceding_ids]

        return sorted_results


class ReactomeReactionLoaderBySubsequent(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            subsequent_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_preceding_reactions(subsequent_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(subsequent_id, []) for subsequent_id in subsequent_ids]

        return sorted_results


class ReactomeReactionLoaderByPathway(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            pathway_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_reactions_in_pathway(pathway_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(pathway_id, []) for pathway_id in pathway_ids]

        return sorted_results


class ReactomeReactionLoaderByInput(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            gene_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_gene_input_reactions(gene_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(input_id, []) for input_id in gene_ids]

        return sorted_results


class ReactomeReactionLoaderByOutput(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            gene_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_gene_output_reactions(gene_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(output_id, []) for output_id in gene_ids]

        return sorted_results


class ReactomeGeneLoaderByInput(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            reaction_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_reaction_inputs(reaction_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(reaction_id, []) for reaction_id in reaction_ids]

        return sorted_results


class ReactomeGeneLoaderByOutput(DataLoader[str, list[str]]):
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            reaction_ids: list[str]
                            ) -> list[list[str]]:
        results = await self._graph_db.reactome.get_reaction_outputs(reaction_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(reaction_id, []) for reaction_id in reaction_ids]

        return sorted_results


class ReactomeLoader:
    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        self._graph_db = graph_db

        self._pathway_parent_loader = None
        self._pathway_child_loader = None
        self._reaction_preceding_loader = None
        self._reaction_subsequent_loader = None
        self._reaction_pathway_loader = None
        self._gene_input_loader = None
        self._gene_output_loader = None

    @property
    def sub_pathway(self) -> ReactomePathwayLoaderByParent:
        if self._pathway_parent_loader is None:
            self._pathway_parent_loader = ReactomePathwayLoaderByParent(
                graph_db=self._graph_db,
            )

        return self._pathway_parent_loader

    @property
    def super_pathway(self) -> ReactomePathwayLoaderByChild:
        if self._pathway_child_loader is None:
            self._pathway_child_loader = ReactomePathwayLoaderByChild(
                graph_db=self._graph_db,
            )

        return self._pathway_child_loader

    @property
    def pathway_reactions(self) -> ReactomeReactionLoaderByPathway:
        if self._reaction_pathway_loader is None:
            self._reaction_pathway_loader = ReactomeReactionLoaderByPathway(
                graph_db=self._graph_db,
            )

        return self._reaction_pathway_loader

    @property
    def subsequent_reaction(self) -> ReactomeReactionLoaderByPreceding:
        if self._reaction_preceding_loader is None:
            self._reaction_preceding_loader = ReactomeReactionLoaderByPreceding(
                graph_db=self._graph_db,
            )

        return self._reaction_preceding_loader

    @property
    def preceding_reaction(self) -> ReactomeReactionLoaderBySubsequent:
        if self._reaction_subsequent_loader is None:
            self._reaction_subsequent_loader = ReactomeReactionLoaderBySubsequent(
                graph_db=self._graph_db,
            )

        return self._reaction_subsequent_loader

    @property
    def reaction_inputs(self) -> ReactomeGeneLoaderByInput:
        if self._gene_input_loader is None:
            self._gene_input_loader = ReactomeGeneLoaderByInput(
                graph_db=self._graph_db,
            )

        return self._gene_input_loader

    @property
    def reaction_outputs(self) -> ReactomeGeneLoaderByOutput:
        if self._gene_output_loader is None:
            self._gene_output_loader = ReactomeGeneLoaderByOutput(
                graph_db=self._graph_db,
            )

        return self._gene_output_loader

    @property
    def gene_as_input(self) -> ReactomeReactionLoaderByInput:
        if self._gene_input_loader is None:
            self._gene_input_loader = ReactomeReactionLoaderByInput(
                graph_db=self._graph_db,
            )

        return self._gene_input_loader

    @property
    def gene_as_output(self) -> ReactomeReactionLoaderByOutput:
        if self._gene_output_loader is None:
            self._gene_output_loader = ReactomeReactionLoaderByOutput(
                graph_db=self._graph_db,
            )

        return self._gene_output_loader
