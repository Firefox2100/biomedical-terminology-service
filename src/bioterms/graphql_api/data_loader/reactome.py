"""
Data loaders for Reactome pathways, reactions, and genes.
"""

from aiodataloader import DataLoader

from bioterms.database import GraphDatabase


class ReactomePathwayLoaderByParent(DataLoader[str, list[str]]):
    """
    Data loader to fetch sub-pathways given parent pathway IDs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            parent_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load sub-pathways for the given parent pathway IDs.
        :param parent_ids: List of parent pathway IDs.
        :return: List of lists of sub-pathway IDs corresponding to each parent ID.
        """
        results = await self._graph_db.reactome.get_sub_pathways(parent_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(parent_id, []) for parent_id in parent_ids]

        return sorted_results


class ReactomePathwayLoaderByChild(DataLoader[str, list[str]]):
    """
    Data loader to fetch parent pathways given child pathway IDs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            child_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load parent pathways for the given child pathway IDs.
        :param child_ids: List of child pathway IDs.
        :return: List of lists of parent pathway IDs corresponding to each child ID.
        """
        results = await self._graph_db.reactome.get_super_pathways(child_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(child_id, []) for child_id in child_ids]

        return sorted_results


class ReactomeReactionLoaderByPreceding(DataLoader[str, list[str]]):
    """
    Data loader to fetch subsequent reactions given preceding reaction IDs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            preceding_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load subsequent reactions for the given preceding reaction IDs.
        :param preceding_ids: List of preceding reaction IDs.
        :return: List of lists of subsequent reaction IDs corresponding to each preceding ID.
        """
        results = await self._graph_db.reactome.get_subsequent_reactions(preceding_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(preceding_id, []) for preceding_id in preceding_ids]

        return sorted_results


class ReactomeReactionLoaderBySubsequent(DataLoader[str, list[str]]):
    """
    Data loader to fetch preceding reactions given subsequent reaction IDs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            subsequent_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load preceding reactions for the given subsequent reaction IDs.
        :param subsequent_ids: List of subsequent reaction IDs.
        :return: List of lists of preceding reaction IDs corresponding to each subsequent ID.
        """
        results = await self._graph_db.reactome.get_preceding_reactions(subsequent_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(subsequent_id, []) for subsequent_id in subsequent_ids]

        return sorted_results


class ReactomeReactionLoaderByPathway(DataLoader[str, list[str]]):
    """
    Data loader to fetch reactions given pathway IDs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            pathway_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load reactions for the given pathway IDs.
        :param pathway_ids: List of pathway IDs.
        :return: List of lists of reaction IDs corresponding to each pathway ID.
        """
        results = await self._graph_db.reactome.get_reactions_in_pathway(pathway_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(pathway_id, []) for pathway_id in pathway_ids]

        return sorted_results


class ReactomeReactionLoaderByInput(DataLoader[str, list[str]]):
    """
    Data loader to fetch reactions given gene IDs as inputs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            gene_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load reactions for the given gene IDs as inputs.
        :param gene_ids: List of gene IDs.
        :return: List of lists of reaction IDs corresponding to each gene ID.
        """
        results = await self._graph_db.reactome.get_gene_input_reactions(gene_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(input_id, []) for input_id in gene_ids]

        return sorted_results


class ReactomeReactionLoaderByOutput(DataLoader[str, list[str]]):
    """
    Data loader to fetch reactions given gene IDs as outputs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            gene_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load reactions for the given gene IDs as outputs.
        :param gene_ids: List of gene IDs.
        :return: List of lists of reaction IDs corresponding to each gene ID.
        """
        results = await self._graph_db.reactome.get_gene_output_reactions(gene_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(output_id, []) for output_id in gene_ids]

        return sorted_results


class ReactomeGeneLoaderByInput(DataLoader[str, list[str]]):
    """
    Data loader to fetch gene inputs given reaction IDs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            reaction_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load gene inputs for the given reaction IDs.
        :param reaction_ids: List of reaction IDs.
        :return: List of lists of gene IDs corresponding to each reaction ID.
        """
        results = await self._graph_db.reactome.get_reaction_inputs(reaction_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(reaction_id, []) for reaction_id in reaction_ids]

        return sorted_results


class ReactomeGeneLoaderByOutput(DataLoader[str, list[str]]):
    """
    Data loader to fetch gene outputs given reaction IDs.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the loader with the graph database.
        :param graph_db: The graph database instance.
        """
        super().__init__()

        self._graph_db = graph_db

    async def batch_load_fn(self,
                            reaction_ids: list[str]
                            ) -> list[list[str]]:
        """
        Load gene outputs for the given reaction IDs.
        :param reaction_ids: List of reaction IDs.
        :return: List of lists of gene IDs corresponding to each reaction ID.
        """
        results = await self._graph_db.reactome.get_reaction_outputs(reaction_ids)

        result_map = {result.concept_id: result.related_concepts for result in results}
        sorted_results = [result_map.get(reaction_id, []) for reaction_id in reaction_ids]

        return sorted_results


class ReactomeLoader:
    """
    Singleton data loader for Reactome pathways, reactions, and genes.
    """

    def __init__(self,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the Reactome data loader.
        :param graph_db: The graph database instance.
        """
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
        """
        Get the loader for sub-pathways given parent pathway IDs.
        :return: The ReactomePathwayLoaderByParent instance.
        """
        if self._pathway_parent_loader is None:
            self._pathway_parent_loader = ReactomePathwayLoaderByParent(
                graph_db=self._graph_db,
            )

        return self._pathway_parent_loader

    @property
    def super_pathway(self) -> ReactomePathwayLoaderByChild:
        """
        Get the loader for parent pathways given child pathway IDs.
        :return: The ReactomePathwayLoaderByChild instance.
        """
        if self._pathway_child_loader is None:
            self._pathway_child_loader = ReactomePathwayLoaderByChild(
                graph_db=self._graph_db,
            )

        return self._pathway_child_loader

    @property
    def pathway_reactions(self) -> ReactomeReactionLoaderByPathway:
        """
        Get the loader for reactions given pathway IDs.
        :return: The ReactomeReactionLoaderByPathway instance.
        """
        if self._reaction_pathway_loader is None:
            self._reaction_pathway_loader = ReactomeReactionLoaderByPathway(
                graph_db=self._graph_db,
            )

        return self._reaction_pathway_loader

    @property
    def subsequent_reaction(self) -> ReactomeReactionLoaderByPreceding:
        """
        Get the loader for subsequent reactions given preceding reaction IDs.
        :return: The ReactomeReactionLoaderByPreceding instance.
        """
        if self._reaction_preceding_loader is None:
            self._reaction_preceding_loader = ReactomeReactionLoaderByPreceding(
                graph_db=self._graph_db,
            )

        return self._reaction_preceding_loader

    @property
    def preceding_reaction(self) -> ReactomeReactionLoaderBySubsequent:
        """
        Get the loader for preceding reactions given subsequent reaction IDs.
        :return: The ReactomeReactionLoaderBySubsequent instance.
        """
        if self._reaction_subsequent_loader is None:
            self._reaction_subsequent_loader = ReactomeReactionLoaderBySubsequent(
                graph_db=self._graph_db,
            )

        return self._reaction_subsequent_loader

    @property
    def reaction_inputs(self) -> ReactomeGeneLoaderByInput:
        """
        Get the loader for gene inputs given reaction IDs.
        :return: The ReactomeGeneLoaderByInput instance.
        """
        if self._gene_input_loader is None:
            self._gene_input_loader = ReactomeGeneLoaderByInput(
                graph_db=self._graph_db,
            )

        return self._gene_input_loader

    @property
    def reaction_outputs(self) -> ReactomeGeneLoaderByOutput:
        """
        Get the loader for gene outputs given reaction IDs.
        :return: The ReactomeGeneLoaderByOutput instance.
        """
        if self._gene_output_loader is None:
            self._gene_output_loader = ReactomeGeneLoaderByOutput(
                graph_db=self._graph_db,
            )

        return self._gene_output_loader

    @property
    def gene_as_input(self) -> ReactomeReactionLoaderByInput:
        """
        Get the loader for reactions given gene IDs as inputs.
        :return: The ReactomeReactionLoaderByInput instance.
        """
        if self._gene_input_loader is None:
            self._gene_input_loader = ReactomeReactionLoaderByInput(
                graph_db=self._graph_db,
            )

        return self._gene_input_loader

    @property
    def gene_as_output(self) -> ReactomeReactionLoaderByOutput:
        """
        Get the loader for reactions given gene IDs as outputs.
        :return: The ReactomeReactionLoaderByOutput instance.
        """
        if self._gene_output_loader is None:
            self._gene_output_loader = ReactomeReactionLoaderByOutput(
                graph_db=self._graph_db,
            )

        return self._gene_output_loader
