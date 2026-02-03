"""
Resolvers for Reactome concepts.
"""

from ariadne import ObjectType, InterfaceType

from bioterms.etc.enums import ConceptPrefix
from ..data_loader import DataLoader
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, \
    resolve_concept_similar_concepts, resolve_get_concept, resolve_auto_complete, \
    resolve_concept_annotated_concepts, resolve_concept_paths_to


REACTOME_CONCEPT = InterfaceType('ReactomeConcept')
REACTOME_PATHWAY = ObjectType('ReactomePathway')
REACTOME_REACTION = ObjectType('ReactomeReaction')
REACTOME_GENE = ObjectType('ReactomeGene')
REACTOME_QUERY = ObjectType('ReactomeQuery')


@REACTOME_PATHWAY.field('prefix')
@REACTOME_PATHWAY.field('label')
@REACTOME_PATHWAY.field('inferred')
@REACTOME_PATHWAY.field('status')
@REACTOME_REACTION.field('prefix')
@REACTOME_REACTION.field('label')
@REACTOME_REACTION.field('inferred')
@REACTOME_REACTION.field('status')
@REACTOME_GENE.field('prefix')
@REACTOME_GENE.field('label')
@REACTOME_GENE.field('inferred')
@REACTOME_GENE.field('status')
async def resolve_reactome_info_fields(obj, info):
    """
    Resolve Reactome concept info fields.
    :param obj: The Reactome concept object.
    :param info: The GraphQL resolve info.
    :return: The value of the requested field.
    """
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_PATHWAY.field('subPathways')
async def resolve_reactome_pathway_sub_pathways(obj, info):
    """
    Resolve Reactome pathway subPathways field.
    :param obj: The Reactome pathway object.
    :param info: The GraphQL resolve info.
    :return: The list of sub-pathway concepts.
    """
    data_loader: DataLoader = info.context['data_loader']
    sub_pathway_ids = await data_loader.reactome.sub_pathway.load(obj['conceptId'])

    return [
        {'conceptId': sub_pathway_id} for sub_pathway_id in sub_pathway_ids
    ]


@REACTOME_PATHWAY.field('superPathways')
async def resolve_reactome_pathway_super_pathways(obj, info):
    """
    Resolve Reactome pathway superPathways field.
    :param obj: The Reactome pathway object.
    :param info: The GraphQL resolve info.
    :return: The list of super-pathway concepts.
    """
    data_loader: DataLoader = info.context['data_loader']
    super_pathway_ids = await data_loader.reactome.super_pathway.load(obj['conceptId'])

    return [
        {'conceptId': super_pathway_id} for super_pathway_id in super_pathway_ids
    ]


@REACTOME_PATHWAY.field('reactions')
async def resolve_reactome_pathway_reactions(obj, info):
    """
    Resolve Reactome pathway reactions field.
    :param obj: The Reactome pathway object.
    :param info: The GraphQL resolve info.
    :return: The list of reaction concepts.
    """
    data_loader: DataLoader = info.context['data_loader']
    reaction_ids = await data_loader.reactome.pathway_reactions.load(obj['conceptId'])

    return [
        {'conceptId': reaction_id} for reaction_id in reaction_ids
    ]


@REACTOME_REACTION.field('precedingReactions')
async def resolve_reactome_reaction_preceding_reactions(obj, info):
    """
    Resolve Reactome reaction precedingReactions field.
    :param obj: The Reactome reaction object.
    :param info: The GraphQL resolve info.
    :return: The list of preceding reaction concepts.
    """
    data_loader: DataLoader = info.context['data_loader']
    preceding_reaction_ids = await data_loader.reactome.preceding_reaction.load(obj['conceptId'])

    return [
        {'conceptId': preceding_reaction_id} for preceding_reaction_id in preceding_reaction_ids
    ]


@REACTOME_REACTION.field('subsequentReactions')
async def resolve_reactome_reaction_subsequent_reactions(obj, info):
    """
    Resolve Reactome reaction subsequentReactions field.
    :param obj: The Reactome reaction object.
    :param info: The GraphQL resolve info.
    :return: The list of subsequent reaction concepts.
    """
    data_loader: DataLoader = info.context['data_loader']
    subsequent_reaction_ids = await data_loader.reactome.subsequent_reaction.load(obj['conceptId'])

    return [
        {'conceptId': subsequent_reaction_id} for subsequent_reaction_id in subsequent_reaction_ids
    ]


@REACTOME_REACTION.field('inputs')
async def resolve_reactome_reaction_inputs(obj, info):
    """
    Resolve Reactome reaction inputs field.
    :param obj: The Reactome reaction object.
    :param info: The GraphQL resolve info.
    :return: The list of input gene concepts.
    """
    data_loader: DataLoader = info.context['data_loader']
    input_gene_ids = await data_loader.reactome.reaction_inputs.load(obj['conceptId'])

    return [
        {'conceptId': input_gene_id} for input_gene_id in input_gene_ids
    ]


@REACTOME_REACTION.field('outputs')
async def resolve_reactome_reaction_outputs(obj, info):
    """
    Resolve Reactome reaction outputs field.
    :param obj: The Reactome reaction object.
    :param info: The GraphQL resolve info.
    :return: The list of output gene concepts.
    """
    data_loader: DataLoader = info.context['data_loader']
    output_gene_ids = await data_loader.reactome.reaction_outputs.load(obj['conceptId'])

    return [
        {'conceptId': output_gene_id} for output_gene_id in output_gene_ids
    ]


@REACTOME_GENE.field('isInput')
async def resolve_reactome_gene_is_input(obj, info):
    """
    Resolve Reactome gene isInput field.
    :param obj: The Reactome gene object.
    :param info: The GraphQL resolve info.
    :return: The list of reaction concepts where the gene is an input.
    """
    data_loader: DataLoader = info.context['data_loader']
    reaction_ids = await data_loader.reactome.gene_as_input.load(obj['conceptId'])

    return [
        {'conceptId': reaction_id} for reaction_id in reaction_ids
    ]


@REACTOME_GENE.field('isOutput')
async def resolve_reactome_gene_is_output(obj, info):
    """
    Resolve Reactome gene isOutput field.
    :param obj: The Reactome gene object.
    :param info: The GraphQL resolve info.
    :return: The list of reaction concepts where the gene is an output.
    """
    data_loader: DataLoader = info.context['data_loader']
    reaction_ids = await data_loader.reactome.gene_as_output.load(obj['conceptId'])

    return [
        {'conceptId': reaction_id} for reaction_id in reaction_ids
    ]


@REACTOME_GENE.field('symbols')
async def resolve_reactome_gene_symbols(obj, info):
    """
    Resolve Reactome gene symbols field.
    :param obj: The Reactome gene object.
    :param info: The GraphQL resolve info.
    :return: The list of HGNC symbol concepts annotated to this gene.
    """
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.REACTOME,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@REACTOME_PATHWAY.field('similarConcepts')
@REACTOME_REACTION.field('similarConcepts')
@REACTOME_GENE.field('similarConcepts')
async def resolve_reactome_similar_concepts(obj,
                                            info,
                                            threshold: float = 1.0,
                                            ):
    """
    Resolve Reactome concept similarConcepts field.
    :param obj: The Reactome concept object.
    :param info: The GraphQL resolve info.
    :param threshold: The similarity threshold.
    :return: The list of similar concepts.
    """
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.REACTOME,
        threshold=threshold,
    )


@REACTOME_PATHWAY.field('pathsTo')
@REACTOME_REACTION.field('pathsTo')
@REACTOME_GENE.field('pathsTo')
async def resolve_reactome_concept_paths_to(obj,
                                            info,
                                            target_prefix: str,
                                            target_concept_id: str,
                                            relationship: str,
                                            direction: str,
                                            max_depth: int,
                                            ):
    return await resolve_concept_paths_to(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.REACTOME,
        target_prefix=target_prefix,
        target_concept_id=target_concept_id,
        relationship=relationship,
        direction=direction,
        max_depth=max_depth,
    )


@REACTOME_CONCEPT.type_resolver
def reactome_concept_type_resolver(obj, *_):
    """
    Resolve the Reactome concept type.
    :param obj: The Reactome concept object.
    :param _: Additional arguments.
    :return: The GraphQL type name for the Reactome concept.
    """
    concept_type = obj['conceptTypes'][0]

    if concept_type == 'pathway':
        return 'ReactomePathway'
    if concept_type == 'reaction':
        return 'ReactomeReaction'
    if concept_type == 'gene':
        return 'ReactomeGene'

    raise ValueError(f'Unknown Reactome concept type: {concept_type}')


@REACTOME_QUERY.field('reactomeConcept')
async def resolve_get_reactome_concept(_, info, concept_id: str) -> dict:
    """
    Resolve Reactome concept by ID.
    :param _: The parent object, not used.
    :param info: The GraphQL resolve info.
    :param concept_id: The Reactome concept ID.
    :return: The Reactome concept object.
    """
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_QUERY.field('autoComplete')
async def resolve_reactome_autocomplete(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve Reactome auto-complete.
    :param _: The parent object, not used.
    :param info: The GraphQL resolve info.
    :param query: The auto-complete query string.
    :param limit: The maximum number of results to return.
    :return: The auto-complete results.
    """
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.REACTOME,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('reactome')
async def resolve_reactome_query(_, __) -> dict:
    """
    Resolve Reactome query root.
    :param _: The parent object, not used.
    :param __: The GraphQL resolve info, not used.
    :return: An empty dictionary representing the Reactome query root.
    """
    return {}
