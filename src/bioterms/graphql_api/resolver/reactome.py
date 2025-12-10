from ariadne import ObjectType, InterfaceType

from bioterms.etc.enums import ConceptPrefix
from ..data_loader import DataLoader
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_similar_concepts, \
    resolve_get_concept, resolve_auto_complete, resolve_concept_annotated_concepts


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
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_PATHWAY.field('subPathways')
async def resolve_reactome_pathway_sub_pathways(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    sub_pathway_ids = await data_loader.reactome.sub_pathway.load(obj['conceptId'])

    return [
        {'conceptId': sub_pathway_id} for sub_pathway_id in sub_pathway_ids
    ]


@REACTOME_PATHWAY.field('superPathways')
async def resolve_reactome_pathway_super_pathways(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    super_pathway_ids = await data_loader.reactome.super_pathway.load(obj['conceptId'])

    return [
        {'conceptId': super_pathway_id} for super_pathway_id in super_pathway_ids
    ]


@REACTOME_PATHWAY.field('reactions')
async def resolve_reactome_pathway_reactions(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    reaction_ids = await data_loader.reactome.pathway_reactions.load(obj['conceptId'])

    return [
        {'conceptId': reaction_id} for reaction_id in reaction_ids
    ]


@REACTOME_REACTION.field('precedingReactions')
async def resolve_reactome_reaction_preceding_reactions(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    preceding_reaction_ids = await data_loader.reactome.preceding_reaction.load(obj['conceptId'])

    return [
        {'conceptId': preceding_reaction_id} for preceding_reaction_id in preceding_reaction_ids
    ]


@REACTOME_REACTION.field('subsequentReactions')
async def resolve_reactome_reaction_subsequent_reactions(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    subsequent_reaction_ids = await data_loader.reactome.subsequent_reaction.load(obj['conceptId'])

    return [
        {'conceptId': subsequent_reaction_id} for subsequent_reaction_id in subsequent_reaction_ids
    ]


@REACTOME_REACTION.field('inputs')
async def resolve_reactome_reaction_inputs(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    input_gene_ids = await data_loader.reactome.reaction_inputs.load(obj['conceptId'])

    return [
        {'conceptId': input_gene_id} for input_gene_id in input_gene_ids
    ]


@REACTOME_REACTION.field('outputs')
async def resolve_reactome_reaction_outputs(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    output_gene_ids = await data_loader.reactome.reaction_outputs.load(obj['conceptId'])

    return [
        {'conceptId': output_gene_id} for output_gene_id in output_gene_ids
    ]


@REACTOME_GENE.field('isInput')
async def resolve_reactome_gene_is_input(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    reaction_ids = await data_loader.reactome.gene_as_input.load(obj['conceptId'])

    return [
        {'conceptId': reaction_id} for reaction_id in reaction_ids
    ]


@REACTOME_GENE.field('isOutput')
async def resolve_reactome_gene_is_output(obj, info):
    data_loader: DataLoader = info.context['data_loader']
    reaction_ids = await data_loader.reactome.gene_as_output.load(obj['conceptId'])

    return [
        {'conceptId': reaction_id} for reaction_id in reaction_ids
    ]


@REACTOME_GENE.field('symbols')
async def resolve_reactome_gene_symbols(obj, info):
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
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.REACTOME,
        threshold=threshold,
    )


@REACTOME_CONCEPT.type_resolver
def reactome_concept_type_resolver(obj, *_):
    concept_type = obj['conceptTypes'][0]

    if concept_type == 'pathway':
        return 'ReactomePathway'
    elif concept_type == 'reaction':
        return 'ReactomeReaction'
    elif concept_type == 'gene':
        return 'ReactomeGene'
    else:
        raise ValueError(f'Unknown Reactome concept type: {concept_type}')


@REACTOME_QUERY.field('reactomeConcept')
async def resolve_get_reactome_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_QUERY.field('autoComplete')
async def resolve_reactome_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.REACTOME,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('reactome')
async def resolve_reactome_query(_, __) -> dict:
    return {}
