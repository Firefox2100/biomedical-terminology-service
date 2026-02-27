from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_get_concept, \
    resolve_concept_similar_concepts, resolve_concept_paths_to, resolve_auto_complete, \
    resolve_concept_annotated_concepts, resolve_search
from .gene import GENE_CONCEPT


ENSEMBL_CONCEPT = ObjectType('EnsemblConcept')
ENSEMBL_QUERY = ObjectType('EnsemblQuery')


@ENSEMBL_CONCEPT.field('prefix')
@ENSEMBL_CONCEPT.field('label')
@ENSEMBL_CONCEPT.field('bioType')
@ENSEMBL_CONCEPT.field('start')
@ENSEMBL_CONCEPT.field('end')
@ENSEMBL_CONCEPT.field('sequence')
@ENSEMBL_CONCEPT.field('status')
async def resolve_ensembl_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ENSEMBL,
    )


@ENSEMBL_CONCEPT.field('conceptType')
async def resolve_ensembl_concept_type(obj, info):
    value = await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ENSEMBL,
    )

    if value:
        return value[0]

    raise ValueError('Concept type not found for Ensembl concept')


@ENSEMBL_CONCEPT.field('symbols')
async def resolve_ensembl_concept_symbols(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.ENSEMBL,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@ENSEMBL_CONCEPT.field('similarConcepts')
async def resolve_ensembl_concept_similar_concepts(obj,
                                                   info,
                                                   threshold: float = 1.0,
                                                   ):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ENSEMBL,
        threshold=threshold,
    )


@ENSEMBL_CONCEPT.field('pathsTo')
async def resolve_ensembl_concept_paths_to(obj,
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
        prefix=ConceptPrefix.ENSEMBL,
        target_prefix=target_prefix,
        target_concept_id=target_concept_id,
        relationship=relationship,
        direction=direction,
        max_depth=max_depth,
    )


@ENSEMBL_QUERY.field('ensemblConcept')
async def resolve_get_ensembl_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.ENSEMBL,
    )


@ENSEMBL_QUERY.field('autoComplete')
async def resolve_ensembl_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.ENSEMBL,
        limit=limit,
    )


@ENSEMBL_QUERY.field('search')
async def resolve_ensembl_search(_, info, query: str, limit: int = None) -> dict:
    return await resolve_search(
        info=info,
        query=query,
        prefix=ConceptPrefix.ENSEMBL,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('ensembl')
async def resolve_ensembl_query(_, __) -> dict:
    return {}


@GENE_CONCEPT.field('ensemblConcepts')
async def resolve_gene_ensembl_concepts(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.ENSEMBL,
    )
