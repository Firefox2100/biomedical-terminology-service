"""Resolvers for RxNorm."""

from ariadne import ObjectType
from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_auto_complete, resolve_concept_children, \
    resolve_concept_info_fields, resolve_concept_parents, resolve_concept_paths_to, \
    resolve_concept_replaced_by, resolve_concept_replaces, resolve_concept_similar_concepts, \
    resolve_get_concept

RXNORM_CONCEPT = ObjectType('RxNormConcept')
RXNORM_QUERY = ObjectType('RxNormQuery')


@RXNORM_CONCEPT.field('prefix')
@RXNORM_CONCEPT.field('label')
@RXNORM_CONCEPT.field('synonyms')
@RXNORM_CONCEPT.field('conceptTypes')
@RXNORM_CONCEPT.field('status')
async def resolve_info(obj, info):
    return await resolve_concept_info_fields(obj=obj, info=info, prefix=ConceptPrefix.RXNORM)


@RXNORM_CONCEPT.field('replaces')
async def resolve_replaces(obj, info):
    return await resolve_concept_replaces(obj=obj, info=info, prefix=ConceptPrefix.RXNORM)


@RXNORM_CONCEPT.field('replacedBy')
async def resolve_replaced_by(obj, info):
    return await resolve_concept_replaced_by(obj=obj, info=info, prefix=ConceptPrefix.RXNORM)


@RXNORM_CONCEPT.field('children')
async def resolve_children(obj, info):
    return await resolve_concept_children(obj=obj, info=info, prefix=ConceptPrefix.RXNORM)


@RXNORM_CONCEPT.field('parents')
async def resolve_parents(obj, info):
    return await resolve_concept_parents(obj=obj, info=info, prefix=ConceptPrefix.RXNORM)


@RXNORM_CONCEPT.field('similarConcepts')
async def resolve_similar(obj, info, threshold: float = 1.0):
    return await resolve_concept_similar_concepts(
        obj=obj, info=info, prefix=ConceptPrefix.RXNORM, threshold=threshold,
    )


@RXNORM_CONCEPT.field('pathsTo')
async def resolve_paths(obj, info, target_prefix: str, target_concept_id: str,
                        relationship: str, direction: str, max_depth: int):
    return await resolve_concept_paths_to(
        obj=obj, info=info, prefix=ConceptPrefix.RXNORM, target_prefix=target_prefix,
        target_concept_id=target_concept_id, relationship=relationship,
        direction=direction, max_depth=max_depth,
    )


@RXNORM_QUERY.field('rxnormConcept')
async def resolve_get(_, info, concept_id: str):
    return await resolve_get_concept(info=info, concept_id=concept_id, prefix=ConceptPrefix.RXNORM)


@RXNORM_QUERY.field('autoComplete')
async def resolve_autocomplete(_, info, query: str, limit: int = None):
    return await resolve_auto_complete(info=info, query=query, prefix=ConceptPrefix.RXNORM, limit=limit)


@GRAPHQL_QUERY_TYPE.field('rxnorm')
async def resolve_query(_, __):
    return {}
