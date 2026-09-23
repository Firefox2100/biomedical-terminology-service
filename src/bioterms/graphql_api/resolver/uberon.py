"""Resolvers for Uberon concepts and queries."""

from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, \
    resolve_get_concept, resolve_concept_similar_concepts, resolve_concept_paths_to, \
    resolve_auto_complete


UBERON_CONCEPT = ObjectType('UberonConcept')
UBERON_QUERY = ObjectType('UberonQuery')


@UBERON_CONCEPT.field('prefix')
@UBERON_CONCEPT.field('label')
@UBERON_CONCEPT.field('definition')
@UBERON_CONCEPT.field('comment')
@UBERON_CONCEPT.field('status')
async def resolve_uberon_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(obj=obj, info=info, prefix=ConceptPrefix.UBERON)


@UBERON_CONCEPT.field('replaces')
async def resolve_uberon_concept_replaces(obj, info):
    return await resolve_concept_replaces(obj=obj, info=info, prefix=ConceptPrefix.UBERON)


@UBERON_CONCEPT.field('replacedBy')
async def resolve_uberon_concept_replaced_by(obj, info):
    return await resolve_concept_replaced_by(obj=obj, info=info, prefix=ConceptPrefix.UBERON)


@UBERON_CONCEPT.field('children')
async def resolve_uberon_concept_children(obj, info):
    return await resolve_concept_children(obj=obj, info=info, prefix=ConceptPrefix.UBERON)


@UBERON_CONCEPT.field('parents')
async def resolve_uberon_concept_parents(obj, info):
    return await resolve_concept_parents(obj=obj, info=info, prefix=ConceptPrefix.UBERON)


@UBERON_CONCEPT.field('similarConcepts')
async def resolve_uberon_concept_similar_concepts(obj, info, threshold: float = 1.0):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.UBERON,
        threshold=threshold,
    )


@UBERON_CONCEPT.field('pathsTo')
async def resolve_uberon_concept_paths_to(obj, info, target_prefix: str,
                                          target_concept_id: str, relationship: str,
                                          direction: str, max_depth: int):
    return await resolve_concept_paths_to(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.UBERON,
        target_prefix=target_prefix,
        target_concept_id=target_concept_id,
        relationship=relationship,
        direction=direction,
        max_depth=max_depth,
    )


@UBERON_QUERY.field('uberonConcept')
async def resolve_get_uberon_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.UBERON,
    )


@UBERON_QUERY.field('autoComplete')
async def resolve_uberon_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.UBERON,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('uberon')
async def resolve_uberon_query(_, __) -> dict:
    return {}
