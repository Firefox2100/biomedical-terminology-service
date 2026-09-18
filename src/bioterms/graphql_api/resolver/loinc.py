"""Resolvers for LOINC concepts and queries."""

from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_auto_complete, resolve_concept_children, \
    resolve_concept_info_fields, resolve_concept_parents, resolve_concept_paths_to, \
    resolve_concept_replaced_by, resolve_concept_replaces, resolve_concept_similar_concepts, \
    resolve_get_concept, resolve_search


LOINC_CONCEPT = ObjectType('LoincConcept')
LOINC_QUERY = ObjectType('LoincQuery')


@LOINC_CONCEPT.field('prefix')
@LOINC_CONCEPT.field('label')
@LOINC_CONCEPT.field('definition')
@LOINC_CONCEPT.field('status')
async def resolve_loinc_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(obj=obj, info=info, prefix=ConceptPrefix.LOINC)


@LOINC_CONCEPT.field('replaces')
async def resolve_loinc_concept_replaces(obj, info):
    return await resolve_concept_replaces(obj=obj, info=info, prefix=ConceptPrefix.LOINC)


@LOINC_CONCEPT.field('replacedBy')
async def resolve_loinc_concept_replaced_by(obj, info):
    return await resolve_concept_replaced_by(obj=obj, info=info, prefix=ConceptPrefix.LOINC)


@LOINC_CONCEPT.field('children')
async def resolve_loinc_concept_children(obj, info):
    return await resolve_concept_children(obj=obj, info=info, prefix=ConceptPrefix.LOINC)


@LOINC_CONCEPT.field('parents')
async def resolve_loinc_concept_parents(obj, info):
    return await resolve_concept_parents(obj=obj, info=info, prefix=ConceptPrefix.LOINC)


@LOINC_CONCEPT.field('similarConcepts')
async def resolve_loinc_concept_similar_concepts(obj, info, threshold: float = 1.0):
    return await resolve_concept_similar_concepts(
        obj=obj, info=info, prefix=ConceptPrefix.LOINC, threshold=threshold,
    )


@LOINC_CONCEPT.field('pathsTo')
async def resolve_loinc_concept_paths_to(obj, info, target_prefix: str,
                                         target_concept_id: str, relationship: str,
                                         direction: str, max_depth: int):
    return await resolve_concept_paths_to(
        obj=obj, info=info, prefix=ConceptPrefix.LOINC, target_prefix=target_prefix,
        target_concept_id=target_concept_id, relationship=relationship,
        direction=direction, max_depth=max_depth,
    )


@LOINC_QUERY.field('loincConcept')
async def resolve_get_loinc_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info, concept_id=concept_id, prefix=ConceptPrefix.LOINC,
    )


@LOINC_QUERY.field('autoComplete')
async def resolve_loinc_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info, query=query, prefix=ConceptPrefix.LOINC, limit=limit,
    )


@LOINC_QUERY.field('search')
async def resolve_loinc_search(_, info, query: str, limit: int = None) -> dict:
    return await resolve_search(info=info, query=query, prefix=ConceptPrefix.LOINC, limit=limit)


@GRAPHQL_QUERY_TYPE.field('loinc')
async def resolve_loinc_query(_, __) -> dict:
    return {}
