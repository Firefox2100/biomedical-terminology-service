"""
Resolvers for NCIT concepts.
"""

from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_children, \
    resolve_concept_parents, resolve_get_concept, resolve_concept_similar_concepts, \
    resolve_concept_paths_to, resolve_auto_complete, resolve_search


NCIT_CONCEPT = ObjectType('NcitConcept')
NCIT_QUERY = ObjectType('NcitQuery')


@NCIT_CONCEPT.field('prefix')
@NCIT_CONCEPT.field('label')
@NCIT_CONCEPT.field('definition')
@NCIT_CONCEPT.field('synonyms')
@NCIT_CONCEPT.field('status')
async def resolve_ncit_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.NCIT,
    )


@NCIT_CONCEPT.field('children')
async def resolve_ncit_concept_children(obj, info):
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.NCIT,
    )


@NCIT_CONCEPT.field('parents')
async def resolve_ncit_concept_parents(obj, info):
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.NCIT,
    )


@NCIT_CONCEPT.field('similarConcepts')
async def resolve_ncit_concept_similar_concepts(obj,
                                               info,
                                               threshold: float = 1.0,
                                               ):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.NCIT,
        threshold=threshold,
    )


@NCIT_CONCEPT.field('pathsTo')
async def resolve_ncit_concept_paths_to(obj,
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
        prefix=ConceptPrefix.NCIT,
        target_prefix=target_prefix,
        target_concept_id=target_concept_id,
        relationship=relationship,
        direction=direction,
        max_depth=max_depth,
    )


@NCIT_QUERY.field('ncitConcept')
async def resolve_get_ncit_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.NCIT,
    )


@NCIT_QUERY.field('autoComplete')
async def resolve_ncit_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.NCIT,
        limit=limit,
    )


@NCIT_QUERY.field('search')
async def resolve_ncit_search(_, info, query: str, limit: int = None) -> dict:
    return await resolve_search(
        info=info,
        query=query,
        prefix=ConceptPrefix.NCIT,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('ncit')
async def resolve_ncit_query(_, __) -> dict:
    return {}
