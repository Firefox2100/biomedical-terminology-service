from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, resolve_get_concept, \
    resolve_concept_similar_concepts, resolve_concept_paths_to, resolve_auto_complete, resolve_search


CTV3_CONCEPT = ObjectType('Ctv3Concept')
CTV3_QUERY = ObjectType('Ctv3Query')


@CTV3_CONCEPT.field('prefix')
@CTV3_CONCEPT.field('label')
@CTV3_CONCEPT.field('definition')
@CTV3_CONCEPT.field('synonyms')
@CTV3_CONCEPT.field('status')
async def resolve_ctv3_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.CTV3,
    )


@CTV3_CONCEPT.field('replaces')
async def resolve_ctv3_concept_replaces(obj, info):
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.CTV3,
    )


@CTV3_CONCEPT.field('replacedBy')
async def resolve_ctv3_concept_replaced_by(obj, info):
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.CTV3,
    )


@CTV3_CONCEPT.field('children')
async def resolve_ctv3_concept_children(obj, info):
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.CTV3,
    )


@CTV3_CONCEPT.field('parents')
async def resolve_ctv3_concept_parents(obj, info):
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.CTV3,
    )


@CTV3_CONCEPT.field('similarConcepts')
async def resolve_ctv3_concept_similar_concepts(obj,
                                                info,
                                                threshold: float = 1.0,
                                                ):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.CTV3,
        threshold=threshold,
    )


@CTV3_CONCEPT.field('pathsTo')
async def resolve_ctv3_concept_paths_to(obj,
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
        prefix=ConceptPrefix.CTV3,
        target_prefix=target_prefix,
        target_concept_id=target_concept_id,
        relationship=relationship,
        direction=direction,
        max_depth=max_depth,
    )


@CTV3_QUERY.field('ctv3Concept')
async def resolve_get_ctv3_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.CTV3,
    )


@CTV3_QUERY.field('autoComplete')
async def resolve_ctv3_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.CTV3,
        limit=limit,
    )


@CTV3_QUERY.field('search')
async def resolve_ctv3_search(_, info, query: str, limit: int = None) -> dict:
    return await resolve_search(
        info=info,
        query=query,
        prefix=ConceptPrefix.CTV3,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('ctv3')
async def resolve_ctv3_query(_, __) -> dict:
    return {}

