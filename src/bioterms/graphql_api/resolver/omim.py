from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, resolve_get_concept, \
    resolve_concept_similar_concepts, resolve_auto_complete


OMIM_CONCEPT = ObjectType('OmimConcept')
OMIM_QUERY = ObjectType('OmimQuery')


@OMIM_CONCEPT.field('prefix')
@OMIM_CONCEPT.field('label')
@OMIM_CONCEPT.field('synonyms')
@OMIM_CONCEPT.field('status')
async def resolve_omim_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('replaces')
async def resolve_omim_concept_replaces(obj, info):
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('replacedBy')
async def resolve_omim_concept_replaced_by(obj, info):
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('children')
async def resolve_omim_concept_children(obj, info):
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('parents')
async def resolve_omim_concept_parents(obj, info):
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('similarConcepts')
async def resolve_omim_concept_similar_concepts(obj,
                                               info,
                                               threshold: float = 1.0,
                                               ):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
        threshold=threshold,
    )


@OMIM_QUERY.field('omimConcept')
async def resolve_get_omim_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_QUERY.field('autoComplete')
async def resolve_omim_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.OMIM,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('omim')
async def resolve_omim_query(_, __) -> dict:
    return {}

