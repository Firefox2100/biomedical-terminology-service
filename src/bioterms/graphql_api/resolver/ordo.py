from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, resolve_get_concept, \
    resolve_concept_similar_concepts, resolve_auto_complete


ORDO_CONCEPT = ObjectType('OrdoConcept')
ORDO_QUERY = ObjectType('OrdoQuery')


@ORDO_CONCEPT.field('prefix')
@ORDO_CONCEPT.field('label')
@ORDO_CONCEPT.field('definition')
@ORDO_CONCEPT.field('synonyms')
@ORDO_CONCEPT.field('status')
async def resolve_ordo_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('replaces')
async def resolve_ordo_concept_replaces(obj, info):
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('replacedBy')
async def resolve_ordo_concept_replaced_by(obj, info):
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('children')
async def resolve_ordo_concept_children(obj, info):
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('parents')
async def resolve_ordo_concept_parents(obj, info):
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('similarConcepts')
async def resolve_ordo_concept_similar_concepts(obj,
                                                info,
                                                threshold: float = 1.0,
                                                ):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.ORDO,
        threshold=threshold,
    )


@ORDO_QUERY.field('ordoConcept')
async def resolve_get_ordo_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.ORDO,
    )


@ORDO_QUERY.field('autoComplete')
async def resolve_ordo_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.ORDO,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('ordo')
async def resolve_ordo_query(_, __) -> dict:
    return {}
