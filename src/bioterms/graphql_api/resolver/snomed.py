from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, resolve_get_concept, \
    resolve_concept_similar_concepts, resolve_auto_complete


SNOMED_CONCEPT = ObjectType('SnomedConcept')
SNOMED_QUERY = ObjectType('SnomedQuery')


@SNOMED_CONCEPT.field('prefix')
@SNOMED_CONCEPT.field('label')
@SNOMED_CONCEPT.field('definition')
@SNOMED_CONCEPT.field('synonyms')
@SNOMED_CONCEPT.field('fullyDefined')
@SNOMED_CONCEPT.field('status')
async def resolve_snomed_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('replaces')
async def resolve_snomed_concept_replaces(obj, info):
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('replacedBy')
async def resolve_snomed_concept_replaced_by(obj, info):
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('children')
async def resolve_snomed_concept_children(obj, info):
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('parents')
async def resolve_snomed_concept_parents(obj, info):
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('similarConcepts')
async def resolve_snomed_concept_similar_concepts(obj,
                                               info,
                                               threshold: float = 1.0,
                                               ):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.SNOMED,
        threshold=threshold,
    )


@SNOMED_QUERY.field('snomedConcept')
async def resolve_get_snomed_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_QUERY.field('autoComplete')
async def resolve_snomed_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.SNOMED,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('snomed')
async def resolve_snomed_query(_, __) -> dict:
    return {}
