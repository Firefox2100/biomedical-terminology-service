"""
Resolvers for HPO concepts and queries.
"""

from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, \
    resolve_get_concept, resolve_concept_similar_concepts, resolve_auto_complete


HPO_CONCEPT = ObjectType('HpoConcept')
HPO_QUERY = ObjectType('HpoQuery')


@HPO_CONCEPT.field('prefix')
@HPO_CONCEPT.field('label')
@HPO_CONCEPT.field('definition')
@HPO_CONCEPT.field('comment')
@HPO_CONCEPT.field('status')
async def resolve_hpo_concept_info_fields(obj, info):
    """
    Resolve HPO concept info fields.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The requested field value.
    """
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HPO,
    )


@HPO_CONCEPT.field('replaces')
async def resolve_hpo_concept_replaces(obj, info):
    """
    Resolve HPO concept replaces field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of replaced concept IDs.
    """
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HPO,
    )


@HPO_CONCEPT.field('replacedBy')
async def resolve_hpo_concept_replaced_by(obj, info):
    """
    Resolve HPO concept replacedBy field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of concept IDs that replace this concept.
    """
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HPO,
    )


@HPO_CONCEPT.field('children')
async def resolve_hpo_concept_children(obj, info):
    """
    Resolve HPO concept children field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of child concept IDs.
    """
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HPO,
    )


@HPO_CONCEPT.field('parents')
async def resolve_hpo_concept_parents(obj, info):
    """
    Resolve HPO concept parents field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of parent concept IDs.
    """
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HPO,
    )


@HPO_CONCEPT.field('similarConcepts')
async def resolve_hpo_concept_similar_concepts(obj,
                                               info,
                                               threshold: float = 1.0,
                                               ):
    """
    Resolve HPO concept similarConcepts field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :param threshold: The similarity threshold.
    :return: The list of similar concept IDs.
    """
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HPO,
        threshold=threshold,
    )


@HPO_QUERY.field('hpoConcept')
async def resolve_get_hpo_concept(_, info, concept_id: str) -> dict:
    """
    Resolve HPO concept by ID.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param concept_id: The HPO concept ID.
    :return: The HPO concept data as a dictionary.
    """
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.HPO,
    )


@HPO_QUERY.field('autoComplete')
async def resolve_hpo_autocomplete(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve HPO auto-complete query.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param query: The auto-complete query string.
    :param limit: The maximum number of results to return.
    :return: The auto-complete results as a dictionary.
    """
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.HPO,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('hpo')
async def resolve_hpo_query(_, __) -> dict:
    """
    Resolve HPO query root. Returns an empty dict as a placeholder.
    :param _: The GraphQL parent object, not used.
    :param __: The GraphQL resolve info, not used.
    :return: An empty dictionary representing the HPO query root.
    """
    return {}
