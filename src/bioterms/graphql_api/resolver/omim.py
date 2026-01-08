"""
OMIM GraphQL Resolvers
"""

from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, \
    resolve_get_concept, resolve_concept_similar_concepts, resolve_auto_complete


OMIM_CONCEPT = ObjectType('OmimConcept')
OMIM_QUERY = ObjectType('OmimQuery')


@OMIM_CONCEPT.field('prefix')
@OMIM_CONCEPT.field('label')
@OMIM_CONCEPT.field('synonyms')
@OMIM_CONCEPT.field('status')
async def resolve_omim_concept_info_fields(obj, info):
    """
    Resolve OMIM concept info fields.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The requested field value.
    """
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('replaces')
async def resolve_omim_concept_replaces(obj, info):
    """
    Resolve OMIM concept replaces field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of replaced concept IDs.
    """
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('replacedBy')
async def resolve_omim_concept_replaced_by(obj, info):
    """
    Resolve OMIM concept replacedBy field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of concept IDs that replace this concept.
    """
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('children')
async def resolve_omim_concept_children(obj, info):
    """
    Resolve OMIM concept children field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of child concepts.
    """
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('parents')
async def resolve_omim_concept_parents(obj, info):
    """
    Resolve OMIM concept parents field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of parent concepts.
    """
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
    """
    Resolve OMIM concept similarConcepts field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :param threshold: The similarity threshold.
    :return: The list of similar concepts above the threshold.
    """
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OMIM,
        threshold=threshold,
    )


@OMIM_QUERY.field('omimConcept')
async def resolve_get_omim_concept(_, info, concept_id: str) -> dict:
    """
    Resolve get OMIM concept query.
    :param _: The GraphQL parent object. not used.
    :param info: The GraphQL resolve info.
    :param concept_id: The OMIM concept ID.
    :return: The OMIM concept data as a dictionary.
    """
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.OMIM,
    )


@OMIM_QUERY.field('autoComplete')
async def resolve_omim_autocomplete(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve OMIM autocomplete query.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param query: The autocomplete query string.
    :param limit: The maximum number of results to return.
    :return: The autocomplete results as a dictionary.
    """
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.OMIM,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('omim')
async def resolve_omim_query(_, __) -> dict:
    """
    Resolve OMIM query root.
    :param _: The GraphQL parent object, not used.
    :param __: The GraphQL resolve info, not used.
    :return: An empty dictionary representing the OMIM query root.
    """
    return {}
