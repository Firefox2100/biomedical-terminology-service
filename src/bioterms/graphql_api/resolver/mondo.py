"""
Resolvers for Mondo concepts and queries.
"""

from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_children, \
    resolve_concept_parents, resolve_get_concept, resolve_concept_similar_concepts, \
    resolve_concept_paths_to, resolve_auto_complete, resolve_search


MONDO_CONCEPT = ObjectType('MondoConcept')
MONDO_QUERY = ObjectType('MondoQuery')


@MONDO_CONCEPT.field('prefix')
@MONDO_CONCEPT.field('label')
@MONDO_CONCEPT.field('definition')
@MONDO_CONCEPT.field('synonyms')
@MONDO_CONCEPT.field('comment')
@MONDO_CONCEPT.field('status')
async def resolve_mondo_concept_info_fields(obj, info):
    """
    Resolve Mondo concept info fields.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The requested field value.
    """
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.MONDO,
    )


@MONDO_CONCEPT.field('children')
async def resolve_mondo_concept_children(obj, info):
    """
    Resolve Mondo concept children field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of child concept IDs.
    """
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.MONDO,
    )


@MONDO_CONCEPT.field('parents')
async def resolve_mondo_concept_parents(obj, info):
    """
    Resolve Mondo concept parents field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of parent concept IDs.
    """
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.MONDO,
    )


@MONDO_CONCEPT.field('similarConcepts')
async def resolve_mondo_concept_similar_concepts(obj,
                                                 info,
                                                 threshold: float = 1.0,
                                                 ):
    """
    Resolve Mondo concept similarConcepts field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :param threshold: The similarity threshold.
    :return: The list of similar concept IDs.
    """
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.MONDO,
        threshold=threshold,
    )


@MONDO_CONCEPT.field('pathsTo')
async def resolve_mondo_concept_paths_to(obj,
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
        prefix=ConceptPrefix.MONDO,
        target_prefix=target_prefix,
        target_concept_id=target_concept_id,
        relationship=relationship,
        direction=direction,
        max_depth=max_depth,
    )


@MONDO_QUERY.field('mondoConcept')
async def resolve_get_mondo_concept(_, info, concept_id: str) -> dict:
    """
    Resolve Mondo concept by ID.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param concept_id: The Mondo concept ID.
    :return: The Mondo concept data as a dictionary.
    """
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.MONDO,
    )


@MONDO_QUERY.field('autoComplete')
async def resolve_mondo_autocomplete(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve Mondo auto-complete query.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param query: The auto-complete query string.
    :param limit: The maximum number of results to return.
    :return: The auto-complete results as a dictionary.
    """
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.MONDO,
        limit=limit,
    )


@MONDO_QUERY.field('search')
async def resolve_mondo_search(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve Mondo search query.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param query: The search query string.
    :param limit: The maximum number of results to return.
    :return: The search results as a dictionary.
    """
    return await resolve_search(
        info=info,
        query=query,
        prefix=ConceptPrefix.MONDO,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('mondo')
async def resolve_mondo_query(_, __) -> dict:
    """
    Resolve Mondo query root. Returns an empty dict as a placeholder.
    :param _: The GraphQL parent object, not used.
    :param __: The GraphQL resolve info, not used.
    :return: An empty dictionary representing the Mondo query root.
    """
    return {}
