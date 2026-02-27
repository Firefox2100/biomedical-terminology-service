from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_concept_children, resolve_concept_parents, \
    resolve_get_concept, resolve_concept_similar_concepts, resolve_concept_paths_to, \
    resolve_auto_complete, resolve_search


OHDSI_CONCEPT = ObjectType('OhdsiConcept')
OHDSI_QUERY = ObjectType('OhdsiQuery')


@OHDSI_CONCEPT.field('prefix')
@OHDSI_CONCEPT.field('label')
@OHDSI_CONCEPT.field('status')
async def resolve_ohdsi_concept_info_fields(obj, info):
    """
    Resolve OHDSI concept info fields.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The requested field value.
    """
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OHDSI,
    )


@OHDSI_CONCEPT.field('replaces')
async def resolve_ohdsi_concept_replaces(obj, info):
    """
    Resolve OHDSI concept replaces field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of replaced concept IDs.
    """
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OHDSI,
    )


@OHDSI_CONCEPT.field('replacedBy')
async def resolve_ohdsi_concept_replaced_by(obj, info):
    """
    Resolve OHDSI concept replacedBy field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of concept IDs that replace this concept.
    """
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OHDSI,
    )


@OHDSI_CONCEPT.field('children')
async def resolve_ohdsi_concept_children(obj, info):
    """
    Resolve OHDSI concept children field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of child concept IDs.
    """
    return await resolve_concept_children(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OHDSI,
    )


@OHDSI_CONCEPT.field('parents')
async def resolve_ohdsi_concept_parents(obj, info):
    """
    Resolve OHDSI concept parents field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :return: The list of parent concept IDs.
    """
    return await resolve_concept_parents(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OHDSI,
    )


@OHDSI_CONCEPT.field('similarConcepts')
async def resolve_ohdsi_concept_similar_concepts(obj,
                                                 info,
                                                 threshold: float = 1.0,
                                                 ):
    """
    Resolve OHDSI concept similarConcepts field.
    :param obj: The GraphQL parent object.
    :param info: The GraphQL resolve info.
    :param threshold: The similarity threshold.
    :return: The list of similar concept IDs.
    """
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.OHDSI,
        threshold=threshold,
    )


@OHDSI_CONCEPT.field('pathsTo')
async def resolve_ohdsi_concept_paths_to(obj,
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
        prefix=ConceptPrefix.OHDSI,
        target_prefix=target_prefix,
        target_concept_id=target_concept_id,
        relationship=relationship,
        direction=direction,
        max_depth=max_depth,
    )


@OHDSI_QUERY.field('ohdsiConcept')
async def resolve_get_ohdsi_concept(_, info, concept_id: str) -> dict:
    """
    Resolve OHDSI concept by ID.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param concept_id: The OHDSI concept ID.
    :return: The OHDSI concept data as a dictionary.
    """
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.OHDSI,
    )


@OHDSI_QUERY.field('autoComplete')
async def resolve_ohdsi_autocomplete(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve OHDSI auto-complete query.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param query: The auto-complete query string.
    :param limit: The maximum number of results to return.
    :return: The auto-complete results as a dictionary.
    """
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.OHDSI,
        limit=limit,
    )


@OHDSI_QUERY.field('search')
async def resolve_ohdsi_search(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve OHDSI search query.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolve info.
    :param query: The search query string.
    :param limit: The maximum number of results to return.
    :return: The search results as a dictionary.
    """
    return await resolve_search(
        info=info,
        query=query,
        prefix=ConceptPrefix.OHDSI,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('ohdsi')
async def resolve_ohdsi_query(_, __) -> dict:
    """
    Resolve OHDSI query root. Returns an empty dict as a placeholder.
    :param _: The GraphQL parent object, not used.
    :param __: The GraphQL resolve info, not used.
    :return: An empty dictionary representing the OHDSI query root.
    """
    return {}

