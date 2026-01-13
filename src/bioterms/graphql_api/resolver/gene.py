"""
Resolver for Gene (HGNC Symbol) related GraphQL queries and fields.
"""

from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_get_concept, \
    resolve_concept_similar_concepts, resolve_auto_complete


GENE_CONCEPT = ObjectType('HgncSymbolConcept')
GENE_QUERY = ObjectType('HgncSymbolQuery')


@GENE_CONCEPT.field('prefix')
@GENE_CONCEPT.field('label')
@GENE_CONCEPT.field('status')
async def resolve_gene_concept_info_fields(obj, info):
    """
    Resolve Gene concept info fields.
    :param obj: The Gene concept object.
    :param info: The GraphQL resolve info.
    :return: The value of the requested field.
    """
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_CONCEPT.field('similarConcepts')
async def resolve_gene_concept_similar_concepts(obj,
                                                info,
                                                threshold: float = 1.0,
                                                ):
    """
    Resolve Gene concept similarConcepts field.
    :param obj: The Gene concept object.
    :param info: The GraphQL resolve info.
    :param threshold: The similarity threshold.
    :return: The list of similar concepts.
    """
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HGNC_SYMBOL,
        threshold=threshold,
    )


@GENE_QUERY.field('geneConcept')
async def resolve_get_gene_concept(_, info, concept_id: str) -> dict:
    """
    Resolve get Gene concept by ID.
    :param _: The parent object (not used).
    :param info: The GraphQL resolve info.
    :param concept_id: The Gene concept ID.
    :return: The Gene concept object.
    """
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_QUERY.field('autoComplete')
async def resolve_gene_autocomplete(_, info, query: str, limit: int = None) -> dict:
    """
    Resolve Gene autoComplete query.
    :param _: The parent object (not used).
    :param info: The GraphQL resolve info.
    :param query: The autocomplete query string.
    :param limit: The maximum number of results to return.
    :return: The autocomplete results.
    """
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.HGNC_SYMBOL,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('gene')
async def resolve_ensembl_query(_, __) -> dict:
    """
    Resolve Gene query.
    :param _: The parent object (not used).
    :param __: The GraphQL resolve info (not used).
    :return: An empty dictionary representing the Gene query root.
    """
    return {}
