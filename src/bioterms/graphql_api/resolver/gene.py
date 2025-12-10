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
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HGNC_SYMBOL,
        threshold=threshold,
    )


@GENE_QUERY.field('geneConcept')
async def resolve_get_gene_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_QUERY.field('autoComplete')
async def resolve_gene_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.HGNC_SYMBOL,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('gene')
async def resolve_ensembl_query(_, __) -> dict:
    return {}
