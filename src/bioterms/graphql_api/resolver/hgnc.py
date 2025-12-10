from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_concept_info_fields, resolve_concept_replaces, \
    resolve_concept_replaced_by, resolve_get_concept, resolve_concept_similar_concepts, \
    resolve_auto_complete, resolve_concept_annotated_concepts
from .gene import GENE_CONCEPT


HGNC_CONCEPT = ObjectType('HgncConcept')
HGNC_QUERY = ObjectType('HgncQuery')


@HGNC_CONCEPT.field('prefix')
@HGNC_CONCEPT.field('label')
@HGNC_CONCEPT.field('definition')
@HGNC_CONCEPT.field('synonyms')
@HGNC_CONCEPT.field('location')
@HGNC_CONCEPT.field('status')
async def resolve_hgnc_concept_info_fields(obj, info):
    return await resolve_concept_info_fields(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HGNC,
    )


@HGNC_CONCEPT.field('replaces')
async def resolve_hgnc_concept_replaces(obj, info):
    return await resolve_concept_replaces(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HGNC,
    )


@HGNC_CONCEPT.field('replacedBy')
async def resolve_hgnc_concept_replaced_by(obj, info):
    return await resolve_concept_replaced_by(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HGNC,
    )


@HGNC_CONCEPT.field('symbols')
async def resolve_hgnc_concept_symbols(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@HGNC_CONCEPT.field('similarConcepts')
async def resolve_hgnc_concept_similar_concepts(obj,
                                                info,
                                                threshold: float = 1.0,
                                                ):
    return await resolve_concept_similar_concepts(
        obj=obj,
        info=info,
        prefix=ConceptPrefix.HGNC,
        threshold=threshold,
    )


@HGNC_QUERY.field('hgncConcept')
async def resolve_get_hgnc_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(
        info=info,
        concept_id=concept_id,
        prefix=ConceptPrefix.HGNC,
    )


@HGNC_QUERY.field('autoComplete')
async def resolve_hgnc_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(
        info=info,
        query=query,
        prefix=ConceptPrefix.HGNC,
        limit=limit,
    )


@GRAPHQL_QUERY_TYPE.field('hgnc')
async def resolve_hgnc_query(_, __) -> dict:
    return {}


@GENE_CONCEPT.field('hgncConcepts')
async def resolve_gene_hgnc_concepts(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.HGNC,
    )
