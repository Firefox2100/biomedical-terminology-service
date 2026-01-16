from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .ordo import ORDO_CONCEPT
from .gene import GENE_CONCEPT


@ORDO_CONCEPT.field('annotatedGene')
async def resolve_ordo_concept_annotated_gene(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.ORDO,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_CONCEPT.field('annotatedOrdo')
async def resolve_gene_concept_annotated_ordo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.ORDO,
    )
