from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .ncit import NCIT_CONCEPT
from .gene import GENE_CONCEPT


@NCIT_CONCEPT.field('annotatedGene')
async def resolve_ncit_concept_annotated_gene(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.NCIT,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_CONCEPT.field('annotatedNcit')
async def resolve_gene_concept_annotated_ncit(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.NCIT,
    )
