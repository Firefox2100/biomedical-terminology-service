from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .omim import OMIM_CONCEPT
from .gene import GENE_CONCEPT


@OMIM_CONCEPT.field('annotatedGene')
async def resolve_omim_concept_annotated_gene(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_CONCEPT.field('annotatedOmim')
async def resolve_gene_concept_annotated_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.OMIM,
    )
