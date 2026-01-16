from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .hpo import HPO_CONCEPT
from .gene import GENE_CONCEPT


@HPO_CONCEPT.field('annotatedGene')
async def resolve_hpo_concept_annotated_gene(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HPO,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_CONCEPT.field('annotatedHpo')
async def resolve_gene_concept_annotated_hpo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.HPO,
    )
