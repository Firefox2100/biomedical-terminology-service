from bioterms.etc.enums import ConceptPrefix
from .ensembl import ENSEMBL_GENE
from .hgnc import HGNC_CONCEPT
from .utils import resolve_concept_annotated_concepts


@ENSEMBL_GENE.field('annotatedHgnc')
async def resolve_ensembl_gene_annotated_hgnc(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.ENSEMBL,
        target_prefix=ConceptPrefix.HGNC,
    )


@HGNC_CONCEPT.field('annotatedEnsemblGenes')
async def resolve_hgnc_concept_annotated_ensembl_genes(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.HGNC,
        target_prefix=ConceptPrefix.ENSEMBL,
    )
