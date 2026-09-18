from bioterms.etc.enums import ConceptPrefix
from .ensembl import ENSEMBL_GENE
from .gene import GENE_CONCEPT
from .utils import resolve_concept_annotated_concepts


@ENSEMBL_GENE.field('symbols')
async def resolve_ensembl_gene_symbols(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.ENSEMBL,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )


@GENE_CONCEPT.field('ensemblGenes')
async def resolve_symbol_ensembl_genes(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.ENSEMBL,
    )
