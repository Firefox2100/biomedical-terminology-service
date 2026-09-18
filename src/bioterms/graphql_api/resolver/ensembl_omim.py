from bioterms.etc.enums import ConceptPrefix
from .ensembl import ENSEMBL_GENE
from .omim import OMIM_CONCEPT
from .utils import resolve_concept_annotated_concepts


@ENSEMBL_GENE.field('annotatedOmim')
async def resolve_ensembl_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.ENSEMBL,
        target_prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('annotatedEnsemblGenes')
async def resolve_omim_ensembl(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.ENSEMBL,
    )
