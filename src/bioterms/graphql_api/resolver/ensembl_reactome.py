from bioterms.etc.enums import ConceptPrefix
from .ensembl import ENSEMBL_GENE, ENSEMBL_TRANSCRIPT, ENSEMBL_PROTEIN
from .reactome import REACTOME_PATHWAY, REACTOME_REACTION, REACTOME_GENE
from .utils import resolve_concept_annotated_concepts


async def resolve_ensembl_reactome(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.ENSEMBL,
        target_prefix=ConceptPrefix.REACTOME,
    )


async def resolve_reactome_ensembl(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.REACTOME,
        target_prefix=ConceptPrefix.ENSEMBL,
    )


for _object in (ENSEMBL_GENE, ENSEMBL_TRANSCRIPT, ENSEMBL_PROTEIN):
    _object.set_field('annotatedReactome', resolve_ensembl_reactome)

for _object in (REACTOME_PATHWAY, REACTOME_REACTION, REACTOME_GENE):
    _object.set_field('annotatedEnsembl', resolve_reactome_ensembl)
