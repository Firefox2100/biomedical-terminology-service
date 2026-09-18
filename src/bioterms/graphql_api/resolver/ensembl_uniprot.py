from bioterms.etc.enums import ConceptPrefix
from .ensembl import ENSEMBL_PROTEIN
from .uniprot import UNIPROT_CONCEPT
from .utils import resolve_concept_annotated_concepts


@ENSEMBL_PROTEIN.field('uniprotConcepts')
async def resolve_ensembl_uniprot(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.ENSEMBL,
        target_prefix=ConceptPrefix.UNIPROT,
    )


@UNIPROT_CONCEPT.field('ensemblProteins')
async def resolve_uniprot_ensembl(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UNIPROT,
        target_prefix=ConceptPrefix.ENSEMBL,
    )
