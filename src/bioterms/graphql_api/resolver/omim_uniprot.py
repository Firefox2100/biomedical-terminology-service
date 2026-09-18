from bioterms.etc.enums import ConceptPrefix
from .omim import OMIM_CONCEPT
from .uniprot import UNIPROT_CONCEPT
from .utils import resolve_concept_annotated_concepts


@OMIM_CONCEPT.field('annotatedUniProt')
async def resolve_omim_uniprot(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.UNIPROT,
    )


@UNIPROT_CONCEPT.field('annotatedOmim')
async def resolve_uniprot_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UNIPROT,
        target_prefix=ConceptPrefix.OMIM,
    )
