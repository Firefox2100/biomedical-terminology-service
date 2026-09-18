from bioterms.etc.enums import ConceptPrefix
from .snomed import SNOMED_CONCEPT
from .uberon import UBERON_CONCEPT
from .utils import resolve_concept_annotated_concepts


@SNOMED_CONCEPT.field('annotatedUberon')
async def resolve_snomed_concept_annotated_uberon(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.SNOMED,
        target_prefix=ConceptPrefix.UBERON,
    )


@UBERON_CONCEPT.field('annotatedSnomed')
async def resolve_uberon_concept_annotated_snomed(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UBERON,
        target_prefix=ConceptPrefix.SNOMED,
    )
