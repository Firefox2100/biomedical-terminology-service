from bioterms.etc.enums import ConceptPrefix
from .loinc import LOINC_CONCEPT
from .snomed import SNOMED_CONCEPT
from .utils import resolve_concept_annotated_concepts


@LOINC_CONCEPT.field('annotatedSnomed')
async def resolve_loinc_concept_annotated_snomed(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.LOINC,
        target_prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('annotatedLoinc')
async def resolve_snomed_concept_annotated_loinc(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.SNOMED,
        target_prefix=ConceptPrefix.LOINC,
    )
