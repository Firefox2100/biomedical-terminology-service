from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .mondo import MONDO_CONCEPT
from .snomed import SNOMED_CONCEPT


@MONDO_CONCEPT.field('annotatedSnomed')
async def resolve_mondo_concept_annotated_snomed(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.MONDO,
        target_prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('annotatedMondo')
async def resolve_snomed_concept_annotated_mondo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.SNOMED,
        target_prefix=ConceptPrefix.MONDO,
    )
