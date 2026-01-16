from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .ordo import ORDO_CONCEPT
from .snomed import SNOMED_CONCEPT


@ORDO_CONCEPT.field('annotatedSnomed')
async def resolve_ordo_concept_annotated_snomed(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.ORDO,
        target_prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('annotatedOrdo')
async def resolve_snomed_concept_annotated_ordo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.SNOMED,
        target_prefix=ConceptPrefix.ORDO,
    )
