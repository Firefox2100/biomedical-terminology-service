from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .ctv3 import CTV3_CONCEPT
from .snomed import SNOMED_CONCEPT


@CTV3_CONCEPT.field('annotatedSnomed')
async def resolve_ctv3_concept_annotated_snomed(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.CTV3,
        target_prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('annotatedCtv3')
async def resolve_snomed_concept_annotated_ctv3(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.SNOMED,
        target_prefix=ConceptPrefix.CTV3,
    )
