from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .ohdsi import OHDSI_CONCEPT
from .snomed import SNOMED_CONCEPT


@OHDSI_CONCEPT.field('annotatedSnomed')
async def resolve_ohdsi_concept_annotated_snomed(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.OHDSI,
        target_prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('annotatedOhdsi')
async def resolve_snomed_concept_annotated_ohdsi(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.SNOMED,
        target_prefix=ConceptPrefix.OHDSI,
    )
