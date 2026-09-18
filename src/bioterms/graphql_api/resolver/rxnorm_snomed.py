from bioterms.etc.enums import ConceptPrefix
from .rxnorm import RXNORM_CONCEPT
from .snomed import SNOMED_CONCEPT
from .utils import resolve_concept_annotated_concepts


@RXNORM_CONCEPT.field('annotatedSnomed')
async def resolve_rxnorm_snomed(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.RXNORM,
        target_prefix=ConceptPrefix.SNOMED,
    )


@SNOMED_CONCEPT.field('annotatedRxNorm')
async def resolve_snomed_rxnorm(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.SNOMED,
        target_prefix=ConceptPrefix.RXNORM,
    )
