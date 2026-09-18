from bioterms.etc.enums import ConceptPrefix
from .loinc import LOINC_CONCEPT
from .rxnorm import RXNORM_CONCEPT
from .utils import resolve_concept_annotated_concepts


@LOINC_CONCEPT.field('annotatedRxNorm')
async def resolve_loinc_rxnorm(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.LOINC,
        target_prefix=ConceptPrefix.RXNORM,
    )


@RXNORM_CONCEPT.field('annotatedLoinc')
async def resolve_rxnorm_loinc(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.RXNORM,
        target_prefix=ConceptPrefix.LOINC,
    )
