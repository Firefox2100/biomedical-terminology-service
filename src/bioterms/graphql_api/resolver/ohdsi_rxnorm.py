from bioterms.etc.enums import ConceptPrefix
from .ohdsi import OHDSI_CONCEPT
from .rxnorm import RXNORM_CONCEPT
from .utils import resolve_concept_annotated_concepts


@OHDSI_CONCEPT.field('annotatedRxNorm')
async def resolve_ohdsi_rxnorm(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.OHDSI,
        target_prefix=ConceptPrefix.RXNORM,
    )


@RXNORM_CONCEPT.field('annotatedOhdsi')
async def resolve_rxnorm_ohdsi(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.RXNORM,
        target_prefix=ConceptPrefix.OHDSI,
    )
