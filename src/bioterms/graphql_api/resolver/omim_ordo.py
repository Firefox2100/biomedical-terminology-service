from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .omim import OMIM_CONCEPT
from .ordo import ORDO_CONCEPT


@OMIM_CONCEPT.field('annotatedOrdo')
async def resolve_omim_concept_annotated_ordo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('annotatedOmim')
async def resolve_ordo_concept_annotated_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.ORDO,
        target_prefix=ConceptPrefix.OMIM,
    )
