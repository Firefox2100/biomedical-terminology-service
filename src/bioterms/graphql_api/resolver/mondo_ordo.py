from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .mondo import MONDO_CONCEPT
from .ordo import ORDO_CONCEPT


@MONDO_CONCEPT.field('annotatedOrdo')
async def resolve_mondo_concept_annotated_ordo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.MONDO,
        target_prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('annotatedMondo')
async def resolve_ordo_concept_annotated_mondo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.ORDO,
        target_prefix=ConceptPrefix.MONDO,
    )
