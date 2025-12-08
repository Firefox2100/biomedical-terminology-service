from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .hpo import HPO_CONCEPT
from .ordo import ORDO_CONCEPT


@HPO_CONCEPT.field('annotatedOrdo')
async def resolve_hpo_concept_annotated_ordo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HPO,
        target_prefix=ConceptPrefix.ORDO,
    )


@ORDO_CONCEPT.field('annotatedHpo')
async def resolve_ordo_concept_annotated_hpo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.ORDO,
        target_prefix=ConceptPrefix.HPO,
    )
