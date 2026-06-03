from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .hpo import HPO_CONCEPT
from .mondo import MONDO_CONCEPT


@HPO_CONCEPT.field('annotatedMondo')
async def resolve_hpo_concept_annotated_mondo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HPO,
        target_prefix=ConceptPrefix.MONDO,
    )


@MONDO_CONCEPT.field('annotatedHpo')
async def resolve_mondo_concept_annotated_hpo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.MONDO,
        target_prefix=ConceptPrefix.HPO,
    )
