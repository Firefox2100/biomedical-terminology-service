from bioterms.etc.enums import ConceptPrefix
from .hpo import HPO_CONCEPT
from .omim import OMIM_CONCEPT
from .utils import resolve_concept_annotated_concepts


@HPO_CONCEPT.field('annotatedOmim')
async def resolve_hpo_concept_annotated_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.HPO,
        target_prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('annotatedHpo')
async def resolve_omim_concept_annotated_hpo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.HPO,
    )
