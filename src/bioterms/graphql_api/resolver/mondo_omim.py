from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .mondo import MONDO_CONCEPT
from .omim import OMIM_CONCEPT


@MONDO_CONCEPT.field('annotatedOmim')
async def resolve_mondo_concept_annotated_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.MONDO,
        target_prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('annotatedMondo')
async def resolve_omim_concept_annotated_mondo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.MONDO,
    )
