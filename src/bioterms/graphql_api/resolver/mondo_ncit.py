from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .mondo import MONDO_CONCEPT
from .ncit import NCIT_CONCEPT


@MONDO_CONCEPT.field('annotatedNcit')
async def resolve_mondo_concept_annotated_ncit(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.MONDO,
        target_prefix=ConceptPrefix.NCIT,
    )


@NCIT_CONCEPT.field('annotatedMondo')
async def resolve_ncit_concept_annotated_mondo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.NCIT,
        target_prefix=ConceptPrefix.MONDO,
    )
