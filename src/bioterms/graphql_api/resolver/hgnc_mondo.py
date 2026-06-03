from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .hgnc import HGNC_CONCEPT
from .mondo import MONDO_CONCEPT


@HGNC_CONCEPT.field('annotatedMondo')
async def resolve_hgnc_concept_annotated_mondo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.HGNC,
        target_prefix=ConceptPrefix.MONDO,
    )


@MONDO_CONCEPT.field('annotatedHgnc')
async def resolve_mondo_concept_annotated_hgnc(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.MONDO,
        target_prefix=ConceptPrefix.HGNC,
    )
