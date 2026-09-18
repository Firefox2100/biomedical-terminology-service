from bioterms.etc.enums import ConceptPrefix
from .hgnc import HGNC_CONCEPT
from .omim import OMIM_CONCEPT
from .utils import resolve_concept_annotated_concepts


@HGNC_CONCEPT.field('annotatedOmim')
async def resolve_hgnc_concept_annotated_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.HGNC,
        target_prefix=ConceptPrefix.OMIM,
    )


@OMIM_CONCEPT.field('annotatedHgnc')
async def resolve_omim_concept_annotated_hgnc(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.HGNC,
    )
