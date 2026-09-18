from bioterms.etc.enums import ConceptPrefix
from .ncit import NCIT_CONCEPT
from .uberon import UBERON_CONCEPT
from .utils import resolve_concept_annotated_concepts


@NCIT_CONCEPT.field('annotatedUberon')
async def resolve_ncit_concept_annotated_uberon(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.NCIT,
        target_prefix=ConceptPrefix.UBERON,
    )


@UBERON_CONCEPT.field('annotatedNcit')
async def resolve_uberon_concept_annotated_ncit(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UBERON,
        target_prefix=ConceptPrefix.NCIT,
    )
