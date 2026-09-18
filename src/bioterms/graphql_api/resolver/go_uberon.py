from bioterms.etc.enums import ConceptPrefix
from .go import GO_CONCEPT
from .uberon import UBERON_CONCEPT
from .utils import resolve_concept_annotated_concepts


@GO_CONCEPT.field('annotatedUberon')
async def resolve_go_concept_annotated_uberon(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.GO,
        target_prefix=ConceptPrefix.UBERON,
    )


@UBERON_CONCEPT.field('annotatedGo')
async def resolve_uberon_concept_annotated_go(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UBERON,
        target_prefix=ConceptPrefix.GO,
    )
