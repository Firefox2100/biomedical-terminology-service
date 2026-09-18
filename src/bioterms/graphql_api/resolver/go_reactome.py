from bioterms.etc.enums import ConceptPrefix
from .go import GO_CONCEPT
from .reactome import REACTOME_CONCEPT
from .utils import resolve_concept_annotated_concepts


@GO_CONCEPT.field('annotatedReactome')
async def resolve_go_concept_annotated_reactome(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.GO,
        target_prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_CONCEPT.field('annotatedGo')
async def resolve_reactome_concept_annotated_go(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.REACTOME,
        target_prefix=ConceptPrefix.GO,
    )
