from bioterms.etc.enums import ConceptPrefix
from .go import GO_CONCEPT
from .uniprot import UNIPROT_CONCEPT
from .utils import resolve_concept_annotated_concepts


@GO_CONCEPT.field('annotatedUniProt')
async def resolve_go_concept_annotated_uniprot(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.GO,
        target_prefix=ConceptPrefix.UNIPROT,
    )


@UNIPROT_CONCEPT.field('annotatedGo')
async def resolve_uniprot_concept_annotated_go(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UNIPROT,
        target_prefix=ConceptPrefix.GO,
    )
