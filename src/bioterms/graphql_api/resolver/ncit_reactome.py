from bioterms.etc.enums import ConceptPrefix
from .ncit import NCIT_CONCEPT
from .reactome import REACTOME_PHYSICAL_ENTITY
from .utils import resolve_concept_annotated_concepts


@NCIT_CONCEPT.field('annotatedReactome')
async def resolve_ncit_reactome(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.NCIT,
        target_prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_PHYSICAL_ENTITY.field('annotatedNcit')
async def resolve_reactome_ncit(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.REACTOME,
        target_prefix=ConceptPrefix.NCIT,
    )
