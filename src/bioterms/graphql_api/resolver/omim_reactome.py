from bioterms.etc.enums import ConceptPrefix
from .omim import OMIM_CONCEPT
from .reactome import REACTOME_GENE
from .utils import resolve_concept_annotated_concepts


@OMIM_CONCEPT.field('annotatedReactome')
async def resolve_omim_reactome(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.OMIM,
        target_prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_GENE.field('annotatedOmim')
async def resolve_reactome_omim(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.REACTOME,
        target_prefix=ConceptPrefix.OMIM,
    )
