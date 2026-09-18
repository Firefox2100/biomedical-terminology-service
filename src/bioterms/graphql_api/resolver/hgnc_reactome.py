from bioterms.etc.enums import ConceptPrefix
from .hgnc import HGNC_CONCEPT
from .reactome import REACTOME_GENE
from .utils import resolve_concept_annotated_concepts


@HGNC_CONCEPT.field('annotatedReactome')
async def resolve_hgnc_reactome(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.HGNC,
        target_prefix=ConceptPrefix.REACTOME,
    )


@REACTOME_GENE.field('annotatedHgnc')
async def resolve_reactome_hgnc(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.REACTOME,
        target_prefix=ConceptPrefix.HGNC,
    )
