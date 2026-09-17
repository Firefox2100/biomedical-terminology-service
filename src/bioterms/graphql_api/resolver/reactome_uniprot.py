from bioterms.etc.enums import ConceptPrefix
from .reactome import REACTOME_GENE
from .uniprot import UNIPROT_CONCEPT
from .utils import resolve_concept_annotated_concepts


@REACTOME_GENE.field('annotatedUniProt')
async def resolve_reactome_uniprot(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.REACTOME,
        target_prefix=ConceptPrefix.UNIPROT,
    )


@UNIPROT_CONCEPT.field('annotatedReactome')
async def resolve_uniprot_reactome(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UNIPROT,
        target_prefix=ConceptPrefix.REACTOME,
    )
