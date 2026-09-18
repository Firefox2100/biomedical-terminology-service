from bioterms.etc.enums import ConceptPrefix
from .gene import GENE_CONCEPT
from .uniprot import UNIPROT_CONCEPT
from .utils import resolve_concept_annotated_concepts


@GENE_CONCEPT.field('uniprotConcepts')
async def resolve_symbol_uniprot_concepts(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.HGNC_SYMBOL,
        target_prefix=ConceptPrefix.UNIPROT,
    )


@UNIPROT_CONCEPT.field('symbols')
async def resolve_uniprot_concept_symbols(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UNIPROT,
        target_prefix=ConceptPrefix.HGNC_SYMBOL,
    )
