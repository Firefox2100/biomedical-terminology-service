from bioterms.etc.enums import ConceptPrefix
from .ordo import ORDO_CONCEPT
from .uniprot import UNIPROT_CONCEPT
from .utils import resolve_concept_annotated_concepts


@ORDO_CONCEPT.field('annotatedUniProt')
async def resolve_ordo_uniprot(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.ORDO,
        target_prefix=ConceptPrefix.UNIPROT,
    )


@UNIPROT_CONCEPT.field('annotatedOrdo')
async def resolve_uniprot_ordo(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj, info=info, source_prefix=ConceptPrefix.UNIPROT,
        target_prefix=ConceptPrefix.ORDO,
    )
