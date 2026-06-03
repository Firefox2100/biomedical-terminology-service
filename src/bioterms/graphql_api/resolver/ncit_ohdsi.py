from bioterms.etc.enums import ConceptPrefix
from .utils import resolve_concept_annotated_concepts
from .ncit import NCIT_CONCEPT
from .ohdsi import OHDSI_CONCEPT


@NCIT_CONCEPT.field('annotatedOhdsi')
async def resolve_ncit_concept_annotated_ohdsi(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.NCIT,
        target_prefix=ConceptPrefix.OHDSI,
    )


@OHDSI_CONCEPT.field('annotatedNcit')
async def resolve_ohdsi_concept_annotated_ncit(obj, info):
    return await resolve_concept_annotated_concepts(
        obj=obj,
        info=info,
        source_prefix=ConceptPrefix.OHDSI,
        target_prefix=ConceptPrefix.NCIT,
    )
