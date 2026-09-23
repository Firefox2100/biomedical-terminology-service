"""Resolvers for the heterogeneous Ensembl genomic-feature vocabulary."""

from ariadne import InterfaceType, ObjectType

from bioterms.etc.enums import ConceptPrefix
from .utils import GRAPHQL_QUERY_TYPE, resolve_auto_complete, resolve_concept_info_fields, \
    resolve_concept_paths_to, resolve_concept_similar_concepts, resolve_get_concept


ENSEMBL_CONCEPT = InterfaceType('EnsemblConcept')
ENSEMBL_GENE = ObjectType('EnsemblGene')
ENSEMBL_TRANSCRIPT = ObjectType('EnsemblTranscript')
ENSEMBL_EXON = ObjectType('EnsemblExon')
ENSEMBL_PROTEIN = ObjectType('EnsemblProtein')
ENSEMBL_QUERY = ObjectType('EnsemblQuery')

_OBJECTS = (ENSEMBL_GENE, ENSEMBL_TRANSCRIPT, ENSEMBL_EXON, ENSEMBL_PROTEIN)


async def resolve_ensembl_info_fields(obj, info):
    return await resolve_concept_info_fields(obj=obj, info=info, prefix=ConceptPrefix.ENSEMBL)


async def resolve_ensembl_similar_concepts(obj, info, threshold: float = 1.0):
    return await resolve_concept_similar_concepts(
        obj=obj, info=info, prefix=ConceptPrefix.ENSEMBL, threshold=threshold,
    )


async def resolve_ensembl_paths_to(obj, info, target_prefix: str, target_concept_id: str,
                                    relationship: str, direction: str, max_depth: int):
    return await resolve_concept_paths_to(
        obj=obj, info=info, prefix=ConceptPrefix.ENSEMBL,
        target_prefix=target_prefix, target_concept_id=target_concept_id,
        relationship=relationship, direction=direction, max_depth=max_depth,
    )


for _object in _OBJECTS:
    for _field in ('prefix', 'label', 'bioType', 'start', 'end', 'sequence', 'version',
                   'strand', 'source', 'status'):
        _object.set_field(_field, resolve_ensembl_info_fields)
    _object.set_field('similarConcepts', resolve_ensembl_similar_concepts)
    _object.set_field('pathsTo', resolve_ensembl_paths_to)

ENSEMBL_TRANSCRIPT.set_field('transcriptSupportLevel', resolve_ensembl_info_fields)


@ENSEMBL_CONCEPT.type_resolver
async def ensembl_concept_type_resolver(obj, info, *_):
    type_names = {
        'gene': 'EnsemblGene',
        'transcript': 'EnsemblTranscript',
        'exon': 'EnsemblExon',
        'protein': 'EnsemblProtein',
    }
    concept_types = obj.get('conceptTypes')
    if not concept_types:
        concept = await info.context['data_loader'].get_concept_loader(
            ConceptPrefix.ENSEMBL,
        ).id.load(obj['conceptId'])
        concept_types = concept['conceptTypes'] if concept else []
    if not concept_types:
        raise ValueError(f'Concept type not found for Ensembl concept {obj["conceptId"]}')
    concept_type = concept_types[0]
    try:
        return type_names[concept_type]
    except KeyError as exc:
        raise ValueError(f'Unknown Ensembl concept type: {concept_type}') from exc


@ENSEMBL_QUERY.field('ensemblConcept')
async def resolve_get_ensembl_concept(_, info, concept_id: str) -> dict:
    return await resolve_get_concept(info=info, concept_id=concept_id, prefix=ConceptPrefix.ENSEMBL)


@ENSEMBL_QUERY.field('autoComplete')
async def resolve_ensembl_autocomplete(_, info, query: str, limit: int = None) -> dict:
    return await resolve_auto_complete(info=info, query=query, prefix=ConceptPrefix.ENSEMBL, limit=limit)


@GRAPHQL_QUERY_TYPE.field('ensembl')
async def resolve_ensembl_query(_, __) -> dict:
    return {}
