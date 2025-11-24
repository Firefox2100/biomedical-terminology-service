from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from ..data_loader import DataLoader
from .utils import GRAPHQL_QUERY_TYPE, assemble_response


ORDO_CONCEPT = ObjectType('OrdoConcept')
ORDO_QUERY = ObjectType('OrdoQuery')


@ORDO_CONCEPT.field('prefix')
@ORDO_CONCEPT.field('label')
@ORDO_CONCEPT.field('definition')
@ORDO_CONCEPT.field('synonyms')
@ORDO_CONCEPT.field('status')
async def resolve_ordo_concept_info_fields(obj, info):
    concept_id = obj['conceptId']
    field_name = info.field_name
    data_loader: DataLoader = info.context['data_loader']

    ordo_loader = data_loader.get_concept_loader(ConceptPrefix.ORDO)
    ordo_concept = await ordo_loader.id.load(concept_id)

    if not ordo_concept:
        raise ValueError('HPO concept not found')

    return ordo_concept[field_name]


@ORDO_QUERY.field('ordoConcept')
async def resolve_get_ordo_concept(_, info, concept_id: str) -> dict:
    data_loader: DataLoader = info.context['data_loader']

    ordo_loader = data_loader.get_concept_loader(ConceptPrefix.ORDO)
    ordo_concept = await ordo_loader.id.load(concept_id)

    if not ordo_concept:
        return assemble_response(
            error_str='HPO concept not found',
            error_code=404,
        )

    return assemble_response(ordo_concept)


@GRAPHQL_QUERY_TYPE.field('ordo')
async def resolve_ordo_query(_, __) -> dict:
    return {}
