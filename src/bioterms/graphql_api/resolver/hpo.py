from ariadne import ObjectType

from bioterms.etc.enums import ConceptPrefix
from ..data_loader import DataLoader
from .utils import GRAPHQL_QUERY_TYPE, assemble_response


HPO_CONCEPT = ObjectType('HpoConcept')
HPO_QUERY = ObjectType('HpoQuery')


@HPO_CONCEPT.field('prefix')
@HPO_CONCEPT.field('label')
@HPO_CONCEPT.field('definition')
@HPO_CONCEPT.field('comment')
@HPO_CONCEPT.field('status')
async def resolve_hpo_concept_info_fields(obj, info):
    concept_id = obj['conceptId']
    field_name = info.field_name
    data_loader: DataLoader = info.context['data_loader']

    hpo_loader = data_loader.get_concept_loader(ConceptPrefix.HPO)
    hpo_concept = await hpo_loader.id.load(concept_id)

    if not hpo_concept:
        raise ValueError('HPO concept not found')

    return hpo_concept[field_name]


@HPO_QUERY.field('hpoConcept')
async def resolve_get_hpo_concept(_, info, concept_id: str) -> dict:
    data_loader: DataLoader = info.context['data_loader']

    hpo_loader = data_loader.get_concept_loader(ConceptPrefix.HPO)
    hpo_concept = await hpo_loader.id.load(concept_id)

    if not hpo_concept:
        return assemble_response(
            error_str='HPO concept not found',
            error_code=404,
        )

    return assemble_response(hpo_concept)


@GRAPHQL_QUERY_TYPE.field('hpo')
async def resolve_hpo_query(_, __) -> dict:
    return {}
