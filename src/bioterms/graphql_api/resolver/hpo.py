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


@HPO_CONCEPT.field('replaces')
async def resolve_hpo_concept_replaces(obj, info):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    hpo_loader = data_loader.get_concept_loader(ConceptPrefix.HPO)
    replaced_ids = await hpo_loader.replaced.load(concept_id)

    return [
        {'conceptId': replaced_id} for replaced_id in replaced_ids
    ]


@HPO_CONCEPT.field('replacedBy')
async def resolve_hpo_concept_replaced_by(obj, info):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    hpo_loader = data_loader.get_concept_loader(ConceptPrefix.HPO)
    replacing_ids = await hpo_loader.replacement.load(concept_id)

    return [
        {'conceptId': replacing_id} for replacing_id in replacing_ids
    ]


@HPO_CONCEPT.field('children')
async def resolve_hpo_concept_children(obj, info):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    hpo_loader = data_loader.get_concept_loader(ConceptPrefix.HPO)
    children_ids = await hpo_loader.children.load(concept_id)

    return [
        {'conceptId': child_id} for child_id in children_ids
    ]


@HPO_CONCEPT.field('parents')
async def resolve_hpo_concept_parents(obj, info):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    hpo_loader = data_loader.get_concept_loader(ConceptPrefix.HPO)
    parents_ids = await hpo_loader.parents.load(concept_id)

    return [
        {'conceptId': parent_id} for parent_id in parents_ids
    ]


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
