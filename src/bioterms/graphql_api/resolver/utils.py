from ariadne import QueryType

from bioterms.etc.enums import ConceptPrefix
from bioterms.vocabulary import get_vocabulary_status
from ..data_loader import DataLoader


GRAPHQL_QUERY_TYPE = QueryType()


def assemble_response(data: dict = None,
                      error_str: str = None,
                      error_code: int = None,
                      ) -> dict:
    """
    Assemble a standardised response dictionary.
    :param data: The data to include in the response.
    :param error_str: The error message, if any.
    :param error_code: The error code, if any.
    :return: A dictionary containing 'data' and 'error' keys.
    """
    error = None
    if error_str is not None:
        if error_code is not None:
            error = {
                'message': error_str,
                'code': error_code
            }
        else:
            error = {
                'message': error_str,
                'code': 400  # Default error code
            }

    return {
        'data': data if data is not None else None,
        'error': error,
    }


@GRAPHQL_QUERY_TYPE.field('loadedPrefixes')
async def resolve_loaded_prefixes(_, info) -> list[str]:
    doc_db = info.context['doc_db']
    graph_db = info.context['graph_db']

    loaded_prefixes = []

    for prefix in ConceptPrefix:
        vocab_status = await get_vocabulary_status(
            prefix,
            doc_db=doc_db,
            graph_db=graph_db,
        )
        if vocab_status.loaded:
            loaded_prefixes.append(prefix.value)

    return loaded_prefixes


async def resolve_concept_info_fields(obj,
                                      info,
                                      prefix: ConceptPrefix,
                                      ):
    concept_id = obj['conceptId']
    field_name = info.field_name
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    concept = await concept_loader.id.load(concept_id)

    if not concept:
        raise ValueError(f'Concept not found for ID: {concept_id} in {prefix.value}')

    return concept[field_name]


async def resolve_concept_replaces(obj,
                                   info,
                                   prefix: ConceptPrefix,
                                   ):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    replaced_ids = await concept_loader.replaced.load(concept_id)

    return [
        {'conceptId': replaced_id} for replaced_id in replaced_ids
    ]


async def resolve_concept_replaced_by(obj,
                                      info,
                                      prefix: ConceptPrefix,
                                      ):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    replacing_ids = await concept_loader.replacement.load(concept_id)

    return [
        {'conceptId': replacing_id} for replacing_id in replacing_ids
    ]


async def resolve_concept_children(obj,
                                   info,
                                   prefix: ConceptPrefix,
                                   ):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    children_ids = await concept_loader.children.load(concept_id)

    return [
        {'conceptId': child_id} for child_id in children_ids
    ]


async def resolve_concept_parents(obj,
                                  info,
                                  prefix: ConceptPrefix,
                                  ):
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    parents_ids = await concept_loader.parents.load(concept_id)

    return [
        {'conceptId': parent_id} for parent_id in parents_ids
    ]


async def resolve_get_concept(info,
                              concept_id: str,
                              prefix: ConceptPrefix,
                              ) -> dict:
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    concept = await concept_loader.id.load(concept_id)

    if not concept:
        return assemble_response(
            error_str=f'Concept not found for ID: {concept_id} in {prefix.value}',
            error_code=404,
        )

    return assemble_response(concept)
