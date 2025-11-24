from ariadne import QueryType

from bioterms.etc.enums import ConceptPrefix
from bioterms.vocabulary import get_vocabulary_status


GRAPHQL_QUERY_TYPE = QueryType()


def assemble_response(data: dict = None,
                      error_str: str = None,
                      error_code: int = None,
                      ) -> dict:
    """
    Assemble a standardized response dictionary.
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

    loaded_prefixes = []

    for prefix in ConceptPrefix:
        vocab_status = await get_vocabulary_status(
            prefix,
            doc_db=doc_db,
        )
        if vocab_status.loaded:
            loaded_prefixes.append(prefix.value)

    return loaded_prefixes
