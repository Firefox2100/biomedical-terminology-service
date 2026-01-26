from concurrent.futures import ProcessPoolExecutor

from bioterms.etc.consts import CONFIG
from bioterms.etc.utils import schedule_tasks
from bioterms.model.concept import Concept


def _generate_extra_data_for_term(concept: Concept) -> tuple[str, list[str], str]:
    """
    Generate extra data for search indexing from a Concept object.
    :param concept: A Concept object
    :return: A tuple containing the term's ID, n-grams, and search text.
    """
    ngrams = concept.n_grams()
    search_text = concept.search_text()

    return concept.concept_id, ngrams, search_text


async def generate_extra_data(concepts: list[Concept],
                              executor: ProcessPoolExecutor = None
                              ) -> list[tuple[str, list[str], str]]:
    """
    Generate extra data for search indexing from a list of Concept objects.
    :param concepts: A list of Concept objects
    :return: A tuple containing the term's ID, n-grams, and search text.
    """
    results = []

    if executor is not None:
        async for result in schedule_tasks(
            executor=executor,
            func=_generate_extra_data_for_term,
            iterable=concepts,
            description='Generating extra data for search indexing from Concept objects.',
            total=len(concepts),
            transient=True,
        ):
            results.append(result)

        return results

    with ProcessPoolExecutor(
        max_workers=CONFIG.process_limit,
    ) as executor:
        async for result in schedule_tasks(
            executor=executor,
            func=_generate_extra_data_for_term,
            iterable=concepts,
            description='Generating extra data for search indexing from Concept objects.',
            total=len(concepts),
            transient=True,
        ):
            results.append(result)

    return results
