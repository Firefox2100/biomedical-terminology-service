import asyncio

from bioterms.etc.consts import EXECUTOR
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


async def generate_extra_data(concepts: list[Concept]) -> list[tuple[str, list[str], str]]:
    """
    Generate extra data for search indexing from a list of Concept objects.
    :param concepts: A list of Concept objects
    :return: A tuple containing the term's ID, n-grams, and search text.
    """
    loop = asyncio.get_running_loop()

    futures = [
        loop.run_in_executor(EXECUTOR, _generate_extra_data_for_term, concept)
        for concept in concepts
    ]

    return await asyncio.gather(*futures)
