from locust.contrib.fasthttp import FastHttpSession

from bioterms.etc.enums import ConceptPrefix


def get_random_concept_ids(client: FastHttpSession,
                           prefix: ConceptPrefix,
                           count: int = 10,
                           ) -> list[str]:
    """
    Retrieve a list of random concept IDs from the API.
    :param client: The HTTP client to use for making requests. it should have been
        configured to have a root URL pointing to the API server.
    :param prefix: The concept prefix to use.
    :param count: The number of concept IDs to retrieve.
    :return: A list of concept IDs.
    """
    with client.get(
        f'/api/vocabularies/{prefix.value}/random',
        params={
            'count': str(count),
        },
        catch_response=True,
    ) as response:
        if response.status_code != 200:
            response.failure(f'Unexpected status code: {response.status_code}')
            return []
        else:
            data = list(response.json())

            if len(data) != count:
                response.failure(f'Unexpected number of concept IDs: {len(data)}')
                return []

            response.success()
            return data
