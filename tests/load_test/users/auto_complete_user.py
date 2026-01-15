"""
A module including the users to mimic auto-completion request usage.
"""

import random
import string
from locust import FastHttpUser, task, between

from bioterms.etc.enums import ConceptPrefix


def prepare_query_parameters(min_length: int = 3,
                             max_length: int = 15,
                             ):
    """
    Prepare the auto-completion request parameters
    :param min_length: The minimum length to generate for the query string
    :param max_length: The maximum length to generate for the query string
    :return: The prefix and query that is randomly generated
    """
    prefix = random.choice(list(ConceptPrefix))

    query_length = random.randint(min_length, max_length)
    query = ''.join(random.choices(string.ascii_lowercase, k=query_length))

    return prefix.value, query


class AutoCompleteV1User(FastHttpUser):
    """
    A user class that simulates someone typing into an auto-complete search box,
    using the V1 API.
    """
    wait_time = between(3, 10)

    @task
    def auto_complete_search(self):
        """
        Run the auto complete request using V1 API
        """
        prefix, query = prepare_query_parameters()
        long_response = random.random() < 0.1

        request_url = f'/api/vocabularies/{prefix}/auto-complete/v1/query/{query}'

        with self.client.get(
            request_url,
            name=f'/api/vocabularies/{prefix}/auto-complete/v1/query',
            params={
                'long': 'true' if long_response else 'false',
            },
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class AutoCompleteV2User(FastHttpUser):
    """
    A user class that simulates someone typing into an auto-complete search box,
    using the V2 API.
    """
    wait_time = between(1, 5)

    @task
    def auto_complete_search(self):
        """
        Run the auto complete request using V2 API
        """
        prefix, query = prepare_query_parameters()
        limit = random.randint(20, 100)
        with_definition = random.random() < 0.3

        request_url = f'/api/vocabularies/{prefix}/auto-complete/v2'

        with self.client.get(
            request_url,
            params={
                'query': query,
                'result_threshold': str(limit),
                'with_definition': 'true' if with_definition else 'false',
            },
            name=f'/api/vocabularies/{prefix}/auto-complete/v2',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class AutoCompleteV3User(FastHttpUser):
    """
    A user class that simulates someone typing into an auto-complete search box,
    using the V3 API.
    """
    wait_time = between(1, 5)

    @task
    def auto_complete_search(self):
        """
        Run the auto complete request using V3 API
        """
        prefix, query = prepare_query_parameters()
        limit = random.randint(20, 100)

        request_url = f'/api/vocabularies/{prefix}/auto-complete/v3'

        with self.client.get(
            request_url,
            params={
                'query': query,
                'limit': str(limit),
            },
            name=f'/api/vocabularies/{prefix}/auto-complete/v2',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()
