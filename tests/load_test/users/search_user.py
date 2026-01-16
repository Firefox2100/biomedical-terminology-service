"""
A locust user class that performs semantic search requests against the API.
"""

import random
import string
from locust import FastHttpUser, task, between

from bioterms.etc.enums import ConceptPrefix


class SearchUser(FastHttpUser):
    """
    A user class that simulates someone searching for a term semantically.
    """
    wait_time = between(5, 20)

    @task
    def semantic_search(self):
        """
        Perform a semantic search using the search API.
        """
        prefix = random.choice(list(ConceptPrefix))

        query_length = random.randint(3, 40)
        query = ''.join(random.choices(string.ascii_lowercase + ' ', k=query_length)).strip()

        limit = random.randint(10, 100)

        with self.client.get(
            f'/api/vocabularies/{prefix.value}/search/v1',
            params={
                'query': query,
                'limit': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/search/v1',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()
