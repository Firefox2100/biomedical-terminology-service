"""
User classes for load testing concept expansion endpoints.
"""

import random
from locust import task, between

from bioterms.etc.enums import ConceptPrefix

from load_test.common.user import ConceptIdConsumer


class QuerySystemV1User(ConceptIdConsumer):
    """
    A user class that simulates a query system that needs ontology traversal for query
    completion, using the V1 API.
    """
    wait_time = between(1, 30)

    @task
    def expand_concepts(self):
        """
        Perform concept expansion using the V1 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        count = random.randint(1, 100)
        concept_ids = self._get_concept_ids(
            prefix=prefix,
            count=count,
        )
        expansion_depth = random.randint(0, 10)
        limit = random.randint(50, 500)

        with self.client.post(
            f'/api/vocabularies/{prefix.value}/expand/v1',
            json={
                'termIds': concept_ids,
            },
            params={
                'depth': str(expansion_depth),
                'result_threshold': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/expand/v1',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class QuerySystemV2User(ConceptIdConsumer):
    """
    A user class that simulates a query system that needs ontology traversal for query
    completion, using the V2 API.
    """
    wait_time = between(1, 30)

    @task
    def expand_concepts(self):
        """
        Perform concept expansion using the V2 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        count = random.randint(1, 100)
        concept_ids = self._get_concept_ids(
            prefix=prefix,
            count=count,
        )
        expansion_depth = random.randint(0, 10)
        limit = random.randint(50, 500)

        with self.client.get(
            f'/api/vocabularies/{prefix.value}/expand/v2',
            params={
                'concept_ids': concept_ids,
                'depth': str(expansion_depth),
                'limit': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/expand/v2',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()
