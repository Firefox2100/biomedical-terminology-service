"""
User classes for performing concept ID annotation mappings using different API versions.
"""

import random
from locust import task, between

from bioterms.etc.enums import ConceptPrefix

from load_test.common.user import ConceptIdConsumer


class MappingV1User(ConceptIdConsumer):
    """
    A user class that simulates a service performing concept ID annotation mappings,
    using the V1 API.
    """
    wait_time = between(1, 20)

    @task
    def map_concept_ids(self):
        """
        Perform concept ID mapping using the V1 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        target_prefix = random.choice(list(ConceptPrefix))
        count = random.randint(1, 100)
        limit = random.randint(50, 500)
        concept_ids = self._get_concept_ids(
            prefix=prefix,
            count=count,
        )

        with self.client.post(
            f'/api/vocabularies/{prefix.value}/map/v1/{target_prefix.value}',
            json={
                'termIds': concept_ids,
            },
            params={
                'result_threshold': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/map/v1/{target_prefix.value}',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class MappingV2User(ConceptIdConsumer):
    """
    A user class that simulates a service performing concept ID annotation mappings,
    using the V2 API.
    """
    wait_time = between(1, 20)

    @task
    def map_concept_ids(self):
        """
        Perform concept ID mapping using the V2 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        target_prefix = random.choice(list(ConceptPrefix))
        count = random.randint(1, 100)
        max_hop = random.randint(1, 3)
        limit = random.randint(50, 500)
        concept_ids = self._get_concept_ids(
            prefix=prefix,
            count=count,
        )

        with self.client.get(
            f'/api/vocabularies/{prefix.value}/map/v2/{target_prefix.value}',
            params={
                'concept_ids': concept_ids,
                'max_hops': str(max_hop),
                'limit': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/map/v2/{target_prefix.value}',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()
