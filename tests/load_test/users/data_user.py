"""
A module including the users to mimic management users calling data related APIs.
"""

import random
from locust import FastHttpUser, task, between, constant

from bioterms.etc.enums import ConceptPrefix

from load_test.common.randoms import get_random_concept_ids
from load_test.common.user import ConceptIdConsumer


class ApiScraper(FastHttpUser):
    """
    A user class that simulates another service scraping the status of the API.
    """
    wait_time = constant(15)

    @task
    def get_vocabulary_status(self):
        """
        Retrieve the status of a random vocabulary.
        """
        prefix = random.choice(list(ConceptPrefix))

        with self.client.get(
            f'/api/vocabularies/{prefix.value}',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()

    @task
    def get_vocabulary_license(self):
        """
        Retrieve the license information of a random vocabulary.
        """
        prefix = random.choice(list(ConceptPrefix))

        with self.client.get(
            f'/api/vocabularies/{prefix.value}/license',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class IntegratedDisplay(ConceptIdConsumer):
    """
    A user class that simulates another service that uses this one to fetch data for displaying.
    """
    wait_time = between(1, 30)

    @task
    def get_random_concept_ids(self):
        """
        Retrieve a list of random concept IDs from the API.
        """
        count = random.randint(1, 100)
        prefix = random.choice(list(ConceptPrefix))

        get_random_concept_ids(
            client=self.client,
            prefix=prefix,
            count=count,
        )

    @task
    def get_concept_details(self):
        """
        Retrieve the details of a random concept ID.
        """
        prefix = random.choice(list(ConceptPrefix))
        concept_id = self._get_concept_ids(
            prefix=prefix,
            count=1,
        )[0]

        with self.client.get(
            f'/api/vocabularies/{prefix.value}/{concept_id}',
            name=f'/api/vocabularies/{prefix.value}/<concept_id>',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()
