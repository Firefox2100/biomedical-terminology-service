import random
from locust import task, between

from bioterms.etc.enums import ConceptPrefix

from load_test.common.user import ConceptIdConsumer


class SimilarityV1User(ConceptIdConsumer):
    """
    A user class that simulates a service performing concept similarity searches,
    using the V1 API.
    """
    wait_time = between(1, 20)

    @task
    def find_similar_concepts(self):
        """
        Perform concept similarity search using the V1 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        count = random.randint(1, 20)
        similarity_threshold = random.random() * 0.5 + 0.5
        limit = random.randint(50, 500)
        concept_ids = self._get_concept_ids(
            prefix=prefix,
            count=count,
        )

        with self.client.post(
            f'/api/vocabularies/{prefix.value}/similarity/v1',
            json={
                'termIds': concept_ids,
                'threshold': similarity_threshold,
            },
            params={
                'result_threshold': str(limit),
            },
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class SimilarityV2User(ConceptIdConsumer):
    """
    A user class that simulates a service performing concept similarity searches,
    using the V2 API.
    """
    wait_time = between(1, 20)

    @task
    def find_similar_concepts(self):
        """
        Perform concept similarity search using the V2 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        count = random.randint(1, 20)
        similarity_threshold = random.random() * 0.5 + 0.5
        same_prefix = random.random() < 0.7
        limit = random.randint(50, 500)
        concept_ids = self._get_concept_ids(
            prefix=prefix,
            count=count,
        )

        with self.client.get(
            f'/api/vocabularies/{prefix.value}/similarity/v2',
            params={
                'concept_ids': ','.join(concept_ids),
                'threshold': str(similarity_threshold),
                'same_prefix': 'true' if same_prefix else 'false',
                'limit': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/similarity/v2',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class TranslateV1User(ConceptIdConsumer):
    """
    A user class that simulates a service performing concept ID translations,
    using the V1 API.
    """
    wait_time = between(1, 20)

    @task
    def translate_concept_ids(self):
        """
        Perform concept ID translation using the V1 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        original_ids = self._get_concept_ids(
            prefix=prefix,
            count=random.randint(1, 100),
        )
        target_ids = self._get_concept_ids(
            prefix=prefix,
            count=random.randint(1, 100),
        )
        similarity_threshold = random.random() * 0.5 + 0.5
        limit = random.randint(50, 500)

        with self.client.post(
            f'/api/vocabularies/{prefix.value}/translate/v1',
            json={
                'termIds': original_ids,
                'constraintIds': target_ids,
                'threshold': similarity_threshold,
            },
            params={
                'result_threshold': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/translate/v1',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()


class TranslateV2User(ConceptIdConsumer):
    """
    A user class that simulates a service performing concept ID translations,
    using the V2 API.
    """
    wait_time = between(1, 20)

    @task
    def translate_concept_ids(self):
        """
        Perform concept ID translation using the V2 API.
        """
        prefix = random.choice(list(ConceptPrefix))
        original_ids = self._get_concept_ids(
            prefix=prefix,
            count=random.randint(1, 100),
        )
        target_ids = self._get_concept_ids(
            prefix=prefix,
            count=random.randint(1, 100),
        )
        similarity_threshold = random.random() * 0.5 + 0.5
        limit = random.randint(50, 500)

        with self.client.get(
            f'/api/vocabularies/{prefix.value}/translate/v2',
            params={
                'original_ids': original_ids,
                'constraint_concepts': target_ids,
                'threshold': str(similarity_threshold),
                'limit': str(limit),
            },
            name=f'/api/vocabularies/{prefix.value}/translate/v2',
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f'Unexpected status code: {response.status_code}')
            else:
                response.success()
