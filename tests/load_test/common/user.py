import random
from locust import FastHttpUser

from bioterms.etc.enums import ConceptPrefix

from load_test.common.randoms import get_random_concept_ids


class ConceptIdConsumer(FastHttpUser):
    """
    A base user class that consumes concept IDs for further processing.
    """
    abstract = True

    def __init__(self,
                 *args,
                 **kwargs,
                 ):
        super().__init__(*args, **kwargs)

        self._concept_ids = {}

    def _get_concept_ids(self,
                         prefix: ConceptPrefix,
                         count: int = 10,
                         ) -> list[str]:
        """
        Retrieve a list of concept IDs for the given prefix.
        :param prefix: The concept prefix to use.
        :param count: The number of concept IDs to retrieve.
        :return: A list of concept IDs.
        """
        if count > 100 or count < 1:
            raise ValueError(f'Invalid count {count}, must be between 1 and 100.')

        concept_ids = self._concept_ids.get(prefix, None)

        if not concept_ids:
            concept_ids = get_random_concept_ids(
                client=self.client,
                prefix=prefix,
                count=100,
            )
            self._concept_ids[prefix] = concept_ids

        return random.sample(concept_ids, k=count)
