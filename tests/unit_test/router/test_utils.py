import os

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'dGVzdC1obWFjLWtleQ==')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

import pytest

from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept
from bioterms.router.utils import response_generator, sanitise_next_url


async def concept_iter(*concepts):
    for concept in concepts:
        yield concept


class TestResponseGenerator:
    @pytest.mark.asyncio
    async def test_generates_json_array_for_empty_iterator(self):
        chunks = [chunk async for chunk in response_generator(concept_iter())]

        assert b''.join(chunks) == b'[]'

    @pytest.mark.asyncio
    async def test_generates_json_array_for_multiple_concepts(self):
        first = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000001',
            label='First Concept',
            status=ConceptStatus.ACTIVE,
        )
        second = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000002',
            label='Second Concept',
            status=ConceptStatus.ACTIVE,
        )

        body = b''.join([
            chunk async for chunk in response_generator(concept_iter(first, second))
        ]).decode()

        assert body.startswith('[')
        assert body.endswith(']')
        assert '"conceptId":"0000001"' in body
        assert '"conceptId":"0000002"' in body
        assert '},\n{' in body


class TestSanitiseNextUrl:
    def test_allows_known_local_destinations_with_query_string(self):
        assert sanitise_next_url('/vocabularies?prefix=hpo') == '/vocabularies?prefix=hpo'

    def test_allows_vocabulary_detail_paths(self):
        assert sanitise_next_url('/vocabularies/hpo') == '/vocabularies/hpo'
        assert sanitise_next_url('/vocabularies/hpo-extended_1') == '/vocabularies/hpo-extended_1'

    def test_rejects_external_redirects(self):
        assert sanitise_next_url('https://example.com/vocabularies') == '/'
        assert sanitise_next_url('//example.com/vocabularies') == '/'

    def test_normalises_local_paths_before_allow_list_matching(self):
        assert sanitise_next_url('/vocabularies/hpo/../admin', default='/login') == '/vocabularies/admin'
