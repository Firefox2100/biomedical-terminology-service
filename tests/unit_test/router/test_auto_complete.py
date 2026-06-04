import os

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'dGVzdC1obWFjLWtleQ==')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

import pytest

from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept
from bioterms.router.auto_complete import auto_complete_v1, auto_complete_v2


class FakeDocumentDatabase:
    def __init__(self, concepts):
        self.concepts = concepts
        self.calls = []

    async def auto_complete_iter(self, prefix, query, limit=None, model_class=Concept):
        self.calls.append({
            'prefix': prefix,
            'query': query,
            'limit': limit,
            'model_class': model_class,
        })
        for concept in self.concepts:
            yield concept


def make_concept(concept_id='0000001', label='Abnormality', synonyms=None, definition=None):
    return Concept(
        conceptTypes=[],
        prefix=ConceptPrefix.HPO,
        conceptId=concept_id,
        label=label,
        synonyms=synonyms,
        definition=definition,
        status=ConceptStatus.ACTIVE,
    )


@pytest.mark.asyncio
async def test_auto_complete_v1_rejects_short_query_without_querying_database():
    doc_db = FakeDocumentDatabase([make_concept()])

    result = await auto_complete_v1(
        prefix=ConceptPrefix.HPO,
        query_str='ab',
        doc_db=doc_db,
    )

    assert result == ['Search term needs at least 3 characters.']
    assert doc_db.calls == []


@pytest.mark.asyncio
async def test_auto_complete_v1_formats_legacy_result_with_label_and_synonyms():
    doc_db = FakeDocumentDatabase([
        make_concept(synonyms=['Variant One', 'Variant Two']),
    ])

    result = await auto_complete_v1(
        prefix=ConceptPrefix.HPO,
        query_str='abn',
        long=False,
        doc_db=doc_db,
    )

    assert result == [
        'ConceptPrefix.HPO:0000001 (Abnormality) synonyms:[Variant One, Variant Two]'
    ]
    assert doc_db.calls[0]['limit'] == 25


@pytest.mark.asyncio
async def test_auto_complete_v1_uses_long_limit_when_requested():
    doc_db = FakeDocumentDatabase([])

    await auto_complete_v1(
        prefix=ConceptPrefix.HPO,
        query_str='abn',
        long=True,
        doc_db=doc_db,
    )

    assert doc_db.calls[0]['limit'] == 250


@pytest.mark.asyncio
async def test_auto_complete_v2_omits_definition_by_default_and_removes_limit_when_threshold_is_zero():
    doc_db = FakeDocumentDatabase([
        make_concept(definition='A test definition.'),
    ])

    result = await auto_complete_v2(
        prefix=ConceptPrefix.HPO,
        query='abn',
        with_definition=False,
        result_threshold=0,
        doc_db=doc_db,
    )

    assert result[0].term_id == '0000001'
    assert result[0].label == 'Abnormality'
    assert result[0].definition is None
    assert doc_db.calls[0]['limit'] is None
    assert doc_db.calls[0]['model_class'] is Concept


@pytest.mark.asyncio
async def test_auto_complete_v2_includes_definition_and_threshold():
    doc_db = FakeDocumentDatabase([
        make_concept(definition='A test definition.'),
    ])

    result = await auto_complete_v2(
        prefix=ConceptPrefix.HPO,
        query='abn',
        with_definition=True,
        result_threshold=3,
        doc_db=doc_db,
    )

    assert result[0].definition == 'A test definition.'
    assert doc_db.calls[0]['limit'] == 3
