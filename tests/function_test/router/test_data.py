import gzip
import os

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'dGVzdC1obWFjLWtleQ==')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

import pytest

from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept
from bioterms.model.related_term import RelatedTerm
from bioterms.router.data import ingest_documents, get_concept


class FakeRequest:
    def __init__(self, chunks, headers=None):
        self._chunks = chunks
        self.headers = headers or {}

    async def stream(self):
        for chunk in self._chunks:
            yield chunk


class FakeDocumentDatabase:
    def __init__(self, concepts=None):
        self.concepts = concepts or {}
        self.saved_batches = []

    async def save_terms(self, terms):
        self.saved_batches.append(list(terms))
        for term in terms:
            self.concepts[term.concept_id] = term

    async def count_terms(self, prefix):
        return len([
            concept for concept in self.concepts.values()
            if concept.prefix == prefix
        ])

    async def get_terms_by_ids(self, prefix, concept_ids, model_class=Concept):
        return [
            self.concepts[concept_id]
            for concept_id in concept_ids
            if concept_id in self.concepts and self.concepts[concept_id].prefix == prefix
        ]


class FakeGraphDatabase:
    async def expand_terms(self, prefix, concept_ids, max_depth=1):
        return [RelatedTerm(conceptId=concept_ids[0], relatedConcepts=['0000002'])]

    async def trace_ancestors(self, prefix, concept_ids, max_depth=1):
        return [RelatedTerm(conceptId=concept_ids[0], relatedConcepts=['0000003'])]


def make_concept(concept_id, label):
    return Concept(
        conceptTypes=[],
        prefix=ConceptPrefix.HPO,
        conceptId=concept_id,
        label=label,
        status=ConceptStatus.ACTIVE,
    )


def as_json_line(concept):
    return concept.model_dump_json(by_alias=True).encode() + b'\n'


@pytest.mark.asyncio
async def test_ingest_documents_accepts_chunked_newline_delimited_json():
    first = make_concept('0000001', 'First Concept')
    second = make_concept('0000002', 'Second Concept')
    payload = as_json_line(first) + as_json_line(second)
    request = FakeRequest([payload[:17], payload[17:42], payload[42:]])
    doc_db = FakeDocumentDatabase()

    response = await ingest_documents(
        prefix=ConceptPrefix.HPO,
        request=request,
        doc_db=doc_db,
        _='tester',
    )

    assert response.concept_count == 2
    assert len(doc_db.saved_batches) == 1
    assert [term.concept_id for term in doc_db.saved_batches[0]] == ['0000001', '0000002']


@pytest.mark.asyncio
async def test_ingest_documents_accepts_gzip_encoded_body():
    concept = make_concept('0000001', 'First Concept')
    payload = gzip.compress(as_json_line(concept))
    request = FakeRequest(
        [payload[:10], payload[10:]],
        headers={'Content-Encoding': 'gzip'},
    )
    doc_db = FakeDocumentDatabase()

    response = await ingest_documents(
        prefix=ConceptPrefix.HPO,
        request=request,
        doc_db=doc_db,
        _='tester',
    )

    assert response.concept_count == 1
    assert doc_db.saved_batches[0][0] == concept


@pytest.mark.asyncio
async def test_get_concept_returns_concept_with_direct_children_and_parents():
    concept = make_concept('0000001', 'Main Concept')
    child = make_concept('0000002', 'Child Concept')
    parent = make_concept('0000003', 'Parent Concept')
    doc_db = FakeDocumentDatabase({
        concept.concept_id: concept,
        child.concept_id: child,
        parent.concept_id: parent,
    })

    response = await get_concept(
        prefix=ConceptPrefix.HPO,
        concept_id='0000001',
        doc_db=doc_db,
        graph_db=FakeGraphDatabase(),
    )

    assert response.concept == concept
    assert response.children == [child]
    assert response.parents == [parent]
