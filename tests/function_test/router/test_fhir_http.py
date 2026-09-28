"""HTTP-level tests for the FHIR terminology endpoints, through FastAPI routing and serialisation."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioterms.database import get_active_doc_db, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.router import fhir

BASE = 'https://terms.example/fhir'
HPO_SYSTEM = f'{BASE}/CodeSystem/hpo'


class FakeDocDb:
    concepts = {
        'HP:0001250': Concept(prefix=ConceptPrefix.HPO, conceptId='HP:0001250', label='Seizure',
                              synonyms=['Epileptic seizure'], definition='A seizure.',
                              status=ConceptStatus.ACTIVE),
        'HP:0000001': Concept(prefix=ConceptPrefix.HPO, conceptId='HP:0000001', label='All',
                              status=ConceptStatus.DEPRECATED),
    }

    async def get_terms_by_ids(self, prefix, concept_ids, model_class):
        return [self.concepts[c] for c in concept_ids if c in self.concepts]


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(CONFIG, 'fhir_canonical_url', BASE + '/')

    async def status(prefix, **_kwargs):
        loaded = prefix == ConceptPrefix.HPO
        return VocabularyStatus(prefix=prefix, name=f'{prefix.value} name', fileDownloaded=loaded, loaded=loaded,
                                conceptCount=2 if loaded else 0, relationshipCount=0, vectorCount=0,
                                annotations=[], similarityMethods=[])

    monkeypatch.setattr(fhir, 'get_vocabulary_status', status)
    app = FastAPI()
    app.include_router(fhir.fhir_router)

    async def doc_db():
        return FakeDocDb()

    app.dependency_overrides[get_active_doc_db] = doc_db
    app.dependency_overrides[get_active_graph_db] = lambda: object()
    return TestClient(app)


def _params(body):
    return {p['name']: p for p in body['parameter']}


def _inactive(body):
    [prop] = [p for p in body['parameter'] if p['name'] == 'property'
              and p['part'][0]['valueCode'] == 'inactive']
    return prop['part'][1]['valueBoolean']


def test_metadata_and_code_systems(client):
    metadata = client.get('/fhir/metadata').json()
    bundle = client.get('/fhir/CodeSystem').json()
    hpo = client.get('/fhir/CodeSystem/hpo')
    missing = client.get('/fhir/CodeSystem/mondo')

    assert metadata['resourceType'] == 'CapabilityStatement'
    assert [e['resource']['id'] for e in bundle['entry']] == ['hpo']
    assert hpo.json()['url'] == HPO_SYSTEM
    assert missing.status_code == 404
    assert missing.json()['issue'][0]['code'] == 'not-found'


def test_lookup_returns_concept_parameters(client):
    body = client.get('/fhir/CodeSystem/$lookup', params={'system': HPO_SYSTEM, 'code': 'HP:0001250'}).json()
    params = _params(body)

    assert params['display']['valueString'] == 'Seizure'
    assert params['system']['valueUri'] == HPO_SYSTEM
    assert _inactive(body) is False
    assert params['designation']['part'][0]['valueString'] == 'Epileptic seizure'
    assert params['definition']['valueString'] == 'A seizure.'


def test_lookup_can_restrict_properties(client):
    body = client.get('/fhir/CodeSystem/$lookup', params=[
        ('system', HPO_SYSTEM), ('code', 'HP:0000001'), ('property', 'display'), ('property', 'inactive'),
    ]).json()

    assert [p['name'] for p in body['parameter']] == ['display', 'property']
    assert _inactive(body) is True


@pytest.mark.parametrize(('system', 'code', 'status', 'issue_code'), [
    ('https://elsewhere.example/CodeSystem/hpo', 'HP:0001250', 422, 'invalid'),
    (f'{BASE}/CodeSystem/nope', 'HP:0001250', 404, 'not-found'),
    (HPO_SYSTEM, 'HP:9999999', 404, 'code-invalid'),
])
def test_lookup_errors_are_operation_outcomes(client, system, code, status, issue_code):
    response = client.get('/fhir/CodeSystem/$lookup', params={'system': system, 'code': code})

    assert response.status_code == status
    assert response.json()['issue'][0]['code'] == issue_code


def test_validate_code_known_and_unknown(client):
    known = client.get('/fhir/CodeSystem/$validate-code', params={'system': HPO_SYSTEM, 'code': 'HP:0001250'})
    unknown = client.post('/fhir/CodeSystem/$validate-code', json={
        'resourceType': 'Parameters', 'parameter': [
            {'name': 'system', 'valueUri': HPO_SYSTEM}, {'name': 'code', 'valueCode': 'HP:404'},
        ],
    })

    assert known.status_code == 200, known.text
    known_params = _params(known.json())
    assert known_params['result']['valueBoolean'] is True
    assert known_params['system']['valueUri'] == HPO_SYSTEM
    assert known_params['display']['valueString'] == 'Seizure'

    unknown_params = _params(unknown.json())
    assert unknown_params['result']['valueBoolean'] is False
    assert 'Unknown code' in unknown_params['message']['valueString']


@pytest.mark.parametrize(('system', 'status', 'issue_code'), [
    ('https://elsewhere.example/CodeSystem/hpo', 422, 'invalid'),
    (f'{BASE}/CodeSystem/nope', 404, 'not-found'),
])
def test_validate_code_rejects_foreign_or_unknown_systems(client, system, status, issue_code):
    response = client.get('/fhir/CodeSystem/$validate-code', params={'system': system, 'code': 'HP:0001250'})

    assert response.status_code == status
    assert response.json()['issue'][0]['code'] == issue_code
