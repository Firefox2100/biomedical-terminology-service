import json

import pytest
from fhir.resources.parameters import Parameters, ParametersParameter

from bioterms.router import fhir


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('resolver_name', 'endpoint'),
    [
        ('_lookup_fhir_code', fhir.lookup_fhir_code_post),
        ('_validate_fhir_code', fhir.validate_fhir_code_post),
    ],
)
async def test_fhir_post_operations_accept_code_and_system(monkeypatch, resolver_name, endpoint):
    captured = {}

    async def fake_resolver(**kwargs):
        captured.update(kwargs)
        return Parameters(parameter=[])

    monkeypatch.setattr(fhir, resolver_name, fake_resolver)
    request = Parameters(parameter=[
        ParametersParameter(name='code', valueCode='0001250'),
        ParametersParameter(name='system', valueUri='https://example.test/CodeSystem/hpo'),
    ])

    await endpoint(request, doc_db=object())

    assert captured['code'] == '0001250'
    assert captured['system'] == 'https://example.test/CodeSystem/hpo'


@pytest.mark.asyncio
@pytest.mark.parametrize('endpoint', [fhir.lookup_fhir_code_post, fhir.validate_fhir_code_post])
async def test_fhir_post_operations_reject_missing_parameters(endpoint):
    response = await endpoint(Parameters(parameter=[]), doc_db=object())

    assert response.status_code == 422
    assert json.loads(response.body)['issue'][0]['code'] == 'invalid'
