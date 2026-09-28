"""Unit tests for the application module's lifespan, cache rebuild and exception handlers."""
import types

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import bioterms.app as app_module
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import BtsError


class _Closable:
    def __init__(self, log, name):
        self.log, self.name = log, name

    async def close(self):
        self.log.append(f'close {self.name}')


@pytest.fixture
def backends(monkeypatch):
    log = []
    cache, doc_db, graph_db = (_Closable(log, n) for n in ('cache', 'doc_db', 'graph_db'))

    async def active_doc_db():
        return doc_db

    monkeypatch.setattr(app_module, 'get_active_cache', lambda: cache)
    monkeypatch.setattr(app_module, 'get_active_doc_db', active_doc_db)
    monkeypatch.setattr(app_module, 'get_active_graph_db', lambda: graph_db)
    monkeypatch.setattr(app_module, 'initialize_metrics', lambda: log.append('metrics'))
    return log, cache, doc_db, graph_db


@pytest.mark.asyncio
async def test_lifespan_initialises_graphql_and_closes_backends(backends):
    log, *_ = backends

    async def initialise():
        log.append('graphql')

    app = types.SimpleNamespace(state=types.SimpleNamespace(
        graphql_service=types.SimpleNamespace(initialise=initialise),
    ))

    async with app_module.lifespan(app):
        log.append('serving')

    assert log == ['metrics', 'graphql', 'serving', 'close cache', 'close doc_db', 'close graph_db']


@pytest.mark.asyncio
async def test_lifespan_closes_backends_when_startup_fails(backends):
    log, *_ = backends

    async def initialise():
        raise RuntimeError('schema build failed')

    app = types.SimpleNamespace(state=types.SimpleNamespace(
        graphql_service=types.SimpleNamespace(initialise=initialise),
    ))

    with pytest.raises(RuntimeError, match='schema build failed'):
        async with app_module.lifespan(app):
            pass  # pragma: no cover

    assert log[-3:] == ['close cache', 'close doc_db', 'close graph_db']


@pytest.mark.asyncio
@pytest.mark.parametrize('rotate', [True, False])
async def test_rebuild_cache_refreshes_every_status_without_cache(backends, monkeypatch, rotate):
    _, cache, _, _ = backends
    calls = []
    cache.rotate_dataset_version = lambda: _record(calls, 'rotate')

    async def vocabulary_status(prefix, use_cache, **_):
        calls.append(('vocabulary', prefix, use_cache))
        return types.SimpleNamespace(annotations=[ConceptPrefix.MONDO] if prefix == ConceptPrefix.HPO else [])

    async def annotation_status(prefix_1, prefix_2, use_cache, **_):
        calls.append(('annotation', prefix_1, prefix_2, use_cache))

    async def similarity_status(prefix, use_cache, **_):
        calls.append(('similarity', prefix, use_cache))

    monkeypatch.setattr(app_module, 'get_vocabulary_status', vocabulary_status)
    monkeypatch.setattr(app_module, 'get_annotation_status', annotation_status)
    monkeypatch.setattr(app_module, 'get_similarity_status', similarity_status)

    await app_module.rebuild_cache(rotate_dataset_version=rotate)

    assert ('annotation', ConceptPrefix.HPO, ConceptPrefix.MONDO, False) in calls
    assert sum(1 for c in calls if c[0] == 'vocabulary') == len(ConceptPrefix)
    assert all(c[-1] is False for c in calls if c != 'rotate')
    assert (calls[-1] == 'rotate') is rotate


async def _record(calls, value):
    calls.append(value)


def _error_app():
    app = FastAPI()
    app.exception_handler(BtsError)(app_module.bioterms_exception_handler)
    app.exception_handler(HTTPException)(app_module.http_exception_handler)
    app.middleware('http')(app_module.disable_cors_for_api)

    @app.get('/api/boom')
    async def api_boom():
        raise HTTPException(status_code=418, detail='teapot')

    @app.get('/fhir/boom')
    async def fhir_boom():
        raise HTTPException(status_code=404, detail='no such code system')

    @app.post('/api/bts')
    async def bts():
        raise BtsError('vocabulary not loaded', status_code=409)

    return app


def test_api_errors_stay_json_with_open_cors(monkeypatch):
    monkeypatch.setattr(app_module, 'report_exception', lambda exc: None)
    client = TestClient(_error_app())

    api = client.get('/api/boom')
    bts = client.post('/api/bts', content=b'payload')

    assert api.status_code == 418
    assert api.json() == {'detail': 'teapot'}
    assert api.headers['access-control-allow-origin'] == '*'
    assert bts.status_code == 409
    assert bts.json() == {'error': {'message': 'vocabulary not loaded'}}


def test_fhir_errors_render_operation_outcome(monkeypatch):
    monkeypatch.setattr(app_module, 'report_exception', lambda exc: None)
    client = TestClient(_error_app())

    response = client.get('/fhir/boom')

    assert response.status_code == 404
    body = response.json()
    assert body['resourceType'] == 'OperationOutcome'
    assert body['issue'][0]['severity'] == 'error'
    assert 'no such code system' in body['issue'][0]['diagnostics']
    assert response.headers['access-control-allow-origin'] == '*'


def test_skip_paths_exempts_api_clients_from_csrf():
    assert app_module.skip_paths({'path': '/api/search'})
    assert app_module.skip_paths({'path': '/fhir/CodeSystem'})
    assert app_module.skip_paths({'path': '/mcp/'})
    assert not app_module.skip_paths({'path': '/login'})
