import os
from datetime import datetime
from types import SimpleNamespace

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'dGVzdC1obWFjLWtleQ==')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

import pytest
from starlette.responses import JSONResponse

from bioterms.router import utils
from bioterms.router.utils import CacheControlMiddleware


class FakeCache:
    async def get_dataset_last_modified(self):
        return datetime(2026, 1, 2, 3, 4, 5)


class FakeRequest:
    def __init__(self, path, headers=None, method='GET'):
        self.method = method
        self.headers = headers or {}
        self.url = SimpleNamespace(path=path)


async def ok_response(request):
    return JSONResponse({'status': 'ok'})


@pytest.mark.asyncio
async def test_cache_control_headers_are_added_to_vocabulary_get(monkeypatch):
    monkeypatch.setattr(utils, 'get_active_cache', lambda: FakeCache())
    middleware = CacheControlMiddleware(app=None)

    response = await middleware.dispatch(
        FakeRequest('/api/vocabularies/hpo'),
        ok_response,
    )

    assert response.status_code == 200
    assert response.headers['Last-Modified'] == 'Fri, 02 Jan 2026 03:04:05 GMT'
    assert response.headers['Cache-Control'] == 'public, max-age=86400, stale-while-revalidate=172800'
    assert response.headers['ETag']


@pytest.mark.asyncio
async def test_cache_control_returns_304_for_matching_if_modified_since(monkeypatch):
    monkeypatch.setattr(utils, 'get_active_cache', lambda: FakeCache())
    middleware = CacheControlMiddleware(app=None)

    response = await middleware.dispatch(
        FakeRequest(
            '/api/vocabularies/hpo',
            headers={'If-Modified-Since': 'Fri, 02 Jan 2026 03:04:05 GMT'},
        ),
        ok_response,
    )

    assert response.status_code == 304
    assert response.headers['Last-Modified'] == 'Fri, 02 Jan 2026 03:04:05 GMT'
    assert response.headers['Cache-Control'] == 'public, max-age=86400, stale-while-revalidate=172800'


@pytest.mark.asyncio
async def test_cache_control_skips_random_endpoint(monkeypatch):
    monkeypatch.setattr(utils, 'get_active_cache', lambda: FakeCache())
    middleware = CacheControlMiddleware(app=None)

    response = await middleware.dispatch(
        FakeRequest('/api/vocabularies/hpo/random'),
        ok_response,
    )

    assert response.status_code == 200
    assert 'Last-Modified' not in response.headers
    assert 'ETag' not in response.headers
