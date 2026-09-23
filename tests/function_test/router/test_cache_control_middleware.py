from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from starlette.responses import JSONResponse

from bioterms.router import utils
from bioterms.router.utils import CacheControlMiddleware


class FakeCache:
    async def get_dataset_last_modified(self):
        return datetime(2026, 1, 2, 3, 4, 5)


class FakeRequest:
    def __init__(self, path, headers=None, method='GET', query=''):
        self.method = method
        self.headers = headers or {}
        self.url = SimpleNamespace(path=path, query=query)


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
    assert response.headers['ETag'].startswith('W/"')


@pytest.mark.asyncio
async def test_cache_control_headers_are_added_to_global_search_v2(monkeypatch):
    monkeypatch.setattr(utils, 'get_active_cache', lambda: FakeCache())
    middleware = CacheControlMiddleware(app=None)

    response = await middleware.dispatch(
        FakeRequest('/api/search/v2'),
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
async def test_cache_control_handles_aware_dataset_timestamp(monkeypatch):
    class AwareFakeCache:
        async def get_dataset_last_modified(self):
            return datetime(2026, 1, 2, 3, 4, 5, 900000, tzinfo=timezone.utc)

    monkeypatch.setattr(utils, 'get_active_cache', lambda: AwareFakeCache())
    middleware = CacheControlMiddleware(app=None)
    response = await middleware.dispatch(
        FakeRequest(
            '/api/vocabularies/hpo',
            headers={'If-Modified-Since': 'Fri, 02 Jan 2026 03:04:05 GMT'},
        ),
        ok_response,
    )

    assert response.status_code == 304


@pytest.mark.asyncio
async def test_if_none_match_takes_precedence_over_if_modified_since(monkeypatch):
    monkeypatch.setattr(utils, 'get_active_cache', lambda: FakeCache())
    middleware = CacheControlMiddleware(app=None)
    response = await middleware.dispatch(
        FakeRequest(
            '/api/vocabularies/hpo',
            headers={
                'If-None-Match': 'W/"different"',
                'If-Modified-Since': 'Fri, 02 Jan 2026 03:04:05 GMT',
            },
        ),
        ok_response,
    )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_etag_is_request_specific_and_supports_conditional_get(monkeypatch):
    monkeypatch.setattr(utils, 'get_active_cache', lambda: FakeCache())
    middleware = CacheControlMiddleware(app=None)
    first = await middleware.dispatch(
        FakeRequest('/api/vocabularies/hpo/search/v2', query='query=heart'),
        ok_response,
    )
    other = await middleware.dispatch(
        FakeRequest('/api/vocabularies/hpo/search/v2', query='query=lung'),
        ok_response,
    )
    conditional = await middleware.dispatch(
        FakeRequest(
            '/api/vocabularies/hpo/search/v2',
            query='query=heart',
            headers={'If-None-Match': first.headers['ETag']},
        ),
        ok_response,
    )

    assert first.headers['ETag'] != other.headers['ETag']
    assert conditional.status_code == 304


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
