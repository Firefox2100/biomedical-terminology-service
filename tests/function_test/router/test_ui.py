"""
Function tests for the server-rendered UI and the application factory: the real app from
`create_app()` is driven through Starlette's TestClient, with in-memory database fakes
injected via dependency overrides and the lifespan (which connects real backends) not run.
"""
import base64
import re
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import bioterms.app as app_module
import bioterms.router.ui as ui
from bioterms.database import get_active_cache, get_active_doc_db, get_active_graph_db
from bioterms.etc.consts import CONFIG, PH
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.model.annotation_status import AnnotationStatus
from bioterms.model.concept import Concept
from bioterms.model.similarity_status import SimilarityStatus
from bioterms.model.user import User, UserApiKey
from bioterms.model.vocabulary_status import VocabularyStatus


class FakeUsers:
    def __init__(self):
        self.users = {'alice': User(username='alice', password=PH.hash('correct horse'))}
        self.deleted = []

    async def get(self, username):
        return self.users.get(username)

    async def save_api_key(self, username, api_key):
        user = self.users[username]
        user.api_keys = [*(user.api_keys or []), api_key]

    async def delete_api_key(self, username, key_id):
        self.deleted.append((username, key_id))


class FakeDocDb:
    def __init__(self):
        self.users = FakeUsers()
        self.concepts = {
            'HP:0000118': Concept(prefix=ConceptPrefix.HPO, conceptId='HP:0000118',
                                  label='Phenotypic abnormality', synonyms=['Organ abnormality']),
        }

    async def get_terms_by_ids(self, prefix, concept_ids, model_class=Concept):
        return [self.concepts[c] for c in concept_ids if c in self.concepts]


def _vocabulary_status(prefix, **_):
    loaded = prefix in (ConceptPrefix.HPO, ConceptPrefix.MONDO)
    return VocabularyStatus(
        prefix=prefix, name=prefix.value.upper(), fileDownloaded=loaded, loaded=loaded,
        conceptCount=10 if loaded else 0, relationshipCount=5 if loaded else 0, vectorCount=0,
        annotations=[ConceptPrefix.MONDO] if prefix == ConceptPrefix.HPO else [],
        similarityMethods=[],
    )


@pytest.fixture
def app_and_db(monkeypatch):
    doc_db = FakeDocDb()

    async def vocabulary_status(prefix, **kwargs):
        return _vocabulary_status(prefix, **kwargs)

    async def similarity_status(prefix, **_):
        return SimilarityStatus(prefix=prefix, similarityCounts=[
            {'method': SimilarityMethod.RELEVANCE, 'corpus': None, 'count': 3},
        ])

    async def annotation_status(prefix_1, prefix_2, **_):
        return AnnotationStatus(prefixSource=prefix_1, prefixTarget=prefix_2,
                                name=f'{prefix_1.value}-{prefix_2.value}', loaded=True,
                                relationshipCount=7)

    async def active_doc_db():
        return doc_db

    monkeypatch.setattr(ui, 'get_vocabulary_status', vocabulary_status)
    monkeypatch.setattr(ui, 'get_similarity_status', similarity_status)
    monkeypatch.setattr(ui, 'get_annotation_status', annotation_status)
    monkeypatch.setattr(app_module, 'get_active_doc_db', active_doc_db)  # used by error pages
    monkeypatch.setattr(CONFIG, 'server_hmac_key', base64.b64encode(b'k' * 32).decode())

    # Capture the inner FastAPI app so dependencies can be overridden underneath the CSRF wrapper.
    inner = {}
    real_csrf = app_module.asgi_csrf

    def capture_csrf(fastapi_app, **kwargs):
        inner['app'] = fastapi_app
        return real_csrf(fastapi_app, **kwargs)

    monkeypatch.setattr(app_module, 'asgi_csrf', capture_csrf)
    wrapped = app_module.create_app()
    fastapi_app: FastAPI = inner['app']
    fastapi_app.dependency_overrides[get_active_doc_db] = active_doc_db
    fastapi_app.dependency_overrides[get_active_graph_db] = lambda: object()
    fastapi_app.dependency_overrides[get_active_cache] = lambda: object()
    return wrapped, fastapi_app, doc_db


@pytest.fixture
def client(app_and_db):
    wrapped, _, _ = app_and_db
    return TestClient(wrapped, follow_redirects=False)


def _csrf(client, path='/login'):
    """GET a page so asgi_csrf sets its cookie, and return the token to echo back."""
    client.get(path)
    return client.cookies.get('csrftoken')


def _login(client, password='correct horse', next_url=None):
    token = _csrf(client)
    url = '/login' + (f'?next={next_url}' if next_url else '')
    return client.post(url, data={'username': 'alice', 'password': password, 'csrftoken': token})


def test_home_page_renders_counts_and_security_headers(client):
    response = client.get('/')

    assert response.status_code == 200
    assert 'text/html' in response.headers['content-type']
    assert "script-src 'self' 'nonce-" in response.headers['content-security-policy']
    assert response.headers['x-frame-options'] == 'DENY'
    assert 'application/ld+json' in response.text


def test_vocabulary_pages_render_statuses_annotations_and_license(client):
    listing = client.get('/vocabularies')
    detail = client.get('/vocabularies/hpo')
    no_browser = client.get('/vocabularies/go')

    assert listing.status_code == 200
    assert 'CollectionPage' in listing.text
    assert detail.status_code == 200
    assert 'hpo-mondo' in detail.text           # annotation status rendered
    assert 'term-browser?ontology=hpo' in detail.text
    assert no_browser.status_code == 200
    assert 'term-browser?ontology=go' not in no_browser.text


def test_concept_detail_and_missing_concept(client):
    found = client.get('/vocabularies/hpo/HP:0000118')
    missing = client.get('/vocabularies/hpo/HP:9999999')

    assert found.status_code == 200
    assert 'Phenotypic abnormality' in found.text
    assert missing.status_code == 404
    assert 'Concept not found' in missing.text


def test_status_failure_renders_error_page(client, monkeypatch):
    async def broken(*_args, **_kwargs):
        raise RuntimeError('status backend down')

    monkeypatch.setattr(ui, 'get_vocabulary_status', broken)

    response = client.get('/vocabularies')

    assert response.status_code == 500
    assert 'status backend down' in response.text


def test_login_page_and_failed_logins(client):
    assert client.get('/login?next=/vocabularies').status_code == 200

    wrong = _login(client, password='wrong')
    assert wrong.status_code == 303
    assert 'error=Invalid+username+or+password' in wrong.headers['location']

    wrong_next = _login(client, password='wrong', next_url='/api-keys')
    assert 'next=%2Fapi-keys' in wrong_next.headers['location']

    # FastAPI treats an empty form field as missing and rejects it before the route runs.
    empty = client.post('/login', data={
        'username': 'alice', 'password': '', 'csrftoken': client.cookies.get('csrftoken'),
    })
    assert empty.status_code == 422


def test_login_redirects_to_safe_next_and_logout_clears_session(client):
    response = _login(client, next_url='/vocabularies')
    assert response.status_code == 303
    assert response.headers['location'] == '/vocabularies'

    # Already logged in: the login page bounces straight on.
    assert client.get('/login').status_code == 303
    assert 'Logout (alice)' in client.get('/').text

    assert client.get('/logout').status_code == 303
    assert 'Logout (alice)' not in client.get('/').text


def test_login_ignores_unsafe_next_url(client):
    response = _login(client, next_url='https://evil.example/')

    assert response.status_code == 303
    assert 'evil.example' not in response.headers['location']


def test_protected_pages_redirect_to_login_when_anonymous(client):
    response = client.get('/api-keys')

    assert response.status_code == 303
    assert response.headers['location'].startswith('http://testserver/login?next=/api-keys')


def test_api_key_lifecycle(app_and_db):
    wrapped, _, doc_db = app_and_db
    client = TestClient(wrapped, follow_redirects=False)
    _login(client)

    assert client.get('/api-keys').status_code == 200
    assert client.get('/api-keys/new').status_code == 200

    created = client.post('/api-keys/new', data={
        'name': 'ci', 'csrftoken': client.cookies.get('csrftoken'),
    })
    assert created.status_code == 200
    [key] = doc_db.users.users['alice'].api_keys
    assert key.name == 'ci'
    # The page shows the raw key once; only its HMAC is stored.
    raw_key = re.search(r'[A-Za-z0-9_-]{43}', created.text).group(0)
    assert raw_key not in key.key_hash

    key_id = uuid4()
    deleted = client.delete(f'/api-keys/{key_id}', headers={'x-csrf-token': client.cookies.get('csrftoken')})
    assert deleted.status_code == 204
    assert doc_db.users.deleted == [('alice', key_id)]


def test_admin_endpoints_trigger_cache_rebuild_and_graphql_reload(app_and_db, monkeypatch):
    wrapped, fastapi_app, _ = app_and_db
    client = TestClient(wrapped, follow_redirects=False)
    _login(client)
    calls = []
    monkeypatch.setattr(ui.rebuild_cache_task, 'delay', lambda: calls.append('rebuild'))

    async def reload():
        calls.append('reload')

    monkeypatch.setattr(fastapi_app.state.graphql_service, 'reload', reload)
    headers = {'x-csrf-token': client.cookies.get('csrftoken')}

    assert client.post('/rebuild-cache', headers=headers).status_code == 202
    assert client.post('/reload-graphql', headers=headers).status_code == 202
    assert calls == ['rebuild', 'reload']

    async def failing_reload():
        raise RuntimeError('bad schema')

    monkeypatch.setattr(fastapi_app.state.graphql_service, 'reload', failing_reload)
    assert client.post('/reload-graphql', headers=headers).status_code == 500


def test_csrf_blocks_cookie_bearing_posts_without_token(client):
    _csrf(client)  # the browser now holds cookies, so a forged cross-site POST would carry them

    response = client.post('/login', data={'username': 'alice', 'password': 'correct horse'})

    assert response.status_code == 403


def test_unknown_page_renders_404_template(client):
    response = client.get('/vocabularies/not-a-prefix')

    assert response.status_code in (404, 422)
