"""
Shared DocumentDatabase contract checks, run against real MongoDB and Elasticsearch servers
via testcontainers. The same assertions apply to both backends, so behavioural drift between
drivers shows up as a failure on one parametrisation.

Like the other `*_checks.py` modules this tier needs Docker; it is skipped without it and
included in CI via `-o python_files="test_*.py *_checks.py"`.
"""
from uuid import uuid4

import pytest
import pytest_asyncio

from bioterms.etc.consts import CONFIG, PH
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from bioterms.model.user import User, UserApiKey

MONGO_IMAGE = 'mongo:8.0'
ELASTICSEARCH_IMAGE = 'docker.elastic.co/elasticsearch/elasticsearch:9.1.5'


def _docker_available() -> bool:
    try:
        import docker
        client = docker.from_env()
        try:
            client.ping()
            return True
        finally:
            client.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason='Docker is not available in this environment')


@pytest.fixture(scope='session')
def mongo_url():
    from testcontainers.mongodb import MongoDbContainer

    with MongoDbContainer(MONGO_IMAGE) as container:
        yield container.get_connection_url()


@pytest.fixture(scope='session')
def elasticsearch_url():
    # testcontainers' ElasticSearchContainer rejects 9.x images, so start it generically and
    # wait for the HTTP API instead.
    import time
    import urllib.request
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(ELASTICSEARCH_IMAGE) \
        .with_exposed_ports(9200) \
        .with_env('discovery.type', 'single-node') \
        .with_env('xpack.security.enabled', 'false') \
        .with_env('ES_JAVA_OPTS', '-Xms512m -Xmx512m')
    with container:
        url = f'http://{container.get_container_host_ip()}:{container.get_exposed_port(9200)}'
        deadline = time.monotonic() + 180
        while True:
            try:
                with urllib.request.urlopen(f'{url}/_cluster/health?wait_for_status=yellow&timeout=5s'):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(2)
        yield url


@pytest_asyncio.fixture(params=['mongo', 'elasticsearch'])
async def doc_db(request, monkeypatch):
    """A freshly initialised backend with no data from previous tests."""
    if request.param == 'mongo':
        from pymongo import AsyncMongoClient
        from bioterms.database.doc_db.mongo_doc_db import MongoDocumentDatabase

        monkeypatch.setattr(CONFIG, 'mongodb_db_name', f'bts_{uuid4().hex}')
        client = AsyncMongoClient(request.getfixturevalue('mongo_url'))
        db = MongoDocumentDatabase(client)
    else:
        from elasticsearch import AsyncElasticsearch
        from bioterms.database.doc_db.elasticsearch_doc_db import ElasticsearchDocumentDatabase

        monkeypatch.setattr(CONFIG, 'elasticsearch_index_prefix', f'bts-{uuid4().hex[:12]}')
        db = ElasticsearchDocumentDatabase(AsyncElasticsearch(request.getfixturevalue('elasticsearch_url')))

    await db.initialize()
    try:
        yield db
    finally:
        await db.close()


async def _refresh(db):
    """Elasticsearch search is near-real-time; make writes visible before reading them back."""
    client = getattr(db, 'client', None)
    if client is not None and hasattr(client, 'indices') and hasattr(client.indices, 'refresh'):
        await client.indices.refresh(index='_all')


def concept(concept_id, label, synonyms=None, prefix=ConceptPrefix.HPO):
    return Concept(prefix=prefix, conceptId=concept_id, label=label, synonyms=synonyms)


TERMS = [
    concept('HP:0001250', 'Seizure', ['Epileptic seizure', 'Convulsion']),
    concept('HP:0002069', 'Bilateral tonic-clonic seizure', ['Grand mal seizure']),
    concept('HP:0001263', 'Global developmental delay'),
    concept('HP:0000750', 'Delayed speech and language development'),
    concept('HP:0000478', 'Abnormality of the eye'),
]


@pytest.mark.asyncio
async def test_save_count_read_and_delete_terms(doc_db):
    await doc_db.create_index(ConceptPrefix.HPO, 'conceptId', unique=True)
    await doc_db.create_index(ConceptPrefix.HPO, 'label')
    await doc_db.save_terms(TERMS, no_upsert=True)
    await doc_db.save_terms([concept('HP:0000478', 'Abnormality of the eye (updated)')])
    await doc_db.save_terms([concept('MONDO:1', 'disease', prefix=ConceptPrefix.MONDO)])
    await _refresh(doc_db)

    assert await doc_db.count_terms(ConceptPrefix.HPO) == 5
    assert len(await doc_db.get_terms(ConceptPrefix.HPO, limit=2)) == 2
    by_id = await doc_db.get_terms_by_ids(ConceptPrefix.HPO, ['HP:0000478', 'HP:missing', 'HP:0001250'])
    assert {c.concept_id: c.label for c in by_id} == {
        'HP:0000478': 'Abnormality of the eye (updated)', 'HP:0001250': 'Seizure',
    }
    assert set(await doc_db.get_random_term_ids(ConceptPrefix.HPO, 3)) <= {t.concept_id for t in TERMS}

    await doc_db.delete_index(ConceptPrefix.HPO, 'label')
    await doc_db.delete_all_for_label(ConceptPrefix.HPO)
    await _refresh(doc_db)
    assert await doc_db.count_terms(ConceptPrefix.HPO) == 0
    assert await doc_db.count_terms(ConceptPrefix.MONDO) == 1


@pytest.mark.asyncio
async def test_lexical_fuzzy_and_auto_complete_search(doc_db):
    await doc_db.create_index(ConceptPrefix.HPO, 'conceptId', unique=True)
    await doc_db.save_terms(TERMS)
    await _refresh(doc_db)

    lexical = await doc_db.lexical_search(ConceptPrefix.HPO, 'tonic clonic seizure', limit=5)
    assert lexical[0][0] == 'HP:0002069'
    assert {cid for cid, _ in lexical} <= {t.concept_id for t in TERMS}
    assert [score for _, score in lexical] == sorted((score for _, score in lexical), reverse=True)

    # Fuzzy recall is optional per backend (no safe indexed implementation -> no results),
    # but whatever is returned must be real concepts of this vocabulary.
    fuzzy = await doc_db.fuzzy_search(ConceptPrefix.HPO, 'seizrue', limit=5)
    assert {cid for cid, _ in fuzzy} <= {t.concept_id for t in TERMS}

    completions = await doc_db.auto_complete_search(ConceptPrefix.HPO, 'dev', limit=5)
    assert {c.concept_id for c in completions} >= {'HP:0001263', 'HP:0000750'}
    assert 'HP:0000478' not in {c.concept_id for c in completions}


@pytest.mark.asyncio
async def test_user_repository_round_trip(doc_db):
    users = doc_db.users
    await users.save(User(username='alice', password=PH.hash('pw')))
    await users.save(User(username='bob', password=PH.hash('pw')))
    await _refresh(doc_db)

    alice = await users.get('alice')
    assert alice.validate_password('pw')
    assert {u.username for u in await users.filter()} == {'alice', 'bob'}

    key = UserApiKey(name='ci', keyHash='a' * 64)
    await users.save_api_key(username='alice', api_key=key)
    await _refresh(doc_db)
    assert (await users.get_user_by_api_key('a' * 64)).username == 'alice'
    assert await users.get_user_by_api_key('b' * 64) is None

    await users.delete_api_key(username='alice', key_id=key.key_id)
    await users.delete('bob')
    await _refresh(doc_db)
    assert not (await users.get('alice')).api_keys
    assert await users.get('bob') is None
