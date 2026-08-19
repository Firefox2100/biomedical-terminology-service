"""
Integration test suite for SqlDocumentDatabase, run against a real, ephemeral PostgreSQL
container via testcontainers -- as opposed to tests/unit_test/database/test_sql_doc_db.py,
which exercises the same driver against real SQLite (no Docker needed) but cannot validate
PostgreSQL-only SQL (the native `ON CONFLICT DO UPDATE` upsert path, the `payload->>'field'`
JSON index expression, `strpos`/`char_length`).

PostgreSQL is this project's primary/recommended SQL backend (see docs/source/build-database.rst),
so this is the tier that actually validates it end to end, since the SQLite-backed unit tests
only prove the driver is *portable*, not that the PostgreSQL-specific code path is correct.

Like tests/load_test and tests/integration_test/neo4j_container_checks.py, this tier is
intentionally NOT named test_*.py/*_test.py, so a bare `pytest` run does not pick it up
(starting a container per run would be slow and requires a working Docker daemon). Run it
explicitly:

    pytest tests/integration_test/sql_container_checks.py -v

Requires: a working Docker daemon reachable from this host, and network access the first time
to pull the postgres image (already present locally is fine/faster).
"""
import os

import pytest
import pytest_asyncio

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

from sqlalchemy.ext.asyncio import create_async_engine

from bioterms.database.doc_db.sql_doc_db import SqlDocumentDatabase
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.model.concept import Concept
from bioterms.model.user import User

# Pinned for reproducibility, matching the general pinning convention used for the other
# integration-test images (see neo4j_container_checks.py).
POSTGRES_IMAGE = 'postgres:18.6'


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
def postgres_container():
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(image=POSTGRES_IMAGE, driver='asyncpg') as container:
        yield container


@pytest_asyncio.fixture
async def doc_db(postgres_container):
    """
    A SqlDocumentDatabase backed by a fresh async engine against the shared Postgres
    container. Tables are dropped before each test for isolation; the container itself is
    only started once per session.
    """
    engine = create_async_engine(postgres_container.get_connection_url())

    async with engine.begin() as conn:
        from sqlalchemy import text
        result = await conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ))
        for (table_name,) in result:
            await conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

    db = SqlDocumentDatabase(engine, batch_size=10)
    await db.initialize()

    try:
        yield db
    finally:
        await db.close()


def make_concept(concept_id: str, label: str) -> Concept:
    return Concept(prefix=ConceptPrefix.HPO, conceptId=concept_id, label=label)


@pytest.mark.asyncio
async def test_save_terms_upserts_via_native_on_conflict(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo bar'), make_concept('HP:2', 'Baz qux')])
    assert await doc_db.count_terms(ConceptPrefix.HPO) == 2

    await doc_db.save_terms([make_concept('HP:1', 'Foo bar updated')])
    assert await doc_db.count_terms(ConceptPrefix.HPO) == 2

    terms = {t.concept_id: t for t in await doc_db.get_terms(ConceptPrefix.HPO)}
    assert terms['HP:1'].label == 'Foo bar updated'
    assert terms['HP:2'].label == 'Baz qux'


@pytest.mark.asyncio
async def test_update_vector_mapping_is_visible_on_read(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo bar')])
    await doc_db.update_vector_mapping(ConceptPrefix.HPO, {'HP:1': 'vec-1'})

    terms = await doc_db.get_terms(ConceptPrefix.HPO)
    assert terms[0].vector_id == 'vec-1'


@pytest.mark.asyncio
async def test_auto_complete_uses_strpos_and_char_length(doc_db):
    await doc_db.save_terms([
        make_concept('HP:1', 'diabetes mellitus'),
        make_concept('HP:2', 'type 1 diabetes'),
    ])

    results = await doc_db.auto_complete_search(ConceptPrefix.HPO, query='diabetes')
    assert [r.concept_id for r in results] == ['HP:1', 'HP:2']


@pytest.mark.asyncio
async def test_create_index_on_json_payload_field(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo')])

    await doc_db.create_index(ConceptPrefix.HPO, field='conceptId', unique=True)
    await doc_db.create_index(ConceptPrefix.HPO, field='label')
    await doc_db.create_index(ConceptPrefix.HPO, field='label', overwrite=True)
    # No dedicated column for "status" -- exercises the payload->>'field' expression index.
    await doc_db.create_index(ConceptPrefix.HPO, field='status')

    await doc_db.delete_index(ConceptPrefix.HPO, field='label')
    await doc_db.delete_index(ConceptPrefix.HPO, field='status')


@pytest.mark.asyncio
async def test_create_index_rejects_unsafe_field_name(doc_db):
    with pytest.raises(IndexCreationError):
        await doc_db.create_index(ConceptPrefix.HPO, field="label'); DROP TABLE users; --")


@pytest.mark.asyncio
async def test_user_repository_save_is_a_real_upsert(doc_db):
    users = doc_db.users

    await users.save(User(username='alice', password='hash1', apiKeys=[]))
    await users.save(User(username='alice', password='hash2', apiKeys=[]))

    fetched = await users.get('alice')
    assert fetched.password == 'hash2'


@pytest.mark.asyncio
async def test_delete_all_for_label_clears_data(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo')])
    await doc_db.delete_all_for_label(ConceptPrefix.HPO)
    assert await doc_db.count_terms(ConceptPrefix.HPO) == 0


@pytest.mark.asyncio
async def test_get_random_term_ids(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo'), make_concept('HP:2', 'Bar')])
    ids = await doc_db.get_random_term_ids(ConceptPrefix.HPO, 2)
    assert sorted(ids) == ['HP:1', 'HP:2']
