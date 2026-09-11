"""
Integration test suite for PostgresVectorDatabase, run against a real, ephemeral PostgreSQL +
pgvector container via testcontainers -- as opposed to mocking the engine/connection outright.
This validates the pgvector-specific SQL (the "vector" column type, HNSW index creation,
cosine-distance ordering, and -- for shared mode -- sharing physical tables with
SqlDocumentDatabase without corrupting its rows) against a real server.

Like tests/load_test and the other tests/integration_test modules, this tier is intentionally
NOT named test_*.py/*_test.py, so a bare `pytest` run does not pick it up (starting a container
per run would be slow and requires a working Docker daemon). Run it explicitly:

    pytest tests/integration_test/pgvector_container_checks.py -v

Requires: a working Docker daemon reachable from this host, and network access the first time to
pull the pgvector/pgvector image (already present locally is fine/faster).
"""
import os

import pytest
import pytest_asyncio

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from bioterms.database.doc_db.sql_doc_db import SqlDocumentDatabase
from bioterms.database.vector_db.postgres_vector_db import PostgresVectorDatabase
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept

# The plain postgres image does not bundle the pgvector extension; pgvector/pgvector does, and
# tracks upstream Postgres major versions via its tag (see https://hub.docker.com/r/pgvector/pgvector).
PGVECTOR_IMAGE = 'pgvector/pgvector:0.8.6-pg18'

EMBEDDING_DIMENSION = 4


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


class FakeTextTransformer:
    """
    Stands in for the real sentence-transformers-backed TextTransformer, so search tests don't
    need to download/run an actual embedding model. Always embeds to a fixed vector matching
    EMBEDDING_DIMENSION, so search ordering can be controlled precisely by test data.
    """
    def __init__(self, *args, **kwargs):
        pass

    def embed_strings(self, texts):
        return [[1.0, 0.0, 0.0, 0.0] for _ in texts]


@pytest.fixture(scope='session')
def postgres_container():
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(image=PGVECTOR_IMAGE, driver='asyncpg') as container:
        yield container


@pytest_asyncio.fixture
async def clean_engine(postgres_container):
    """
    A fresh async engine against the shared container, with every table in the public schema
    dropped first for test isolation.
    """
    engine = create_async_engine(postgres_container.get_connection_url())

    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))
        for (table_name,) in result:
            await conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

    try:
        yield engine
    finally:
        await engine.dispose()


def make_concept(concept_id: str, label: str) -> Concept:
    return Concept(prefix=ConceptPrefix.HPO, conceptId=concept_id, label=label)


async def _embeddings_iter(items):
    for concept_id, vector_id, vector in items:
        yield concept_id, vector_id, vector


@pytest.mark.asyncio
async def test_standalone_load_embeddings_and_count(clean_engine):
    vector_db = PostgresVectorDatabase(
        engine=clean_engine, embedding_dimension=EMBEDDING_DIMENSION, shared_with_doc_db=False,
    )

    id_map = await vector_db.load_embeddings(
        prefix=ConceptPrefix.HPO,
        embeddings=_embeddings_iter([
            ('HP:1', 'HP:1', [1.0, 0.0, 0.0, 0.0]),
            ('HP:2', 'HP:2', [0.0, 1.0, 0.0, 0.0]),
        ]),
    )

    assert id_map == {'HP:1': 'HP:1', 'HP:2': 'HP:2'}
    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 2

    vectors = await vector_db.get_vectors_for_prefix(ConceptPrefix.HPO)
    assert vectors['HP:1'] == pytest.approx([1.0, 0.0, 0.0, 0.0])
    assert vectors['HP:2'] == pytest.approx([0.0, 1.0, 0.0, 0.0])


@pytest.mark.asyncio
async def test_standalone_load_embeddings_upserts(clean_engine):
    vector_db = PostgresVectorDatabase(
        engine=clean_engine, embedding_dimension=EMBEDDING_DIMENSION, shared_with_doc_db=False,
    )

    await vector_db.load_embeddings(
        prefix=ConceptPrefix.HPO,
        embeddings=_embeddings_iter([('HP:1', 'HP:1', [1.0, 0.0, 0.0, 0.0])]),
    )
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.HPO,
        embeddings=_embeddings_iter([('HP:1', 'HP:1', [0.0, 0.0, 1.0, 0.0])]),
    )

    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 1
    vectors = await vector_db.get_vectors_for_prefix(ConceptPrefix.HPO)
    assert vectors['HP:1'] == pytest.approx([0.0, 0.0, 1.0, 0.0])


@pytest.mark.asyncio
async def test_standalone_search_concepts_orders_by_cosine_distance(clean_engine, monkeypatch):
    monkeypatch.setattr(
        'bioterms.database.vector_db.postgres_vector_db.TextTransformer',
        FakeTextTransformer,
    )

    vector_db = PostgresVectorDatabase(
        engine=clean_engine, embedding_dimension=EMBEDDING_DIMENSION, shared_with_doc_db=False,
    )
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.SNOMED,
        embeddings=_embeddings_iter([
            ('123', '123', [1.0, 0.0, 0.0, 0.0]),   # identical to the (fake) query embedding
            ('456', '456', [0.0, 1.0, 0.0, 0.0]),   # orthogonal
        ]),
    )

    concept_ids = await vector_db.search_concepts(query='diabetes', prefix=ConceptPrefix.SNOMED, limit=2)
    assert concept_ids == ['123', '456']


@pytest.mark.asyncio
async def test_standalone_delete_vectors_for_prefix_drops_table(clean_engine):
    vector_db = PostgresVectorDatabase(
        engine=clean_engine, embedding_dimension=EMBEDDING_DIMENSION, shared_with_doc_db=False,
    )
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.HPO,
        embeddings=_embeddings_iter([('HP:1', 'HP:1', [1.0, 0.0, 0.0, 0.0])]),
    )

    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HPO)

    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 0
    # Deleting again (table already gone) must not raise.
    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HPO)


@pytest.mark.asyncio
async def test_shared_mode_stores_vector_on_doc_db_table_without_corrupting_it(clean_engine):
    doc_db = SqlDocumentDatabase(clean_engine, batch_size=10)
    await doc_db.initialize()
    await doc_db.save_terms([make_concept('HP:1', 'Foo bar'), make_concept('HP:2', 'Baz qux')])

    vector_db = PostgresVectorDatabase(
        engine=clean_engine, embedding_dimension=EMBEDDING_DIMENSION, shared_with_doc_db=True,
    )
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.HPO,
        embeddings=_embeddings_iter([('HP:1', 'HP:1', [1.0, 0.0, 0.0, 0.0])]),
    )

    # Visible through the vector store...
    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 1
    vectors = await vector_db.get_vectors_for_prefix(ConceptPrefix.HPO)
    assert vectors['HP:1'] == pytest.approx([1.0, 0.0, 0.0, 0.0])

    # ...and the underlying concept row is untouched other than gaining the vector.
    terms = {t.concept_id: t for t in await doc_db.get_terms(ConceptPrefix.HPO)}
    assert terms['HP:1'].label == 'Foo bar'
    assert terms['HP:2'].label == 'Baz qux'
    # HP:2 was never embedded, so it must not have picked up a vector from anywhere.
    assert 'HP:2' not in vectors

    # No second, separate vector-only table was created.
    async with clean_engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%vector%'"
        ))
        assert result.fetchall() == []

    await doc_db.close()


@pytest.mark.asyncio
async def test_shared_mode_delete_clears_column_not_rows(clean_engine):
    doc_db = SqlDocumentDatabase(clean_engine, batch_size=10)
    await doc_db.initialize()
    await doc_db.save_terms([make_concept('HP:1', 'Foo bar')])

    vector_db = PostgresVectorDatabase(
        engine=clean_engine, embedding_dimension=EMBEDDING_DIMENSION, shared_with_doc_db=True,
    )
    await vector_db.load_embeddings(
        prefix=ConceptPrefix.HPO,
        embeddings=_embeddings_iter([('HP:1', 'HP:1', [1.0, 0.0, 0.0, 0.0])]),
    )

    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HPO)

    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 0
    # The concept document itself must still be there.
    terms = await doc_db.get_terms(ConceptPrefix.HPO)
    assert [t.concept_id for t in terms] == ['HP:1']
    assert terms[0].label == 'Foo bar'

    await doc_db.close()
