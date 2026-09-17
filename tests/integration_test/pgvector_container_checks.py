"""
Integration test suite for PostgresVectorDatabase, run against a real, ephemeral PostgreSQL +
pgvector container via testcontainers -- as opposed to mocking the engine/connection outright.
This validates the pgvector-specific SQL (the "vector" column type, HNSW index creation,
cosine-distance ordering, and running as a vector store alongside a PostgreSQL document
database on the same instance without corrupting its rows) against a real server.

Like tests/load_test and the other tests/integration_test modules, this tier is intentionally
NOT named test_*.py/*_test.py, so a bare `pytest` run does not pick it up (starting a container
per run would be slow and requires a working Docker daemon). Run it explicitly:

    pytest tests/integration_test/pgvector_container_checks.py -v

Requires: a working Docker daemon reachable from this host, and network access the first time to
pull the pgvector/pgvector image (already present locally is fine/faster).
"""
import pytest
import pytest_asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from bioterms.database.doc_db.sql_doc_db import SqlDocumentDatabase
from bioterms.database.vector_db.postgres_vector_db import PostgresVectorDatabase
from bioterms.database.vector_db.vector_db import EmbeddingItemVector
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
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

    @property
    def dimension(self):
        return EMBEDDING_DIMENSION

    def embed_strings(self, texts):
        return [[1.0, 0.0, 0.0, 0.0] for _ in texts]


@pytest.fixture(scope='session')
def postgres_container():
    from testcontainers.postgres import PostgresContainer

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


async def _items_iter(items):
    for item in items:
        yield item


def item(item_id: str, concept_id: str, kind: EmbeddingKind, text_: str, vector: list[float]) -> EmbeddingItemVector:
    return EmbeddingItemVector(item_id=item_id, concept_id=concept_id, kind=kind, text=text_, vector=vector)


@pytest.mark.asyncio
async def test_load_embedding_items_and_count(clean_engine, monkeypatch):
    monkeypatch.setattr(
        'bioterms.database.vector_db.postgres_vector_db.TextTransformer',
        FakeTextTransformer,
    )
    vector_db = PostgresVectorDatabase(engine=clean_engine)

    written = await vector_db.load_embedding_items(
        prefix=ConceptPrefix.HPO,
        items=_items_iter([
            item('HP:1:alias:0', 'HP:1', EmbeddingKind.ALIAS, 'foo', [1.0, 0.0, 0.0, 0.0]),
            item('HP:2:alias:0', 'HP:2', EmbeddingKind.ALIAS, 'bar', [0.0, 1.0, 0.0, 0.0]),
        ]),
    )

    assert written == 2
    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 2


@pytest.mark.asyncio
async def test_load_embedding_items_upserts_by_item_id(clean_engine, monkeypatch):
    monkeypatch.setattr(
        'bioterms.database.vector_db.postgres_vector_db.TextTransformer',
        FakeTextTransformer,
    )
    vector_db = PostgresVectorDatabase(engine=clean_engine)

    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.HPO,
        items=_items_iter([item('HP:1:alias:0', 'HP:1', EmbeddingKind.ALIAS, 'foo', [1.0, 0.0, 0.0, 0.0])]),
    )
    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.HPO,
        items=_items_iter([item('HP:1:alias:0', 'HP:1', EmbeddingKind.ALIAS, 'foo v2', [0.0, 0.0, 1.0, 0.0])]),
    )

    # Same item_id upserts in place rather than accumulating a second row.
    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 1

    hits = await vector_db.search_items(
        query_vector=[0.0, 0.0, 1.0, 0.0], prefix=ConceptPrefix.HPO, kind=EmbeddingKind.ALIAS, limit=1,
    )
    assert hits[0][1] == 'foo v2'


@pytest.mark.asyncio
async def test_search_items_orders_by_cosine_distance_and_filters_by_kind(clean_engine, monkeypatch):
    monkeypatch.setattr(
        'bioterms.database.vector_db.postgres_vector_db.TextTransformer',
        FakeTextTransformer,
    )
    vector_db = PostgresVectorDatabase(engine=clean_engine)
    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.SNOMED,
        items=_items_iter([
            item('123:alias:0', '123', EmbeddingKind.ALIAS, 'diabetes', [1.0, 0.0, 0.0, 0.0]),  # identical
            item('456:alias:0', '456', EmbeddingKind.ALIAS, 'other', [0.0, 1.0, 0.0, 0.0]),  # orthogonal
            item('789:definition:0', '789', EmbeddingKind.DEFINITION, 'a disease', [1.0, 0.0, 0.0, 0.0]),
        ]),
    )

    alias_hits = await vector_db.search_items(
        query_vector=[1.0, 0.0, 0.0, 0.0], prefix=ConceptPrefix.SNOMED, kind=EmbeddingKind.ALIAS, limit=2,
    )
    assert [concept_id for concept_id, _text, _score in alias_hits] == ['123', '456']

    definition_hits = await vector_db.search_items(
        query_vector=[1.0, 0.0, 0.0, 0.0], prefix=ConceptPrefix.SNOMED, kind=EmbeddingKind.DEFINITION, limit=2,
    )
    assert [concept_id for concept_id, _text, _score in definition_hits] == ['789']


@pytest.mark.asyncio
async def test_delete_vectors_for_prefix_drops_table(clean_engine, monkeypatch):
    monkeypatch.setattr(
        'bioterms.database.vector_db.postgres_vector_db.TextTransformer',
        FakeTextTransformer,
    )
    vector_db = PostgresVectorDatabase(engine=clean_engine)
    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.HPO,
        items=_items_iter([item('HP:1:alias:0', 'HP:1', EmbeddingKind.ALIAS, 'foo', [1.0, 0.0, 0.0, 0.0])]),
    )

    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HPO)

    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 0
    # Deleting again (table already gone) must not raise.
    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HPO)


@pytest.mark.asyncio
async def test_vector_store_coexists_with_doc_db_on_same_postgres_instance(clean_engine, monkeypatch):
    """
    When an admin points both BTS_SQL_DB_URL and BTS_POSTGRES_VECTOR_DB_URL at the same
    PostgreSQL instance, the vector-item table must sit alongside the document database's own
    `concept_<prefix>` table without touching it -- there is no more "shared column" mode.
    """
    monkeypatch.setattr(
        'bioterms.database.vector_db.postgres_vector_db.TextTransformer',
        FakeTextTransformer,
    )

    doc_db = SqlDocumentDatabase(clean_engine, batch_size=10)
    await doc_db.initialize()
    await doc_db.save_terms([make_concept('HP:1', 'Foo bar'), make_concept('HP:2', 'Baz qux')])

    vector_db = PostgresVectorDatabase(engine=clean_engine)
    await vector_db.load_embedding_items(
        prefix=ConceptPrefix.HPO,
        items=_items_iter([item('HP:1:alias:0', 'HP:1', EmbeddingKind.ALIAS, 'foo', [1.0, 0.0, 0.0, 0.0])]),
    )

    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 1

    # The document rows are untouched, including HP:2 which was never embedded.
    terms = {t.concept_id: t for t in await doc_db.get_terms(ConceptPrefix.HPO)}
    assert terms['HP:1'].label == 'Foo bar'
    assert terms['HP:2'].label == 'Baz qux'

    async with clean_engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%vector_item%'"
        ))
        assert [row[0] for row in result.fetchall()] == ['concept_hpo_vector_item']

    await vector_db.delete_vectors_for_prefix(ConceptPrefix.HPO)
    assert await vector_db.count_vectors(ConceptPrefix.HPO) == 0
    # Dropping the vector-item table must not touch the document table.
    terms_after = {t.concept_id: t for t in await doc_db.get_terms(ConceptPrefix.HPO)}
    assert terms_after['HP:1'].label == 'Foo bar'

    await doc_db.close()
