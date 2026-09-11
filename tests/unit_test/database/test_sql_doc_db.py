"""
Unit tests for SqlDocumentDatabase, run against a real, ephemeral SQLite database via
aiosqlite -- as opposed to mocking the engine/connection outright. SQLite is one of the
dialects the driver has to support and requires no external service, so it is used here to
exercise the real upsert/index/read code paths on every `pytest` run, rather than only
checking the shape of generated query text.

PostgreSQL-specific behaviour (the native `ON CONFLICT` path, the JSON `->>`-based index
expression) is covered separately in tests/integration_test/sql_container_checks.py against a
real Postgres container, since PostgreSQL is the primary supported SQL backend.
"""
import os

import pytest
import pytest_asyncio

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

from sqlalchemy.ext.asyncio import create_async_engine

from bioterms.database.doc_db.sql_doc_db import SqlDocumentDatabase
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from bioterms.model.user import User, UserApiKey


@pytest_asyncio.fixture
async def doc_db():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    db = SqlDocumentDatabase(engine, batch_size=10)
    await db.initialize()
    try:
        yield db
    finally:
        await db.close()


def make_concept(concept_id: str, label: str) -> Concept:
    return Concept(prefix=ConceptPrefix.HPO, conceptId=concept_id, label=label)


@pytest.mark.asyncio
async def test_save_terms_upserts_existing_rows(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo bar'), make_concept('HP:2', 'Baz qux')])
    assert await doc_db.count_terms(ConceptPrefix.HPO) == 2

    # Re-saving an existing concept_id must update in place, not duplicate or raise.
    await doc_db.save_terms([make_concept('HP:1', 'Foo bar updated')])
    assert await doc_db.count_terms(ConceptPrefix.HPO) == 2

    terms = {t.concept_id: t for t in await doc_db.get_terms(ConceptPrefix.HPO)}
    assert terms['HP:1'].label == 'Foo bar updated'
    assert terms['HP:2'].label == 'Baz qux'


@pytest.mark.asyncio
async def test_save_terms_no_upsert_does_plain_insert(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo')], no_upsert=True)
    assert await doc_db.count_terms(ConceptPrefix.HPO) == 1


@pytest.mark.asyncio
async def test_lexical_search_ranks_more_overlap_first(doc_db):
    # SQLite has no native trigram/ngram full-text feature this driver detects (it looks for
    # the FTS5 "trigram" tokenizer, not FTS5 itself), so this exercises the fallback n-gram
    # overlap-count scoring path.
    await doc_db.save_terms([
        make_concept('HP:1', 'diabetes mellitus'),
        make_concept('HP:2', 'diabetes insipidus'),
        make_concept('HP:3', 'unrelated condition'),
    ])

    results = await doc_db.lexical_search(ConceptPrefix.HPO, query='diabetes mellitus', limit=10)
    ranked_ids = [concept_id for concept_id, _score in results]

    # 'HP:1' shares both query words with the label; 'HP:2' shares only one; 'HP:3' shares none.
    assert ranked_ids[0] == 'HP:1'
    assert 'HP:3' not in ranked_ids


@pytest.mark.asyncio
async def test_auto_complete_ranks_prefix_match_first(doc_db):
    # Matching is case-sensitive (Concept.search_text() does not lowercase the label, mirroring
    # the Mongo driver's behaviour), so the query and labels are lowercase here on purpose.
    await doc_db.save_terms([
        make_concept('HP:1', 'diabetes mellitus'),
        make_concept('HP:2', 'type 1 diabetes'),
    ])

    results = await doc_db.auto_complete_search(ConceptPrefix.HPO, query='diabetes')
    assert [r.concept_id for r in results] == ['HP:1', 'HP:2']


@pytest.mark.asyncio
async def test_create_index_on_dedicated_column_and_json_field(doc_db):
    await doc_db.save_terms([make_concept('HP:1', 'Foo')])

    # conceptId maps to the primary key -- a no-op, must not raise.
    await doc_db.create_index(ConceptPrefix.HPO, field='conceptId', unique=True)
    # label has a dedicated column.
    await doc_db.create_index(ConceptPrefix.HPO, field='label')
    await doc_db.create_index(ConceptPrefix.HPO, field='label', overwrite=True)
    # status has no dedicated column -- falls back to a JSON path expression index.
    await doc_db.create_index(ConceptPrefix.HPO, field='status')

    await doc_db.delete_index(ConceptPrefix.HPO, field='label')
    await doc_db.delete_index(ConceptPrefix.HPO, field='status')
    # conceptId again: nothing to delete, must not raise.
    await doc_db.delete_index(ConceptPrefix.HPO, field='conceptId')


@pytest.mark.asyncio
async def test_create_index_rejects_unsafe_field_name(doc_db):
    from bioterms.etc.errors import IndexCreationError

    with pytest.raises(IndexCreationError):
        await doc_db.create_index(ConceptPrefix.HPO, field="label'); DROP TABLE users; --")


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


@pytest.mark.asyncio
async def test_user_repository_save_is_a_real_upsert(doc_db):
    users = doc_db.users

    await users.save(User(username='alice', password='hash1', apiKeys=[]))
    await users.save(User(username='alice', password='hash2', apiKeys=[]))

    fetched = await users.get('alice')
    assert fetched.password == 'hash2'
    assert len(await users.filter()) == 1


@pytest.mark.asyncio
async def test_user_repository_api_keys(doc_db):
    import datetime
    import uuid

    users = doc_db.users
    await users.save(User(username='bob', password='hash', apiKeys=[]))

    key = UserApiKey(
        keyId=uuid.uuid4(),
        name='ci-key',
        keyHash='hash-of-key',
        createdAt=datetime.datetime.now(datetime.timezone.utc),
    )
    await users.save_api_key('bob', key)

    fetched = await users.get_user_by_api_key('hash-of-key')
    assert fetched.username == 'bob'

    await users.delete_api_key('bob', key.key_id)
    fetched = await users.get('bob')
    assert fetched.api_keys == []

    await users.delete('bob')
    assert await users.get('bob') is None
