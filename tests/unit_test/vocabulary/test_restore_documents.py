"""
Confirms `_restore_documents` restores a `.doc.dump` identically whether or not it carries
the precomputed `nGrams`/`searchText` fallback-search fields `write_concepts_to_file(...,
build_search_index=False)` now lets a caller skip -- `save_terms` always recomputes its own
search indexing from the concept itself, so the dump's extra fields (or lack of them) must
not change what ends up in the document database.
"""
import os

import pytest
import pytest_asyncio

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from bioterms.database.doc_db.sql_doc_db import SqlDocumentDatabase
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept
from bioterms.vocabulary import _restore_documents
from bioterms.vocabulary.utils import write_concepts_to_file


@pytest_asyncio.fixture
async def doc_db():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    db = SqlDocumentDatabase(engine, batch_size=10)
    await db.initialize()
    try:
        yield db
    finally:
        await db.close()


def make_concept(concept_id, label, synonyms=None):
    return Concept(
        conceptTypes=[], prefix=ConceptPrefix.HPO, conceptId=concept_id, label=label,
        synonyms=synonyms, status=ConceptStatus.ACTIVE,
    )


async def _restored_rows(doc_db, doc_path):
    await _restore_documents(
        prefix=ConceptPrefix.HPO, doc_path=doc_path, concept_class=Concept,
        doc_db=doc_db, batch_size=10, no_upsert=True,
    )
    async with doc_db._engine.connect() as conn:
        result = await conn.execute(text('SELECT concept_id, search_text, label FROM concept_hpo ORDER BY concept_id'))
        return [dict(row._mapping) for row in result]


@pytest.mark.asyncio
async def test_restore_is_identical_with_and_without_precomputed_index_fields(monkeypatch, tmp_path, doc_db):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    concepts = [
        make_concept('HP:1', 'Fever', synonyms=['Pyrexia']),
        make_concept('HP:2', 'Headache', synonyms=['Cephalalgia', 'Head pain']),
    ]

    await write_concepts_to_file(prefix=ConceptPrefix.HPO, concepts=concepts, build_search_index=True)
    with_index_path = str(tmp_path / 'offline' / 'hpo.doc.dump')
    rows_with_index = await _restored_rows(doc_db, with_index_path)

    # Fresh database for the second restore, so the first doesn't influence it.
    engine2 = create_async_engine('sqlite+aiosqlite:///:memory:')
    doc_db2 = SqlDocumentDatabase(engine2, batch_size=10)
    await doc_db2.initialize()

    await write_concepts_to_file(prefix=ConceptPrefix.HPO, concepts=concepts, build_search_index=False)
    without_index_path = str(tmp_path / 'offline' / 'hpo.doc.dump')
    rows_without_index = await _restored_rows(doc_db2, without_index_path)
    await doc_db2.close()

    assert rows_with_index == rows_without_index
    assert rows_with_index[0]['search_text']  # sanity: search_text was actually populated
