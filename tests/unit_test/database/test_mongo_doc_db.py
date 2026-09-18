import pytest

from bioterms.database.doc_db.mongo_doc_db import MongoDocumentDatabase


@pytest.mark.asyncio
async def test_save_terms_empty_batch_is_a_noop_without_a_client():
    database = MongoDocumentDatabase()
    database._client = None

    await database.save_terms([])


def test_legacy_autocomplete_uses_concept_id_as_deterministic_tiebreaker():
    pipeline = MongoDocumentDatabase._build_legacy_auto_complete_pipeline(
        ['heart'], 'heart', limit=10,
    )

    sort = next(stage['$sort'] for stage in pipeline if '$sort' in stage)
    assert sort == {'score': 1, 'labelLength': 1, 'conceptId': 1}
