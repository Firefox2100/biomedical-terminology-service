"""
Regression test for a bug where `get_active_doc_db()` constructed the SQL driver but never
called its `initialize()` -- the one call that runs `SqlUserRepository.ensure_schema()`
(`CREATE TABLE IF NOT EXISTS users/api_keys`) -- so `bioterms-cli user create` failed with
`UndefinedTableError: relation "users" does not exist` on a freshly provisioned database that
had never otherwise gone through the SQL doc DB's concept-table creation path.
"""
import pytest

from bioterms.database.doc_db import doc_db as doc_db_module
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import DocDatabaseDriverType
from bioterms.model.user import User


@pytest.mark.asyncio
async def test_get_active_doc_db_initializes_sql_driver_so_user_table_exists(monkeypatch):
    monkeypatch.setattr(doc_db_module, '_active_doc_db', None)
    monkeypatch.setattr(CONFIG, 'doc_database_driver', DocDatabaseDriverType.SQL)
    monkeypatch.setattr(CONFIG, 'sql_db_url', 'sqlite+aiosqlite:///:memory:')

    db = await doc_db_module.get_active_doc_db()
    try:
        # Would raise (no such table) if get_active_doc_db had not called db.initialize().
        await db.users.save(User(username='patrick', password='hashed'))
        assert (await db.users.get('patrick')).username == 'patrick'
    finally:
        await db.close()
