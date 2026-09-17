import asyncio
import re
import time
from uuid import UUID, uuid4
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Optional
from sqlalchemy import Column, ForeignKey, Index, MetaData, String, DateTime, Table, Text, and_, \
    case, delete, func, insert, intersect, literal_column, or_, update, literal, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from sqlalchemy.sql.dml import Insert
from sqlalchemy.types import JSON

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.etc.metrics import DOCDB_OP_DURATION, DOCDB_OP_TTFI, DOCDB_OP_ERRORS
from bioterms.model.concept import Concept, ConceptUnion
from bioterms.model.user import UserApiKey, User, UserRepository
from .doc_db import DocumentDatabase, SearchQuery, normalise_search_query


def _build_upsert_stmt(dialect_name: str,
                       table: Table,
                       rows: list[dict],
                       conflict_columns: list[Column],
                       update_columns: list[str],
                       ) -> Insert | None:
    """
    Build a native insert-or-update (upsert) statement for dialects with one recognised here.
    :param dialect_name: The SQLAlchemy engine dialect name (e.g. "postgresql", "mysql", "sqlite").
    :param table: The target table.
    :param rows: The rows to insert or update.
    :param conflict_columns: The columns identifying an existing row (e.g. the primary key).
    :param update_columns: The names of the columns to update when a row already exists.
    :return: The upsert statement, or None if the dialect has no native upsert construct
        recognised here, in which case the caller should fall back to `_manual_upsert_rows`.
    """
    if dialect_name == 'postgresql':
        stmt = pg_insert(table).values(rows)
        return stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_={col: getattr(stmt.excluded, col) for col in update_columns},
        )

    if dialect_name in ('mysql', 'mariadb'):
        stmt = mysql_insert(table).values(rows)
        return stmt.on_duplicate_key_update(
            **{col: getattr(stmt.inserted, col) for col in update_columns}
        )

    if dialect_name == 'sqlite':
        stmt = sqlite_insert(table).values(rows)
        return stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_={col: getattr(stmt.excluded, col) for col in update_columns},
        )

    return None


async def _manual_upsert_rows(conn: AsyncConnection,
                              table: Table,
                              rows: list[dict],
                              conflict_columns: list[Column],
                              update_columns: list[str],
                              ):
    """
    Portable insert-or-update fallback for SQL dialects without a native upsert construct
    recognised by `_build_upsert_stmt` (i.e. anything other than PostgreSQL, MySQL/MariaDB, or
    SQLite). Each row is attempted as a plain insert inside a savepoint; a primary/unique key
    violation rolls back just that savepoint and falls back to an UPDATE of the existing row
    instead. This is slower than the native bulk upsert used for the three dialects above, but
    works with any SQLAlchemy-supported async dialect and keeps the surrounding transaction
    usable even after a conflict.
    :param conn: The connection to execute on, inside an existing transaction.
    :param table: The target table.
    :param rows: The rows to insert or update.
    :param conflict_columns: The columns identifying an existing row (e.g. the primary key).
    :param update_columns: The names of the columns to update when a row already exists.
    """
    conflict_names = [c.name for c in conflict_columns]

    for row in rows:
        try:
            async with conn.begin_nested():
                await conn.execute(insert(table).values(**row))
        except IntegrityError:
            where_clause = and_(*(table.c[name] == row[name] for name in conflict_names))
            await conn.execute(
                table.update().where(where_clause).values(
                    **{col: row[col] for col in update_columns}
                )
            )


def _escape_like_term(term: str) -> str:
    """
    Escape a search term for safe interpolation into an ILIKE '%...%' pattern.
    :param term: The raw search term.
    :return: The term with LIKE metacharacters escaped (to be used with escape='\\\\').
    """
    return term.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')


def _escape_mysql_boolean_term(term: str) -> str:
    """
    Strip MySQL boolean full-text query operators from a user-supplied search term, so it can
    be safely interpolated into a `MATCH ... AGAINST (... IN BOOLEAN MODE)` query string built
    by string concatenation (boolean mode has no separate parameter placeholder for operators).
    :param term: The raw search term.
    :return: The term with boolean-mode operator characters removed.
    """
    return re.sub(r'[+\-><()~*"@]', '', term)


def _fts5_quote(term: str) -> str:
    """
    Quote a search term as an SQLite FTS5 string literal/phrase, so it is matched literally
    rather than parsed as FTS5 query syntax.
    :param term: The raw search term.
    :return: The term wrapped in double quotes, with internal double quotes escaped.
    """
    return '"' + term.replace('"', '""') + '"'


@dataclass(frozen=True)
class _UserTables:
    users: Table
    api_keys: Table


@dataclass(frozen=True)
class _PrefixTables:
    concept: Table
    ngram: Table
    fts: Table


def _build_user_tables(metadata: MetaData,
                       *,
                       schema: Optional[str] = None
                       ) -> _UserTables:
    """
    Build the user-related tables in the given metadata.
    :param metadata: The SQLAlchemy MetaData object to attach the tables to.
    :param schema: Optional schema name.
    :return: A _UserTables instance containing the user and api_keys tables.
    """
    md = metadata
    if schema is not None:
        md.schema = schema

    users = Table(
        'users',
        md,
        Column(
            'username',
            String(255),
            primary_key=True
        ),
        Column(
            'password',
            String(1024),
            nullable=False
        ),
    )

    api_keys = Table(
        'user_api_keys',
        md,
        Column(
            'key_id',
            String(36),
            primary_key=True
        ),
        Column(
            'username',
            String(255),
            ForeignKey(
                'users.username',
                ondelete='CASCADE'
            ),
            nullable=False
        ),
        Column(
            'name',
            String(255),
            nullable=False
        ),
        Column(
            'key_hash',
            String(128),
            nullable=False
        ),
        Column(
            'created_at',
            DateTime(timezone=True),
            nullable=False
        ),
    )

    Index(
        'ix_user_api_keys_username',
        api_keys.c.username
    )
    Index(
        'ux_user_api_keys_key_hash',
        api_keys.c.key_hash,
        unique=True
    )

    return _UserTables(users=users, api_keys=api_keys)


def safe_table_suffix(prefix_value: str) -> str:
    """
    Generate a safe table suffix from the given prefix value.

    Conservative identifier mapping: letters, digits, underscore only.
    :param prefix_value: The prefix value to convert.
    :return: A safe string suitable for use as a table suffix.
    """
    s = re.sub(r'\W+', '_', str(prefix_value).strip())
    if not s:
        raise ValueError('Invalid prefix for table naming.')
    return s.lower()


_FIELD_NAME_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

# JSON payload fields that also have a dedicated physical column on the concept table. Indexing
# these should target the real column (portable, and usable by the query planner for the
# save_terms/auto_complete_iter queries that already filter/sort on it) rather than a
# dialect-specific JSON path expression.
_DEDICATED_INDEX_COLUMNS = {
    'conceptId': 'concept_id',
    'label': 'label',
}


def _validate_field_name(field: str) -> str:
    """
    Validate that a field name is safe to interpolate into raw index-management SQL.
    :param field: The JSON payload field name to validate.
    :return: The field name, unchanged, if valid.
    :raises IndexCreationError: If the field name is not a plain identifier.
    """
    if not _FIELD_NAME_PATTERN.fullmatch(field):
        raise IndexCreationError(f'Invalid field name for index: {field!r}')
    return field


class SqlUserRepository(UserRepository):
    """
    A SQL implementation of the UserRepository interface.
    """

    def __init__(self,
                 engine: AsyncEngine,
                 *,
                 schema: Optional[str] = None
                 ):
        """
        Initialise the SqlUserRepository with SQLAlchemy engine and schema.
        :param engine: The SQLAlchemy AsyncEngine to use for database connections.
        :param schema: Optional schema name.
        """
        self._engine = engine
        self._schema = schema
        self._md = MetaData(schema=schema)
        self._t = _build_user_tables(self._md, schema=schema)

    async def ensure_schema(self):
        """
        Ensure that the user-related tables exist in the database.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(self._md.create_all, checkfirst=True)

    @staticmethod
    def _utc(dt: datetime) -> datetime:
        """
        Ensure the given datetime is timezone-aware in UTC.
        :param dt: The datetime to convert.
        :return: A timezone-aware UTC datetime.
        """
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

    async def _fetch_user_row(self,
                              conn: AsyncConnection,
                              username: str
                              ):
        """
        Fetch the user row for the given username.
        :param conn: The AsyncConnection to use.
        :param username: The username to fetch.
        :return: The user row or None if not found.
        """
        q = select(
            self._t.users.c.username,
            self._t.users.c.password
        ).where(self._t.users.c.username == username)

        res = await conn.execute(q)

        return res.first()

    async def _fetch_api_keys(self,
                              conn: AsyncConnection,
                              username: str
                              ) -> list[UserApiKey]:
        """
        Fetch the API keys for the given username.
        :param conn: The AsyncConnection to use.
        :param username: The username to fetch API keys for.
        :return: A list of API keys.
        """
        q = select(
            self._t.api_keys.c.key_id,
            self._t.api_keys.c.name,
            self._t.api_keys.c.key_hash,
            self._t.api_keys.c.created_at,
        ).where(
            self._t.api_keys.c.username == username
        ).order_by(
            self._t.api_keys.c.created_at.asc(),
            self._t.api_keys.c.key_id.asc()
        )
        res = await conn.execute(q)
        rows = res.fetchall()
        return [
            UserApiKey(
                keyId=r.key_id,
                name=r.name,
                keyHash=r.key_hash,
                createdAt=r.created_at,
            )
            for r in rows
        ]

    async def get(self, username: str) -> User | None:
        """
        Retrieve a user by their username.
        :param username: The username of the user to retrieve.
        :return: User object or None if not found.
        """
        async with self._engine.connect() as conn:
            row = await self._fetch_user_row(conn, username)
            if not row:
                return None

            api_keys = await self._fetch_api_keys(conn, username)

            return User(
                username=row.username,
                password=row.password,
                apiKeys=api_keys
            )

    async def filter(self) -> list[User]:
        """
        Get a list of all User entities.
        :return: A list of User instances.
        """
        async with self._engine.connect() as conn:
            q = select(
                self._t.users.c.username,
                self._t.users.c.password
            ).order_by(
                self._t.users.c.username.asc()
            )
            res = await conn.execute(q)
            rows = res.fetchall()

            users = []
            for r in rows:
                api_keys = await self._fetch_api_keys(conn, r.username)

                users.append(User(
                    username=r.username,
                    password=r.password,
                    apiKeys=api_keys
                ))
            return users

    async def save(self, user: User):
        """
        Save a User entity to the database.
        :param user: An instance of User to be saved.
        """
        async with self._engine.begin() as conn:
            row = {'username': user.username, 'password': user.password}
            upsert_stmt = _build_upsert_stmt(
                self._engine.dialect.name,
                self._t.users,
                [row],
                conflict_columns=[self._t.users.c.username],
                update_columns=['password'],
            )

            if upsert_stmt is not None:
                await conn.execute(upsert_stmt)
            else:
                await _manual_upsert_rows(
                    conn,
                    self._t.users,
                    [row],
                    conflict_columns=[self._t.users.c.username],
                    update_columns=['password'],
                )

            if user.api_keys is not None:
                await conn.execute(delete(self._t.api_keys).where(
                    self._t.api_keys.c.username == user.username
                ))
                if user.api_keys:
                    rows = []
                    for k in user.api_keys:
                        rows.append(
                            {
                                'key_id': str(k.key_id),
                                'username': user.username,
                                'name': k.name,
                                'key_hash': k.key_hash,
                                'created_at': self._utc(k.created_at),
                            }
                        )
                    await conn.execute(insert(self._t.api_keys), rows)

    async def update(self, user: User):
        """
        Update an existing User entity in the database.
        :param user: An instance of User to be updated.
        """
        async with self._engine.begin() as conn:
            await conn.execute(
                update(self._t.users)
                .where(self._t.users.c.username == user.username)
                .values(password=user.password)
            )

            if user.api_keys is not None:
                await conn.execute(delete(self._t.api_keys).where(
                    self._t.api_keys.c.username == user.username
                ))
                if user.api_keys:
                    rows = []
                    for k in user.api_keys:
                        rows.append(
                            {
                                'key_id': str(k.key_id),
                                'username': user.username,
                                'name': k.name,
                                'key_hash': k.key_hash,
                                'created_at': self._utc(k.created_at),
                            }
                        )
                    await conn.execute(insert(self._t.api_keys), rows)

    async def delete(self, username: str):
        """
        Delete a User entity from the database.
        :param username: The username of the user to be deleted.
        """
        async with self._engine.begin() as conn:
            # In case cascade delete is not supported by the database
            await conn.execute(
                delete(self._t.api_keys).where(self._t.api_keys.c.username == username)
            )
            await conn.execute(
                delete(self._t.users).where(self._t.users.c.username == username)
            )

    async def save_api_key(self,
                           username: str,
                           api_key: UserApiKey,
                           ):
        """
        Save an API key for a user.
        :param username: The username of the user to associate the API key with.
        :param api_key: The UserApiKey instance to be saved.
        """
        async with self._engine.begin() as conn:
            await conn.execute(
                insert(self._t.api_keys).values(
                    key_id=str(api_key.key_id),
                    username=username,
                    name=api_key.name,
                    key_hash=api_key.key_hash,
                    created_at=self._utc(api_key.created_at),
                )
            )

    async def delete_api_key(self,
                             username: str,
                             key_id: UUID,
                             ):
        """
        Delete an API key for a user.
        :param username: The username of the user to disassociate the API key from.
        :param key_id: The UUID of the API key to be deleted.
        """
        async with self._engine.begin() as conn:
            await conn.execute(
                delete(self._t.api_keys).where(
                    (self._t.api_keys.c.username == username) &
                    (self._t.api_keys.c.key_id == str(key_id))
                )
            )

    async def get_user_by_api_key(self,
                                  key_hash: str,
                                  ) -> User | None:
        """
        Retrieve a user by their API key hash.
        :param key_hash: The HMAC-SHA-256 hashed value of the API key.
        :return: User object or None if not found.
        """
        async with self._engine.connect() as conn:
            q = select(self._t.api_keys.c.username).where(
                self._t.api_keys.c.key_hash == key_hash
            ).limit(1)
            res = await conn.execute(q)
            row = res.first()
            if not row:
                return None
            return await self.get(row.username)


class SqlDocumentDatabase(DocumentDatabase):
    """
    A SQL implementation of the DocumentDatabase interface.

    Auto-complete substring search is backed by a native trigram/n-gram text index when the
    connected database supports one, falling back to the portable hand-rolled n-gram side
    table (see `Concept.n_grams()`) otherwise:

    - PostgreSQL: a `pg_trgm` GIN index on `search_text` (the `pg_trgm` extension is enabled
      automatically if the connection has privileges to do so).
    - SQLite: an FTS5 virtual table using the built-in `trigram` tokenizer (SQLite >= 3.34.0).
    - MySQL: a `FULLTEXT ... WITH PARSER ngram` index on `search_text` (MySQL's built-in ngram
      full-text parser plugin; not available on MariaDB, which falls back to the n-gram table).

    Capability is probed once per instance (see `_get_native_search_mode`) and cached for the
    instance's lifetime -- it is not re-probed per query.
    """
    _backend_name = 'sql'

    # Sentinel values for `self._native_search_mode`.
    _NATIVE_NONE = 'none'
    _NATIVE_PG_TRGM = 'pg_trgm'
    _NATIVE_SQLITE_TRIGRAM = 'sqlite_trigram'
    _NATIVE_MYSQL_NGRAM = 'mysql_ngram'

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        schema: Optional[str] = None,
        batch_size: int = 5000,
    ):
        self._engine = engine
        self._schema = schema
        self._batch_size = batch_size
        self._md = MetaData(schema=schema)
        self._tables_cache: dict[str, _PrefixTables] = {}

        # Decide JSON type per dialect
        self._is_postgres = self._engine.dialect.name == 'postgresql'
        self._is_mysql = self._engine.dialect.name in ('mysql', 'mariadb')
        self._is_sqlite = self._engine.dialect.name == 'sqlite'

        self._json_type = JSONB if self._is_postgres else JSON

        self._native_search_mode: Optional[str] = None
        self._native_search_lock = asyncio.Lock()
        self._search_index_ready: set[str] = set()

    @property
    def users(self) -> SqlUserRepository:
        """
        Get the user repository for managing admin users in the document database.
        :return: UserRepository instance.
        """
        repo = SqlUserRepository(
            engine=self._engine,
            schema=self._schema,
        )
        return repo

    async def initialize(self):
        """
        Initialise the database driver/connection.
        """
        await self.users.ensure_schema()

    async def close(self):
        """
        Close the SQL database connection.
        """
        await self._engine.dispose()

    def _tables_for_prefix(self,
                           prefix: ConceptPrefix | str,
                           ) -> _PrefixTables:
        """
        Get or create the SQLAlchemy Table objects for the given prefix.
        :param prefix: The concept prefix (ConceptPrefix or str).
        :return: _PrefixTables containing concept and ngram tables.
        """
        p = safe_table_suffix(prefix.value if hasattr(prefix, 'value') else str(prefix))
        if p in self._tables_cache:
            return self._tables_cache[p]

        concept_table_name = f'concept_{p}'
        ngram_table_name = f'concept_{p}_ngram'

        concept = Table(
            concept_table_name,
            self._md,
            Column('concept_id', String(255), primary_key=True),
            Column('payload', self._json_type, nullable=False),
            Column('search_text', Text, nullable=False),
            Column('label', Text, nullable=True),
        )

        ngram = Table(
            ngram_table_name,
            self._md,
            Column(
                'concept_id',
                String(255),
                ForeignKey(
                    f'{concept_table_name}.concept_id',
                    ondelete='CASCADE'),
                primary_key=True
            ),
            Column(
                'ngram',
                String(64),
                primary_key=True
            ),
        )

        # Only used in `_NATIVE_SQLITE_TRIGRAM` mode. Not part of `create_all` (SQLite FTS5
        # virtual tables need the dialect-specific `CREATE VIRTUAL TABLE ... USING fts5(...)`
        # DDL issued by `_ensure_search_index`) -- this Table object exists purely so DML
        # (insert/delete/select) against it can go through the normal query builder.
        fts = Table(
            f'{concept_table_name}_fts',
            self._md,
            Column('concept_id', String(255), primary_key=True),
            Column('search_text', Text, nullable=False),
        )

        Index(f'ix_{ngram_table_name}_ngram', ngram.c.ngram)

        self._tables_cache[p] = _PrefixTables(concept=concept, ngram=ngram, fts=fts)
        return self._tables_cache[p]

    async def _detect_native_search_mode(self,
                                         conn: AsyncConnection,
                                         ) -> str:
        """
        Probe the connected database for a native trigram/n-gram full-text search capability.
        Each probe is a real (throwaway) DDL statement rather than a version/catalog check,
        since the capability can be missing even on a database that generally supports it
        (e.g. PostgreSQL without the `pg_trgm` extension installed, or MySQL/MariaDB without
        the `ngram` full-text parser plugin loaded) -- and can be missing for permission
        reasons even where the feature is technically installed.
        :param conn: An AsyncConnection with an open write transaction.
        :return: One of the `_NATIVE_*` sentinels, or `_NATIVE_NONE` if nothing usable was found.
        """
        if self._is_postgres:
            try:
                # In its own SAVEPOINT: a failed statement aborts the whole surrounding
                # transaction on PostgreSQL, which would otherwise take the fallback SELECT
                # below down with it too (masking whatever it would have found).
                async with conn.begin_nested():
                    await conn.execute(text('CREATE EXTENSION IF NOT EXISTS pg_trgm'))
                return self._NATIVE_PG_TRGM
            except Exception:
                # Might already be enabled by a DBA even though this connection lacks
                # privileges to CREATE EXTENSION itself -- check before giving up on it.
                try:
                    result = await conn.execute(
                        text("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
                    )
                    if result.first() is not None:
                        return self._NATIVE_PG_TRGM
                except Exception:
                    pass
                return self._NATIVE_NONE

        if self._is_sqlite:
            probe = f'_bts_fts5_probe_{uuid4().hex}'
            try:
                await conn.execute(
                    text(f"CREATE VIRTUAL TABLE {probe} USING fts5(x, tokenize='trigram')")
                )
                await conn.execute(text(f'DROP TABLE {probe}'))
                return self._NATIVE_SQLITE_TRIGRAM
            except Exception:
                return self._NATIVE_NONE

        if self._is_mysql:
            probe = f'_bts_ngram_probe_{uuid4().hex}'
            try:
                await conn.execute(
                    text(
                        f'CREATE TEMPORARY TABLE {probe} '
                        f'(x TEXT, FULLTEXT idx_{probe} (x) WITH PARSER ngram) ENGINE=InnoDB'
                    )
                )
                await conn.execute(text(f'DROP TEMPORARY TABLE {probe}'))
                return self._NATIVE_MYSQL_NGRAM
            except Exception:
                return self._NATIVE_NONE

        return self._NATIVE_NONE

    async def _get_native_search_mode(self) -> str:
        """
        Get the native search mode for this database, detecting and caching it on first use.
        :return: One of the `_NATIVE_*` sentinels.
        """
        if self._native_search_mode is not None:
            return self._native_search_mode

        async with self._native_search_lock:
            if self._native_search_mode is not None:
                return self._native_search_mode

            async with self._engine.begin() as conn:
                self._native_search_mode = await self._detect_native_search_mode(conn)

        return self._native_search_mode

    async def _ensure_search_index(self,
                                   conn: AsyncConnection,
                                   tables: _PrefixTables,
                                   mode: str,
                                   suffix: str,
                                   ):
        """
        Ensure the native search index (or, for `_NATIVE_NONE`, nothing -- the n-gram table is
        already handled by `create_all`) exists for a prefix's concept table, doing nothing if
        it was already ensured earlier in this instance's lifetime.
        :param conn: AsyncConnection with an open write transaction.
        :param tables: The _PrefixTables for this prefix.
        :param mode: The native search mode, from `_get_native_search_mode`.
        :param suffix: The safe table-name suffix for this prefix (see `safe_table_suffix`).
        """
        if suffix in self._search_index_ready:
            return

        concept_name = tables.concept.name

        if mode == self._NATIVE_PG_TRGM:
            idx_name = f'ix_{concept_name}_trgm'
            await conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS {idx_name} ON {concept_name} '
                    f'USING GIN (search_text gin_trgm_ops)'
                )
            )
        elif mode == self._NATIVE_SQLITE_TRIGRAM:
            await conn.execute(
                text(
                    f"CREATE VIRTUAL TABLE IF NOT EXISTS {tables.fts.name} USING fts5("
                    f"concept_id UNINDEXED, search_text, tokenize='trigram')"
                )
            )
        elif mode == self._NATIVE_MYSQL_NGRAM:
            idx_name = f'ftx_{concept_name}_ngram'
            exists_result = await conn.execute(
                text(
                    'SELECT 1 FROM information_schema.statistics '
                    'WHERE table_schema = DATABASE() AND table_name = :t AND index_name = :i'
                ),
                {'t': concept_name, 'i': idx_name},
            )
            if exists_result.first() is None:
                await conn.execute(
                    text(
                        f'ALTER TABLE {concept_name} ADD FULLTEXT INDEX {idx_name} '
                        f'(search_text) WITH PARSER ngram'
                    )
                )

        self._search_index_ready.add(suffix)

    async def _ensure_tables_exist(self,
                                   conn: AsyncConnection,
                                   prefix: ConceptPrefix | str,
                                   ) -> _PrefixTables:
        """
        Ensure that the tables for the given prefix exist in the database.
        :param conn: AsyncConnection
        :param prefix: The concept prefix.
        :return: A _PrefixTables instance.
        """
        tables = self._tables_for_prefix(prefix)
        mode = await self._get_native_search_mode()

        create_tables = [tables.concept]
        if mode == self._NATIVE_NONE:
            create_tables.append(tables.ngram)

        await conn.run_sync(
            self._md.create_all,
            tables=create_tables,
            checkfirst=True
        )

        suffix = safe_table_suffix(prefix.value if hasattr(prefix, 'value') else str(prefix))
        await self._ensure_search_index(conn, tables, mode, suffix)

        return tables

    def _index_target_sql(self,
                          field: str,
                          ) -> str:
        """
        Build the SQL index target for a JSON payload field: either the dedicated physical
        column backing it (see `_DEDICATED_INDEX_COLUMNS`), or a dialect-specific JSON path
        expression as a fallback. The returned string is the content of the index's column
        list, i.e. it still needs to be wrapped in `(...)` by the caller.

        The "payload" column reference below is deliberately NOT table-qualified: expression
        indexes are implicitly scoped to the single table in the surrounding `CREATE INDEX ...
        ON table (<expr>)`, and PostgreSQL/MySQL/SQLite all reject (or, for SQLite, error
        outright on) a table-qualified column reference inside an index expression.
        :param field: The JSON field to index.
        :return: The SQL expression to place inside `CREATE INDEX ... (<expr>)`.
        """
        dedicated_column = _DEDICATED_INDEX_COLUMNS.get(field)
        if dedicated_column is not None:
            return dedicated_column

        if self._is_postgres:
            return f"(payload->>'{field}')"
        if self._is_mysql:
            # MySQL requires functional key parts to be doubly parenthesised, i.e.
            # `CREATE INDEX ix ON t ((JSON_EXTRACT(...)))`; the caller adds the outer layer.
            return f"(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.{field}')))"
        if self._is_sqlite:
            return f"(json_extract(payload, '$.{field}'))"

        raise IndexCreationError(f'Unsupported SQL dialect for create_index: {self._engine.dialect.name}')

    @staticmethod
    def _row_to_payload(row) -> dict:
        """
        Reconstruct a concept payload dict from a result row selecting `payload`.
        :param row: A result row with a `payload` column.
        :return: The payload dict.
        """
        return dict(row.payload)

    async def _index_exists(self,
                            conn,
                            idx_name: str,
                            table_name: str,
                            ) -> bool:
        """
        Check whether an index with the given name already exists, without raising if it does
        not. Used to make `create_index` idempotent -- a plain `CREATE INDEX` has no portable
        `IF NOT EXISTS` across every dialect this driver supports (see the comment in
        `create_index`), so a duplicate-name failure is disambiguated afterwards from a real
        failure by checking the catalog directly instead.
        :param conn: The database connection to execute on.
        :param idx_name: The name of the index to check for.
        :param table_name: The name of the table the index belongs to.
        :return: True if the index already exists, False otherwise (including on dialects this
            doesn't know how to check, so callers should treat False as "inconclusive").
        """
        try:
            if self._is_postgres:
                result = await conn.execute(
                    text('SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = :name'),
                    {'name': idx_name},
                )
            elif self._is_mysql:
                result = await conn.execute(
                    text(
                        'SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE() '
                        'AND table_name = :table AND index_name = :name'
                    ),
                    {'table': table_name, 'name': idx_name},
                )
            elif self._is_sqlite:
                result = await conn.execute(
                    text("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = :name"),
                    {'name': idx_name},
                )
            else:
                return False
        except Exception:
            return False

        return result.first() is not None

    @staticmethod
    async def _drop_index_if_exists(conn,
                                    idx_name: str,
                                    table_name: str,
                                    ):
        """
        Best-effort drop of an index, trying both the standalone and table-qualified DROP INDEX
        syntax since dialects differ on which is required.

        Each attempt runs inside its own SAVEPOINT (`conn.begin_nested()`): on PostgreSQL, a
        failed statement -- including a routine "no such index" from an index that was never
        there in the first place, the expected case here -- aborts the whole surrounding
        transaction, not just that statement, which would otherwise take down the CREATE INDEX
        this is normally called right before. The savepoint contains the failure to itself.
        :param conn: The database connection to execute on.
        :param idx_name: The name of the index to drop.
        :param table_name: The name of the table the index belongs to.
        """
        try:
            async with conn.begin_nested():
                await conn.execute(text(f'DROP INDEX {idx_name}'))
        except Exception:
            # Some DBs need "DROP INDEX idx ON table"
            try:
                async with conn.begin_nested():
                    await conn.execute(text(f'DROP INDEX {idx_name} ON {table_name}'))
            except Exception:
                pass

    async def create_index(self,
                           prefix: ConceptPrefix,
                           field: str,
                           unique: bool = False,
                           overwrite: bool = False,
                           ):
        """
        Create an index on a specified field within the JSON payload of concepts.
        :param prefix: The concept prefix.
        :param field: The JSON field to index.
        :param unique: Whether the index should enforce uniqueness.
        :param overwrite: Whether to overwrite an existing index.
        :raises IndexCreationError: If index creation fails.
        """
        _validate_field_name(field)

        if _DEDICATED_INDEX_COLUMNS.get(field) == 'concept_id':
            # concept_id is the table's primary key; it is already indexed/unique.
            return

        async with self._engine.begin() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            concept = tables.concept

            idx_name = f'{concept.name}_{field}_index'
            target_sql = self._index_target_sql(field)

            if overwrite:
                await self._drop_index_if_exists(conn, idx_name, concept.name)

            unique_sql = 'UNIQUE ' if unique else ''
            create_stmt = text(f'CREATE {unique_sql}INDEX {idx_name} ON {concept.name} ({target_sql})')

            # Some DBs do not support IF NOT EXISTS for indexes uniformly, hence the plain
            # CREATE + catch below rather than relying on that -- wrapped in its own SAVEPOINT
            # since on PostgreSQL a failed statement aborts the whole surrounding transaction,
            # which would otherwise take down the _index_exists check right after it too.
            try:
                async with conn.begin_nested():
                    await conn.execute(create_stmt)
            except Exception as e:
                if not overwrite:
                    # create_index() is meant to be idempotent -- called on every
                    # load/restore, same as the Mongo driver's create_index(), where
                    # re-creating an already-identical index is a silent no-op. A duplicate
                    # index name is not a real error here; anything else still is.
                    if await self._index_exists(conn, idx_name, concept.name):
                        return
                    raise IndexCreationError(f'Failed to create index {idx_name}: {e}') from e

                # Last attempt: drop then create
                await self._drop_index_if_exists(conn, idx_name, concept.name)
                try:
                    async with conn.begin_nested():
                        await conn.execute(create_stmt)
                except Exception as e2:
                    raise IndexCreationError(f'Failed to create index {idx_name}: {e2}') from e2

    async def delete_index(self,
                           prefix: ConceptPrefix,
                           field: str
                           ):
        """
        Delete an index on a specified field in the document database.
        :param prefix: The vocabulary prefix to delete the index for.
        :param field: The field to delete the index on.
        """
        _validate_field_name(field)

        if _DEDICATED_INDEX_COLUMNS.get(field) == 'concept_id':
            # No standalone index was created for concept_id; nothing to delete.
            return

        async with self._engine.begin() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            idx_name = f"{tables.concept.name}_{field}_index"
            try:
                # Own SAVEPOINT: a failed statement aborts the whole surrounding transaction on
                # PostgreSQL, which would otherwise take the "ON table" fallback below with it.
                async with conn.begin_nested():
                    await conn.execute(text(f"DROP INDEX {idx_name}"))
            except Exception:
                await conn.execute(text(f"DROP INDEX {idx_name} ON {tables.concept.name}"))

    async def save_terms(self,
                         terms: list[Concept],
                         no_upsert: bool = False,
                         ):
        """
        Save a list of terms into the document database.
        :param terms: A list of Concept instances to save.
        :param no_upsert: Force direct insert. The caller must ensure that there is no existing data that
            may be a duplicate, or it will fail from the unique index
        """
        if not terms:
            return
        prefix = terms[0].prefix

        async with self._engine.begin() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            concept_t = tables.concept
            ngram_t = tables.ngram
            fts_t = tables.fts
            mode = await self._get_native_search_mode()

            for i in range(0, len(terms), self._batch_size):
                batch = terms[i : i + self._batch_size]

                rows = []
                ngram_rows = []
                fts_rows = []

                for c in batch:
                    payload = c.model_dump(exclude_none=True)

                    st = c.search_text()

                    rows.append(
                        {
                            'concept_id': c.concept_id,
                            'payload': payload,
                            'search_text': st,
                            'label': getattr(c, 'label', None),
                        }
                    )

                    if mode == self._NATIVE_NONE:
                        # No native trigram/n-gram search available: keep populating the
                        # portable n-gram side table used by the fallback query path.
                        for ng in c.n_grams():
                            ngram_rows.append({'concept_id': c.concept_id, 'ngram': ng})
                    elif mode == self._NATIVE_SQLITE_TRIGRAM:
                        # The FTS5 shadow table isn't kept in sync automatically (it isn't
                        # declared as an "external content" table over `concept_t`), so it is
                        # mirrored by hand alongside the concept row itself.
                        fts_rows.append({'concept_id': c.concept_id, 'search_text': st})
                    # NATIVE_PG_TRGM / NATIVE_MYSQL_NGRAM index `concept_t.search_text`
                    # directly -- no extra row needed beyond the concept upsert below.

                if not rows:
                    continue

                if no_upsert:
                    await conn.execute(insert(concept_t).values(rows))
                else:
                    update_columns = ['payload', 'search_text', 'label']
                    upsert_stmt = _build_upsert_stmt(
                        self._engine.dialect.name,
                        concept_t,
                        rows,
                        conflict_columns=[concept_t.c.concept_id],
                        update_columns=update_columns,
                    )

                    if upsert_stmt is not None:
                        await conn.execute(upsert_stmt)
                    else:
                        await _manual_upsert_rows(
                            conn,
                            concept_t,
                            rows,
                            conflict_columns=[concept_t.c.concept_id],
                            update_columns=update_columns,
                        )

                concept_ids = [c.concept_id for c in batch]

                if mode == self._NATIVE_NONE:
                    await conn.execute(delete(ngram_t).where(ngram_t.c.concept_id.in_(concept_ids)))
                    if ngram_rows:
                        await conn.execute(insert(ngram_t), ngram_rows)
                elif mode == self._NATIVE_SQLITE_TRIGRAM:
                    await conn.execute(delete(fts_t).where(fts_t.c.concept_id.in_(concept_ids)))
                    if fts_rows:
                        await conn.execute(insert(fts_t), fts_rows)

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of terms for a given prefix in the document database.
        :param prefix: The vocabulary prefix to count documents for.
        :return: The number of terms/documents
        """
        async with self._engine.connect() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            stmt = select(func.count()).select_from(tables.concept)
            result = await conn.execute(stmt)
            return int(result.scalar_one())

    async def get_terms_iter(self,
                             prefix: ConceptPrefix,
                             limit: int = 0,
                             model_class: type[Concept] = Concept,
                             ) -> AsyncIterator[ConceptUnion]:
        """
        Get an asynchronous iterator over all items for a given prefix in the document database.
        :param prefix: The vocabulary prefix to get documents for.
        :param limit: The maximum number of documents to retrieve. If 0, retrieve all documents.
        :param model_class: The Concept subclass to instantiate for results.
        :return: An asynchronous iterator yielding Concept instances.
        """
        async with self._engine.connect() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            stmt = select(tables.concept.c.payload)
            if limit and limit > 0:
                stmt = stmt.limit(limit)

            stream = await conn.stream(stmt)
            async for row in stream:
                yield model_class.model_validate(self._row_to_payload(row))

    async def get_terms_by_ids_iter(self,
                                    prefix: ConceptPrefix,
                                    concept_ids: list[str],
                                    model_class: type[Concept] = Concept,
                                    ) -> AsyncIterator[ConceptUnion]:
        """
        Get terms by their IDs for a given prefix in the document database as an async iterator.
        :param prefix: The vocabulary prefix to get documents for.
        :param concept_ids: A list of concept IDs to retrieve.
        :param model_class: The Concept subclass to instantiate for results.
        :return: An asynchronous iterator yielding Concept instances.
        """
        if not concept_ids:
            return

        start = time.perf_counter()
        first_item_at = None
        result_label = 'ok'

        try:
            async with self._engine.connect() as conn:
                tables = await self._ensure_tables_exist(conn, prefix)
                stmt = select(
                    tables.concept.c.payload
                ).where(tables.concept.c.concept_id.in_(concept_ids))
                stream = await conn.stream(stmt)
                async for row in stream:
                    if first_item_at is None:
                        first_item_at = time.perf_counter()
                    yield model_class.model_validate(self._row_to_payload(row))
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            DOCDB_OP_ERRORS.labels(
                backend='sql',
                op='get_terms_by_ids',
                prefix=prefix.value,
                error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            DOCDB_OP_DURATION.labels(
                backend='sql',
                op='get_terms_by_ids',
                prefix=prefix.value,
                result=result_label,
            ).observe(end - start)

            if first_item_at is not None:
                DOCDB_OP_TTFI.labels(
                    backend='sql',
                    op='get_terms_by_ids',
                    prefix=prefix.value,
                    result=result_label,
                ).observe(first_item_at - start)

    async def delete_all_for_label(self,
                                   prefix: ConceptPrefix,
                                   ):
        """
        Delete all documents/records for a given label in the document database.
        :param prefix: The vocabulary prefix to delete documents for.
        """
        async with self._engine.begin() as conn:
            tables = self._tables_for_prefix(prefix)

            # The FTS5 shadow table isn't managed by `create_all`/metadata, so it needs an
            # explicit drop here; PostgreSQL's trgm GIN index and MySQL's ngram FULLTEXT index
            # both live on `concept_t` itself and are dropped along with it.
            await conn.execute(text(f'DROP TABLE IF EXISTS {tables.fts.name}'))
            await conn.execute(text(f'DROP TABLE IF EXISTS {tables.ngram.name}'))
            await conn.execute(text(f'DROP TABLE IF EXISTS {tables.concept.name}'))

            # The concept table (and, with it, any native search index) was just dropped, so
            # the "already ensured" cache entry for this prefix is stale -- clear it before
            # `_ensure_tables_exist` recreates the tables, or the search index would silently
            # never come back.
            suffix = safe_table_suffix(prefix.value if hasattr(prefix, 'value') else str(prefix))
            self._search_index_ready.discard(suffix)

            await self._ensure_tables_exist(conn, prefix)

    def _label_length_expr(self,
                           tables: _PrefixTables,
                           ):
        """
        Build the "shorter label first" tie-break expression shared by every auto-complete
        query mode: NULL labels sort last, matching Mongo's historical `999` sentinel.
        :param tables: The _PrefixTables for this prefix.
        :return: A SQLAlchemy expression giving the label's character length, or 999 if absent.
        """
        concept_t = tables.concept
        label_len = func.char_length(concept_t.c.label) if self._is_postgres else func.length(concept_t.c.label)
        return case(
            (concept_t.c.label.is_(None), literal(999)),
            else_=label_len
        )

    def _build_auto_complete_stmt(self,
                                  tables: _PrefixTables,
                                  mode: str,
                                  n_gram_query: list[str],
                                  score_query: str,
                                  limit: int | None,
                                  ):
        """
        Build the auto-complete SELECT statement appropriate for the given native search mode.
        Every mode requires every word in `n_gram_query` to occur as a substring somewhere in
        the concept's `search_text`; modes differ in how relevance ("more relevant first") is
        approximated on top of that, since the underlying engines don't expose comparable
        scoring -- see the driver docstring and `docs/source/build-database.rst` for the exact
        differences. All modes order ties by shorter label first, then concept ID, for
        determinism.
        :param tables: The _PrefixTables for this prefix.
        :param mode: The native search mode, from `_get_native_search_mode`.
        :param n_gram_query: The lowercased, whitespace-split query words (each len > 2).
        :param score_query: The whitespace-stripped lowercased full query, used for position
            scoring in the fallback and PostgreSQL modes.
        :param limit: The maximum number of rows to return, or None for no limit.
        :return: A SQLAlchemy Select statement yielding a `payload` column.
        """
        concept_t = tables.concept
        label_length = self._label_length_expr(tables)

        if mode == self._NATIVE_PG_TRGM:
            conditions = [
                concept_t.c.search_text.ilike(f'%{_escape_like_term(w)}%', escape='\\')
                for w in n_gram_query
            ]
            # The `pg_trgm` GIN index accelerates the ILIKE substring filtering above (the
            # expensive part at this dataset's scale); ranking reuses the same position-based
            # score as the fallback path for consistent "more relevant first" behaviour, rather
            # than trigram `similarity()` (which scores the whole search_text's trigram overlap
            # and does not reliably prefer an earlier/more exact match of the query itself).
            pos = func.strpos(concept_t.c.search_text, score_query)
            score = case(
                (pos == 0, literal(10 ** 9)),
                else_=pos - 1
            )
            stmt = (
                select(concept_t.c.payload)
                .where(and_(*conditions))
                .order_by(score.asc(), label_length.asc(), concept_t.c.concept_id.asc())
            )

        elif mode == self._NATIVE_MYSQL_NGRAM:
            boolean_query = ' '.join(
                f'+{_escape_mysql_boolean_term(w)}' for w in n_gram_query if _escape_mysql_boolean_term(w)
            )
            match_expr = 'MATCH(search_text) AGAINST (:bts_bq IN BOOLEAN MODE)'
            where_clause = text(match_expr).bindparams(bts_bq=boolean_query)
            order_clause = text(f'{match_expr} DESC').bindparams(bts_bq=boolean_query)
            stmt = (
                select(concept_t.c.payload)
                .select_from(concept_t)
                .where(where_clause)
                .order_by(order_clause, label_length.asc(), concept_t.c.concept_id.asc())
            )

        elif mode == self._NATIVE_SQLITE_TRIGRAM:
            fts_t = tables.fts
            word_subqueries = [
                select(fts_t.c.concept_id).where(fts_t.c.search_text.op('MATCH')(_fts5_quote(w)))
                for w in n_gram_query
            ]
            matched = word_subqueries[0] if len(word_subqueries) == 1 else intersect(*word_subqueries)
            subq = matched.subquery()

            # FTS5 doesn't expose a relevance score meaningful across an INTERSECT of
            # independent trigram matches, so relevance is approximated the same way as the
            # fallback path: how early the full (whitespace-stripped) query appears in
            # `search_text`. This is a plain scalar function over the already-small matched
            # set, not a full scan -- the trigram index has already done the heavy filtering.
            pos = func.instr(concept_t.c.search_text, score_query)
            score = case(
                (pos == 0, literal(10 ** 9)),
                else_=pos - 1
            )

            stmt = (
                select(concept_t.c.payload)
                .select_from(concept_t.join(subq, subq.c.concept_id == concept_t.c.concept_id))
                .order_by(score.asc(), label_length.asc(), concept_t.c.concept_id.asc())
            )

        else:
            ngram_t = tables.ngram
            subq = (
                select(ngram_t.c.concept_id)
                .where(ngram_t.c.ngram.in_(n_gram_query))
                .group_by(ngram_t.c.concept_id)
                .having(func.count(func.distinct(ngram_t.c.ngram)) == literal(len(n_gram_query)))
                .subquery()
            )

            if self._is_postgres:
                pos = func.strpos(concept_t.c.search_text, score_query)
            elif self._is_mysql:
                pos = func.locate(score_query, concept_t.c.search_text)
            else:
                pos = func.instr(concept_t.c.search_text, score_query)

            score = case(
                (pos == 0, literal(10 ** 9)),
                else_=pos - 1  # convert to 0-based like Mongo, best-effort
            )

            stmt = (
                select(concept_t.c.payload)
                .select_from(concept_t.join(subq, subq.c.concept_id == concept_t.c.concept_id))
                .order_by(score.asc(), label_length.asc(), concept_t.c.concept_id.asc())
            )

        if limit is not None:
            stmt = stmt.limit(limit)

        return stmt

    def _build_lexical_search_stmt(self,
                                   tables: _PrefixTables,
                                   mode: str,
                                   n_gram_query: list[str],
                                   score_query: str,
                                   limit: int,
                                   ):
        """
        Build the lexical-recall SELECT statement appropriate for the given native search
        mode, yielding `(concept_id, score)` rows ranked best-first. Unlike
        `_build_auto_complete_stmt`, only one query word needs to match (OR, not AND) --
        this is a recall arm for RRF fusion in `bioterms.search.hybrid`, not a precise
        autocomplete match, so it favours recall over precision and leaves ranking to each
        mode's real relevance function where one exists.
        :param tables: The _PrefixTables for this prefix.
        :param mode: The native search mode, from `_get_native_search_mode`.
        :param n_gram_query: The lowercased, whitespace-split query words (each len > 2).
        :param score_query: The whitespace-stripped lowercased full query.
        :param limit: The maximum number of rows to return.
        :return: A SQLAlchemy Select statement yielding `concept_id`/`score` columns.
        """
        concept_t = tables.concept

        if mode == self._NATIVE_PG_TRGM:
            conditions = [
                concept_t.c.search_text.ilike(f'%{_escape_like_term(w)}%', escape='\\')
                for w in n_gram_query
            ]
            score = func.similarity(concept_t.c.search_text, score_query)
            stmt = (
                select(concept_t.c.concept_id, score.label('score'))
                .where(or_(*conditions))
                .order_by(score.desc())
            )

        elif mode == self._NATIVE_MYSQL_NGRAM:
            # Natural language mode (unlike the boolean mode auto-complete uses) returns a
            # real relevance score and matches on any of the words, which is what a recall
            # arm wants.
            match_expr = 'MATCH(search_text) AGAINST (:bts_nl)'
            score = text(f'{match_expr} AS score').bindparams(bts_nl=score_query)
            where_clause = text(match_expr).bindparams(bts_nl=score_query)
            stmt = (
                select(concept_t.c.concept_id, score)
                .select_from(concept_t)
                .where(where_clause)
                .order_by(text('score DESC'))
            )

        elif mode == self._NATIVE_SQLITE_TRIGRAM:
            fts_t = tables.fts
            match_query = ' OR '.join(_fts5_quote(w) for w in n_gram_query)
            subq = (
                select(
                    fts_t.c.concept_id,
                    literal_column(f'bm25({fts_t.name})').label('bm25_score'),
                )
                .where(fts_t.c.search_text.op('MATCH')(match_query))
                .subquery()
            )
            stmt = (
                select(concept_t.c.concept_id, subq.c.bm25_score.label('score'))
                .select_from(concept_t.join(subq, subq.c.concept_id == concept_t.c.concept_id))
                # SQLite's bm25() is lower-is-better, unlike every other mode here.
                .order_by(subq.c.bm25_score.asc())
            )

        else:
            ngram_t = tables.ngram
            subq = (
                select(ngram_t.c.concept_id, func.count(func.distinct(ngram_t.c.ngram)).label('score'))
                .where(ngram_t.c.ngram.in_(n_gram_query))
                .group_by(ngram_t.c.concept_id)
                .subquery()
            )
            stmt = (
                select(concept_t.c.concept_id, subq.c.score)
                .select_from(concept_t.join(subq, subq.c.concept_id == concept_t.c.concept_id))
                .order_by(subq.c.score.desc())
            )

        return stmt.limit(limit)

    async def lexical_search_iter(self,
                                  prefix: ConceptPrefix,
                                  query: str,
                                  limit: int = 10,
                                  ) -> AsyncIterator[tuple[str, float]]:
        """
        Run a scored lexical/keyword search against a vocabulary's concept_id/label/synonyms,
        and return matching concept IDs ranked best-first.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The top number of concepts to return.
        :return: An async iterator of (concept_id, score) tuples, best match first.
        """
        search_query = normalise_search_query(query)
        n_gram_query = search_query.words

        if not n_gram_query:
            return

        async with self._engine.connect() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            mode = await self._get_native_search_mode()

            stmt = self._build_lexical_search_stmt(
                tables=tables,
                mode=mode,
                n_gram_query=n_gram_query,
                score_query=search_query.compact,
                limit=limit,
            )

            stream = await conn.stream(stmt)
            async for row in stream:
                score = float(row.score) if row.score is not None else 0.0
                if mode == self._NATIVE_SQLITE_TRIGRAM:
                    # Undo bm25()'s lower-is-better convention so "higher score = better
                    # match" holds uniformly across every backend.
                    score = -score
                yield row.concept_id, score

    async def _auto_complete_iter(self,
                                  prefix: ConceptPrefix,
                                  search_query: SearchQuery,
                                  limit: int,
                                  model_class: type[Concept],
                                  ) -> AsyncIterator[ConceptUnion]:
        async with self._engine.connect() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            mode = await self._get_native_search_mode()

            stmt = self._build_auto_complete_stmt(
                tables=tables,
                mode=mode,
                n_gram_query=search_query.words,
                score_query=search_query.compact,
                limit=limit,
            )

            stream = await conn.stream(stmt)
            async for row in stream:
                yield model_class.model_validate(self._row_to_payload(row))

    async def get_random_term_ids(self,
                                  prefix: ConceptPrefix,
                                  count: int,
                                  ) -> list[str]:
        """
        Get a list of random term IDs for a given prefix from the document database.
        :param prefix: The vocabulary prefix to get random term IDs for.
        :param count: The number of random term IDs to retrieve.
        :return: A list of random term IDs.
        """
        async with self._engine.connect() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            concept_t = tables.concept

            if self._is_postgres:
                order_func = func.random()
            elif self._is_mysql:
                order_func = func.rand()
            else:
                order_func = func.random()

            stmt = (
                select(concept_t.c.concept_id)
                .order_by(order_func)
                .limit(count)
            )

            res = await conn.execute(stmt)
            rows = res.fetchall()
            return [r.concept_id for r in rows]
