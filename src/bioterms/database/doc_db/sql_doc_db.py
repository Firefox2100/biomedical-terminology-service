import re
from uuid import UUID
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Optional
from sqlalchemy import Column, ForeignKey, Index, MetaData, String, DateTime, Table, Text, bindparam, case, \
    delete, func, insert, update, literal, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from sqlalchemy.types import JSON

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.model.concept import Concept, ConceptUnion
from bioterms.model.user import UserApiKey, User, UserRepository
from .doc_db import DocumentDatabase


@dataclass(frozen=True)
class _UserTables:
    users: Table
    api_keys: Table


@dataclass(frozen=True)
class _PrefixTables:
    concept: Table
    ngram: Table


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


def _safe_table_suffix(prefix_value: str) -> str:
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
            stmt = insert(self._t.users).values(
                username=user.username,
                password=user.password
            )

            if self._engine.dialect.name == 'postgresql':
                stmt = stmt.on_conflict_do_update(
                    index_elements=[self._t.users.c.username],
                    set_={'password': stmt.excluded.password},
                )
            elif self._engine.dialect.name in ('mysql', 'mariadb'):
                stmt = stmt.on_duplicate_key_update(password=stmt.inserted.password)
            else:
                # SQLite fallback: try insert then update
                try:
                    await conn.execute(stmt)
                except IntegrityError:
                    await conn.execute(
                        update(self._t.users)
                        .where(self._t.users.c.username == user.username)
                        .values(password=user.password)
                    )
            if self._engine.dialect.name in ('postgresql', 'mysql', 'mariadb'):
                await conn.execute(stmt)

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
    """

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
        p = _safe_table_suffix(prefix.value if hasattr(prefix, 'value') else str(prefix))
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
            Column('vector_id', Text, nullable=True),
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

        Index(f'ix_{concept_table_name}_vector_id', concept.c.vector_id)
        Index(f'ix_{ngram_table_name}_ngram', ngram.c.ngram)

        self._tables_cache[p] = _PrefixTables(concept=concept, ngram=ngram)
        return self._tables_cache[p]

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

        await conn.run_sync(
            self._md.create_all,
            tables=[tables.concept, tables.ngram],
            checkfirst=True
        )
        return tables

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
        async with self._engine.begin() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            concept = tables.concept

            idx_name = f'{concept.name}_{field}_index'
            col_expr_sql: str

            if self._is_postgres:
                col_expr_sql = f"(({concept.name}.payload->>'{field}'))"
            elif self._is_mysql:
                col_expr_sql = f"(JSON_UNQUOTE(JSON_EXTRACT({concept.name}.payload, '$.{field}')))"
            elif self._is_sqlite:
                col_expr_sql = f"(json_extract({concept.name}.payload, '$.{field}'))"
            else:
                raise ValueError(f'Unsupported SQL dialect for create_index: {self._engine.dialect.name}')

            if overwrite:
                # Drop if exists
                try:
                    await conn.execute(text(f'DROP INDEX {idx_name}'))
                except Exception:
                    # Some DBs need "DROP INDEX idx ON table"
                    try:
                        await conn.execute(text(f'DROP INDEX {idx_name} ON {concept.name}'))
                    except Exception:
                        pass

            unique_sql = 'UNIQUE ' if unique else ''
            # Some DBs do not support IF NOT EXISTS for indexes uniformly.
            try:
                await conn.execute(text(f'CREATE {unique_sql}INDEX {idx_name} ON {concept.name} {col_expr_sql}'))
            except Exception as e:
                if overwrite:
                    # Last attempt: drop then create
                    try:
                        await conn.execute(text(f'DROP INDEX {idx_name}'))
                    except Exception:
                        try:
                            await conn.execute(text(f'DROP INDEX {idx_name} ON {concept.name}'))
                        except Exception:
                            pass
                    await conn.execute(text(f'CREATE {unique_sql}INDEX {idx_name} ON {concept.name} {col_expr_sql}'))
                else:
                    raise IndexCreationError(f'Failed to create index {idx_name}: {e}') from e

    async def delete_index(self,
                           prefix: ConceptPrefix,
                           field: str
                           ):
        """
        Delete an index on a specified field in the document database.
        :param prefix: The vocabulary prefix to delete the index for.
        :param field: The field to delete the index on.
        """
        async with self._engine.begin() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            idx_name = f"{tables.concept.name}_{field}_index"
            try:
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

            for i in range(0, len(terms), self._batch_size):
                batch = terms[i : i + self._batch_size]

                rows = []
                ngram_rows = []

                for c in batch:
                    payload = c.model_dump(exclude_none=True)

                    st = c.search_text()
                    ngrams = c.n_grams()

                    rows.append(
                        {
                            'concept_id': c.concept_id,
                            'payload': payload,
                            'search_text': st,
                            'label': getattr(c, 'label', None),
                            'vector_id': getattr(c, 'vector_id', None),
                        }
                    )
                    for ng in ngrams:
                        ngram_rows.append({'concept_id': c.concept_id, 'ngram': ng})

                if not rows:
                    continue

                stmt = insert(concept_t).values(rows)

                if no_upsert:
                    pass
                elif self._is_postgres:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[concept_t.c.concept_id],
                        set_={
                            'payload': stmt.excluded.payload,
                            'search_text': stmt.excluded.search_text,
                            'label': stmt.excluded.label,
                            'vector_id': stmt.excluded.vector_id,
                        },
                    )
                elif self._is_mysql:
                    stmt = stmt.on_duplicate_key_update(
                        payload=stmt.inserted.payload,
                        search_text=stmt.inserted.search_text,
                        label=stmt.inserted.label,
                        vector_id=stmt.inserted.vector_id,
                    )
                else:
                    # SQLite fallback: try insert then update
                    # TODO: Optimize with upsert if needed
                    pass

                await conn.execute(stmt)

                concept_ids = [c.concept_id for c in batch]
                await conn.execute(delete(ngram_t).where(ngram_t.c.concept_id.in_(concept_ids)))
                if ngram_rows:
                    await conn.execute(insert(ngram_t), ngram_rows)

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
                payload = dict(row[0])
                yield model_class.model_validate(payload)

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

        async with self._engine.connect() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            stmt = select(tables.concept.c.payload).where(tables.concept.c.concept_id.in_(concept_ids))
            stream = await conn.stream(stmt)
            async for row in stream:
                payload = dict(row[0])
                yield model_class.model_validate(payload)

    async def delete_all_for_label(self,
                                   prefix: ConceptPrefix,
                                   ):
        """
        Delete all documents/records for a given label in the document database.
        :param prefix: The vocabulary prefix to delete documents for.
        """
        async with self._engine.begin() as conn:
            tables = self._tables_for_prefix(prefix)

            await conn.execute(text(f'DROP TABLE IF EXISTS {tables.ngram.name}'))
            await conn.execute(text(f'DROP TABLE IF EXISTS {tables.concept.name}'))
            await self._ensure_tables_exist(conn, prefix)

    async def update_vector_mapping(self,
                                    prefix: ConceptPrefix,
                                    mapping: dict[str, str],
                                    ):
        """
        Update the vector mapping for concepts in the document database.
        :param prefix: The vocabulary prefix to update the vector mapping for.
        :param mapping: A dictionary mapping concept IDs to vector IDs.
        """
        if not mapping:
            return

        async with self._engine.begin() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            concept_t = tables.concept

            # Executemany update is typically fine and portable
            rows = [{'concept_id': cid, 'vector_id': vid} for cid, vid in mapping.items()]
            stmt = (
                concept_t.update()
                .where(concept_t.c.concept_id == bindparam('concept_id'))
                .values(vector_id=bindparam('vector_id'))
            )
            await conn.execute(stmt, rows)

    async def auto_complete_iter(self,
                                 prefix: ConceptPrefix,
                                 query: str,
                                 limit: int = None,
                                 model_class: type[Concept] = Concept,
                                 ) -> AsyncIterator[ConceptUnion]:
        """
        Run an auto-complete search query against the document database and return an async iterator.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The maximum number of results to return. If None, return all matches.
        :param model_class: The Concept subclass to instantiate for results.
        :return: An asynchronous iterator yielding Concept instances matching the auto-complete query.
        """
        clean_query = re.sub(r'[()"\']', '', query.lower())
        n_gram_query = [word for word in clean_query.split() if len(word) > 2]
        score_query = re.sub(r'\s', '', clean_query)

        async with self._engine.connect() as conn:
            tables = await self._ensure_tables_exist(conn, prefix)
            concept_t = tables.concept
            ngram_t = tables.ngram

            if not n_gram_query:
                return

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

            # labelLength: Mongo used 999 if label missing
            if self._is_postgres:
                label_len = func.char_length(concept_t.c.label)
            else:
                label_len = func.length(concept_t.c.label)

            label_length = case(
                (concept_t.c.label.is_(None), literal(999)),
                else_=label_len
            )

            stmt = (
                select(concept_t.c.payload)
                .select_from(concept_t.join(subq, subq.c.concept_id == concept_t.c.concept_id))
                .order_by(score.asc(), label_length.asc(), concept_t.c.concept_id.asc())
            )

            if limit is not None:
                stmt = stmt.limit(limit)

            stream = await conn.stream(stmt)
            async for row in stream:
                payload = dict(row[0])
                yield model_class.model_validate(payload)

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
