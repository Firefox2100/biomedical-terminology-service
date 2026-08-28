"""
PostgreSQL/pgvector implementation of the VectorDatabase interface.
"""
from typing import AsyncIterator
from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, MetaData, String, Table, bindparam, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection

from bioterms.database.doc_db.sql_doc_db import safe_table_suffix
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import Concept
from bioterms.embedding import ConceptTransformer, TextTransformer
from .vector_db import VectorDatabase


class PostgresVectorDatabase(VectorDatabase):
    """
    PostgreSQL implementation of the VectorDatabase interface, using the pgvector extension for
    storage and HNSW/cosine-distance similarity search.

    Two modes, selected by `shared_with_doc_db` (see `get_active_vector_db()`, which sets this
    based on whether `BTS_SQL_DB_URL` and `BTS_POSTGRES_VECTOR_DB_URL` are the same PostgreSQL
    database):

    - Shared (True): the SQL document database driver (`SqlDocumentDatabase`) is also pointed at
      this same database. Vectors are stored as an extra "vector" column added to the same
      `concept_<prefix>` tables it already maintains, matched on the same "concept_id" primary
      key - one PostgreSQL instance, one set of tables, no separate vector-only store. Since
      those rows are expected to already exist (concepts are always loaded before they are
      embedded, see `vocabulary.embed_vocabulary`), writes here are plain UPDATEs rather than
      upserts, and the table itself is never created or dropped by this driver.
    - Standalone (False): vectors are stored in their own dedicated `concept_<prefix>_vector`
      tables ("concept_id", "vector"), independent of whatever document database is actually in
      use - analogous to the shadow documents `MongoVectorDatabase` maintains when the document
      database isn't MongoDB.
    """

    _engine: AsyncEngine | None = None

    def __init__(self,
                 engine: AsyncEngine | None = None,
                 embedding_dimension: int = 768,
                 shared_with_doc_db: bool = False,
                 ):
        """
        Initialise the PostgreSQL vector database.
        :param engine: Optional AsyncEngine instance or None to use the class variable.
        :param embedding_dimension: Dimension of the embedding vectors, defaults to 768 (for BGE)
        :param shared_with_doc_db: Whether to store vectors on the SQL document database's own
            concept tables instead of separate vector-only tables.
        """
        if engine is not None:
            self._engine = engine

        self._embedding_dimension = embedding_dimension
        self._shared_with_doc_db = shared_with_doc_db
        self._md = MetaData()
        self._tables_cache: dict[str, Table] = {}

    @property
    def engine(self) -> AsyncEngine:
        """
        Return the SQLAlchemy async engine instance.
        :return: The AsyncEngine
        """
        if self._engine is None:
            raise ValueError(
                'PostgreSQL engine is not set. Please set it using set_engine method or pass it '
                'during initialization.'
            )

        return self._engine

    @classmethod
    def set_engine(cls, engine: AsyncEngine):
        """
        Set the SQLAlchemy async engine for the class.
        :param engine: The AsyncEngine instance
        """
        cls._engine = engine

    async def close(self):
        """
        Dispose of the SQLAlchemy engine.
        """
        if self._engine is not None:
            await self._engine.dispose()

    def _table_name(self,
                    prefix: ConceptPrefix,
                    ) -> str:
        """
        Determine the name of the table backing a vocabulary prefix's vectors.
        :param prefix: The vocabulary prefix.
        :return: The table name.
        """
        suffix = safe_table_suffix(prefix.value)
        if self._shared_with_doc_db:
            return f'concept_{suffix}'
        return f'concept_{suffix}_vector'

    def _table_for_prefix(self,
                          prefix: ConceptPrefix,
                          ) -> Table:
        """
        Get or create the (partial, DML-only) SQLAlchemy Table object for a vocabulary prefix.

        In shared mode this deliberately only declares "concept_id" and "vector" even though the
        real table (owned by SqlDocumentDatabase) has more columns - SQLAlchemy Core only needs
        to know about the columns actually referenced in the statements built against it.
        :param prefix: The vocabulary prefix.
        :return: The Table object.
        """
        name = self._table_name(prefix)
        if name in self._tables_cache:
            return self._tables_cache[name]

        table = Table(
            name,
            self._md,
            Column('concept_id', String(255), primary_key=True),
            Column('vector', Vector(self._embedding_dimension)),
            extend_existing=True,
        )

        self._tables_cache[name] = table
        return table

    async def _table_exists(self,
                            conn: AsyncConnection,
                            table_name: str,
                            ) -> bool:
        """
        Check whether a table exists, without raising if it does not.
        :param conn: The connection to check on.
        :param table_name: The name of the table to check for.
        :return: True if the table exists, False otherwise.
        """
        result = await conn.execute(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': table_name})
        return bool(result.scalar())

    @staticmethod
    async def _vector_column_exists(conn: AsyncConnection,
                                    table_name: str,
                                    ) -> bool:
        """
        Check whether a table's "vector" column exists, without raising if it does not.

        In shared mode the table itself (owned by `SqlDocumentDatabase`) exists as soon as the
        vocabulary is loaded, well before the "vector" column is added by
        `_ensure_table_and_index` on first embedding write -- so a table existing is not enough
        to assume the column does too (e.g. a vocabulary that is loaded but not yet embedded).
        :param conn: The connection to check on.
        :param table_name: The name of the table to check.
        :return: True if the column exists, False otherwise.
        """
        result = await conn.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = :t AND column_name = 'vector'"
            ),
            {'t': table_name},
        )
        return result.first() is not None

    async def _ensure_table_and_index(self,
                                      prefix: ConceptPrefix,
                                      ) -> Table:
        """
        Ensure the table and HNSW similarity index backing a vocabulary prefix's vectors exist,
        creating them if necessary. In shared mode, only the "vector" column and index are
        added - the table itself is owned and created by SqlDocumentDatabase, and is expected to
        already exist (i.e. the vocabulary must already be loaded).
        :param prefix: The vocabulary prefix.
        :return: The Table object for the prefix.
        """
        table = self._table_for_prefix(prefix)

        async with self.engine.begin() as conn:
            await conn.execute(text('CREATE EXTENSION IF NOT EXISTS vector'))

            if self._shared_with_doc_db:
                await conn.execute(text(
                    f'ALTER TABLE {table.name} ADD COLUMN IF NOT EXISTS '
                    f'vector vector({self._embedding_dimension})'
                ))
            else:
                await conn.execute(text(
                    f'CREATE TABLE IF NOT EXISTS {table.name} ('
                    f'concept_id VARCHAR(255) PRIMARY KEY, '
                    f'vector vector({self._embedding_dimension}))'
                ))

            idx_name = f'{table.name}_vector_hnsw_idx'
            await conn.execute(text(
                f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table.name} '
                f'USING hnsw (vector vector_cosine_ops)'
            ))

        return table

    async def _write_vectors(self,
                             table: Table,
                             rows: list[dict],
                             ):
        """
        Write a batch of (concept_id, vector) rows, upserting or updating depending on mode.
        :param table: The table to write to.
        :param rows: A list of {'concept_id': ..., 'vector': ...} dicts.
        """
        async with self.engine.begin() as conn:
            if self._shared_with_doc_db:
                # A plain UPDATE, not an upsert: the concept rows are already there (owned by
                # SqlDocumentDatabase), and inserting a bare (concept_id, vector) row would
                # violate that table's NOT NULL constraints on its other columns. The WHERE
                # bindparam is named differently from the "concept_id" column for the same
                # reason as SqlDocumentDatabase.update_vector_mapping: SQLAlchemy reserves the
                # column's own name for the implicit UPDATE bindparam.
                stmt = (
                    table.update()
                    .where(table.c.concept_id == bindparam('b_concept_id'))
                    .values(vector=bindparam('vector'))
                )
                renamed_rows = [{'b_concept_id': r['concept_id'], 'vector': r['vector']} for r in rows]
                await conn.execute(stmt, renamed_rows)
            else:
                stmt = pg_insert(table).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[table.c.concept_id],
                    set_={'vector': stmt.excluded.vector},
                )
                await conn.execute(stmt)

    async def load_embeddings(self,
                              prefix: ConceptPrefix,
                              embeddings: AsyncIterator[tuple[str, str, list[float]]],
                              total_embeddings: int | None = None,
                              ) -> dict[str, str]:
        """
        Load precomputed embeddings into the PostgreSQL vector store.
        :param prefix: The vocabulary prefix of the embeddings
        :param embeddings: An async iterator of tuples containing (concept_id, text, embedding_vector)
        :param total_embeddings: Optional total number of embeddings, used for progress tracking
        :return: A mapping of concept IDs to their assigned vector IDs
        """
        table = await self._ensure_table_and_index(prefix)

        rows: list[dict] = []
        id_map: dict[str, str] = {}

        async for concept_id, vector_id, vector in embeddings:
            rows.append({'concept_id': concept_id, 'vector': vector})
            id_map[concept_id] = vector_id

            if len(rows) >= 1000:
                await self._write_vectors(table, rows)
                rows = []

        if rows:
            await self._write_vectors(table, rows)

        return id_map

    async def insert_concepts(self,
                              concepts: list[Concept] | AsyncIterator[Concept],
                              prefix: ConceptPrefix,
                              total_concepts: int | None = None,
                              ) -> dict[str, str]:
        """
        Embed and insert concepts' vectors into the PostgreSQL vector store.
        :param concepts: list of Concept instances to insert, or an async iterator of Concept instances
        :param prefix: The prefix of the concepts being inserted
        :param total_concepts: Optional total number of concepts, used for progress tracking
        :return: A mapping of concept IDs to their assigned vector IDs
        """
        if not concepts:
            return {}

        if isinstance(concepts, list):
            total_concepts = len(concepts)

        transformer = ConceptTransformer()

        async def embedding_iter():
            async for embedded_batch in transformer.embed_concepts(concepts, total_concepts=total_concepts):
                for concept_id, vector in embedded_batch:
                    # No separate ID space is needed: vectors are keyed by concept_id directly,
                    # unlike Qdrant's point IDs.
                    yield concept_id, concept_id, vector

        return await self.load_embeddings(
            prefix=prefix,
            embeddings=embedding_iter(),
            total_embeddings=total_concepts,
        )

    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of concept vectors for a given prefix in the vector database.
        :param prefix: The vocabulary prefix to count vectors for.
        :return: The number of vectors as an integer.
        """
        table = self._table_for_prefix(prefix)

        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, table.name):
                return 0
            if not await self._vector_column_exists(conn, table.name):
                # Table exists (the vocabulary is loaded) but no embedding has ever been
                # written for it yet, so the "vector" column hasn't been added -- zero vectors,
                # not an error.
                return 0

            stmt = select(func.count()).select_from(table).where(table.c.vector.is_not(None))
            result = await conn.execute(stmt)
            return int(result.scalar_one())

    async def get_vectors_for_prefix_iter(self,
                                          prefix: ConceptPrefix,
                                          ) -> AsyncIterator[tuple[str, list[float]]]:
        """
        Get all vectors for a given prefix from the vector database as an async iterator.
        :param prefix: The vocabulary prefix to get vectors for.
        :return: An asynchronous iterator yielding tuples of concept IDs and their embedding vectors.
        """
        table = self._table_for_prefix(prefix)

        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, table.name):
                raise ValueError(f'Vocabulary prefix {prefix} does not exist in the PostgreSQL vector store.')
            if not await self._vector_column_exists(conn, table.name):
                # Loaded but never embedded yet -- no vectors to yield, same as an empty table.
                return

            stmt = select(table.c.concept_id, table.c.vector).where(table.c.vector.is_not(None))
            stream = await conn.stream(stmt)
            async for row in stream:
                yield row.concept_id, row.vector

    async def search_concepts_iter(self,
                                   query: str,
                                   prefix: ConceptPrefix,
                                   limit: int = 10,
                                   ) -> AsyncIterator[str]:
        """
        Search for concepts matching the query within the specified vocabulary prefix, and
        return an async iterator of matching concept IDs.
        :param query: The search query string.
        :param prefix: The vocabulary prefix to search within.
        :param limit: The top number of concepts to return.
        :return: An async iterator of matching concept IDs.
        """
        table = await self._ensure_table_and_index(prefix)

        text_transformer = TextTransformer()
        query_vector = text_transformer.embed_strings([query])[0]

        async with self.engine.connect() as conn:
            stmt = (
                select(table.c.concept_id)
                .where(table.c.vector.is_not(None))
                .order_by(table.c.vector.cosine_distance(query_vector))
                .limit(limit)
            )
            result = await conn.execute(stmt)
            for row in result:
                yield row.concept_id

    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all vectors for a given prefix from the vector database.
        :param prefix: The vocabulary prefix to delete vectors for.
        """
        table = self._table_for_prefix(prefix)

        async with self.engine.begin() as conn:
            if not await self._table_exists(conn, table.name):
                return
            if not await self._vector_column_exists(conn, table.name):
                # Loaded but never embedded yet -- nothing to clear/drop.
                return

            if self._shared_with_doc_db:
                # Clear the column rather than touching rows/table owned by SqlDocumentDatabase.
                await conn.execute(table.update().values(vector=None).where(table.c.vector.is_not(None)))
            else:
                await conn.execute(text(f'DROP TABLE IF EXISTS {table.name}'))
                self._tables_cache.pop(table.name, None)
