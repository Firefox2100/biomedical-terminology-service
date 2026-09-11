"""
PostgreSQL/pgvector implementation of the VectorDatabase interface.
"""
from typing import AsyncIterator
from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, MetaData, String, Table, Text, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection

from bioterms.database.doc_db.sql_doc_db import safe_table_suffix
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.embedding import TextTransformer
from .vector_db import VectorDatabase, EmbeddingItemVector


class PostgresVectorDatabase(VectorDatabase):
    """
    PostgreSQL implementation of the VectorDatabase interface, using the pgvector extension for
    storage and HNSW/cosine-distance similarity search.

    Embedding items always live in their own dedicated `concept_<prefix>_vector_item` table
    (`item_id` primary key, `concept_id`, `kind`, `text`, `vector`), independent of whatever
    document database is actually in use. This works identically whether PostgreSQL is only
    the vector store or is also the document/graph store on the same instance (`BTS_SQL_DB_URL`
    / `BTS_POSTGRES_GRAPH_DB_URL` equal to `BTS_POSTGRES_VECTOR_DB_URL`) -- the vector-item
    table simply sits alongside the document database's own `concept_<prefix>` table rather
    than being merged into it, since a concept can have several embedding items and a single
    extra column could not represent that.
    """

    _engine: AsyncEngine | None = None

    def __init__(self,
                 engine: AsyncEngine | None = None,
                 ):
        """
        Initialise the PostgreSQL vector database.
        :param engine: Optional AsyncEngine instance or None to use the class variable.
        """
        if engine is not None:
            self._engine = engine

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
        Determine the name of the table backing a vocabulary prefix's embedding items.
        :param prefix: The vocabulary prefix.
        :return: The table name.
        """
        suffix = safe_table_suffix(prefix.value)
        return f'concept_{suffix}_vector_item'

    def _table_for_prefix(self,
                          prefix: ConceptPrefix,
                          dimension: int | None = None,
                          ) -> Table:
        """
        Get or create the SQLAlchemy Table object for a vocabulary prefix's embedding items.
        :param prefix: The vocabulary prefix.
        :param dimension: The embedding vector dimension, required the first time this table
            is declared (i.e. before it's cached); ignored on subsequent calls.
        :return: The Table object.
        """
        name = self._table_name(prefix)
        if name in self._tables_cache:
            return self._tables_cache[name]

        if dimension is None:
            dimension = TextTransformer().dimension

        table = Table(
            name,
            self._md,
            Column('item_id', String(255), primary_key=True),
            Column('concept_id', String(255), nullable=False),
            Column('kind', String(32), nullable=False),
            Column('text', Text, nullable=False),
            Column('vector', Vector(dimension)),
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

    async def _ensure_table_and_index(self,
                                      prefix: ConceptPrefix,
                                      ) -> Table:
        """
        Ensure the table and HNSW similarity index backing a vocabulary prefix's embedding
        items exist, creating them if necessary.
        :param prefix: The vocabulary prefix.
        :return: The Table object for the prefix.
        """
        dimension = TextTransformer().dimension
        table = self._table_for_prefix(prefix, dimension=dimension)

        async with self.engine.begin() as conn:
            await conn.execute(text('CREATE EXTENSION IF NOT EXISTS vector'))

            await conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS {table.name} ('
                f'item_id VARCHAR(255) PRIMARY KEY, '
                f'concept_id VARCHAR(255) NOT NULL, '
                f'kind VARCHAR(32) NOT NULL, '
                f'text TEXT NOT NULL, '
                f'vector vector({dimension}))'
            ))
            await conn.execute(text(
                f'CREATE INDEX IF NOT EXISTS ix_{table.name}_concept_id ON {table.name} (concept_id)'
            ))
            await conn.execute(text(
                f'CREATE INDEX IF NOT EXISTS ix_{table.name}_kind ON {table.name} (kind)'
            ))

            idx_name = f'{table.name}_vector_hnsw_idx'
            await conn.execute(text(
                f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table.name} '
                f'USING hnsw (vector vector_cosine_ops)'
            ))

        return table

    async def load_embedding_items(self,
                                   prefix: ConceptPrefix,
                                   items: AsyncIterator[EmbeddingItemVector],
                                   total_items: int | None = None,
                                   ) -> int:
        """
        Load precomputed embedding items into the PostgreSQL vector store.
        :param prefix: The vocabulary prefix of the embedding items
        :param items: An async iterator of EmbeddingItemVector instances
        :param total_items: Optional total number of items, used for progress tracking (unused
            by this driver, kept for interface compatibility)
        :return: The number of embedding items written
        """
        table = await self._ensure_table_and_index(prefix)

        rows: list[dict] = []
        written = 0

        async def flush():
            if not rows:
                return
            async with self.engine.begin() as conn:
                stmt = pg_insert(table).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[table.c.item_id],
                    set_={
                        'concept_id': stmt.excluded.concept_id,
                        'kind': stmt.excluded.kind,
                        'text': stmt.excluded.text,
                        'vector': stmt.excluded.vector,
                    },
                )
                await conn.execute(stmt)

        async for item in items:
            rows.append({
                'item_id': item.item_id,
                'concept_id': item.concept_id,
                'kind': item.kind.value,
                'text': item.text,
                'vector': item.vector,
            })
            written += 1

            if len(rows) >= 1000:
                await flush()
                rows = []

        await flush()

        return written

    async def count_vectors(self,
                            prefix: ConceptPrefix,
                            ) -> int:
        """
        Count the number of embedding items for a given prefix in the vector database.
        :param prefix: The vocabulary prefix to count embedding items for.
        :return: The number of embedding items as an integer.
        """
        table_name = self._table_name(prefix)

        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, table_name):
                return 0

            result = await conn.execute(text(f'SELECT count(*) FROM {table_name}'))
            return int(result.scalar_one())

    async def search_items_iter(self,
                                query_vector: list[float],
                                prefix: ConceptPrefix,
                                kind: EmbeddingKind,
                                limit: int = 10,
                                ) -> AsyncIterator[tuple[str, str, float]]:
        """
        Search for embedding items of the given kind whose vector is closest to
        `query_vector`, within the specified vocabulary prefix.
        :param query_vector: The already-embedded query vector.
        :param prefix: The vocabulary prefix to search within.
        :param kind: Which embedding items to search (ALIAS or DEFINITION).
        :param limit: The top number of items to return.
        :return: An async iterator of (concept_id, item_text, score) tuples, best match first.
        """
        table = self._table_for_prefix(prefix, dimension=len(query_vector))

        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, table.name):
                return

            distance = table.c.vector.cosine_distance(query_vector)
            stmt = (
                select(table.c.concept_id, table.c.text, distance.label('distance'))
                .where(table.c.kind == kind.value)
                .order_by(distance)
                .limit(limit)
            )
            result = await conn.execute(stmt)
            for row in result:
                yield row.concept_id, row.text, 1.0 - float(row.distance)

    async def delete_vectors_for_prefix(self,
                                        prefix: ConceptPrefix,
                                        ) -> None:
        """
        Delete all embedding items for a given prefix from the vector database by dropping
        the `concept_<prefix>_vector_item` table outright, so a later `load_embedding_items`
        starts from a clean schema (e.g. after an embedding model change alters the vector
        dimension).
        :param prefix: The vocabulary prefix to delete embedding items for.
        """
        table_name = self._table_name(prefix)

        async with self.engine.begin() as conn:
            await conn.execute(text(f'DROP TABLE IF EXISTS {table_name}'))
            self._tables_cache.pop(table_name, None)
