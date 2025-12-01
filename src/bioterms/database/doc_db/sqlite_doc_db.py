import re
import json
import aiosqlite
from typing import AsyncIterator

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import IndexCreationError
from bioterms.model.concept import Concept
from bioterms.vocabulary import get_vocabulary_config
from .doc_db import DocumentDatabase
from .utils import generate_extra_data


class SqliteDocumentDatabase(DocumentDatabase):
    """
    An SQLite implementation of the DocumentDatabase interface with
    sqlite.
    """

    _client: aiosqlite.Connection = None

    def __init__(self,
                 client: aiosqlite.Connection = None,
                 ):
        """
        Initialise the SqliteDocumentDatabase with an aiosqlite.Connection instance.
        :param client: aiosqlite.Connection instance.
        """
        if client is not None:
            self._client = client

    @property
    def db(self):
        """
        Get the SQLite database instance.
        :return: The SQLite database instance.
        """
        if self._client is None:
            raise ValueError('SQLite client is not set. Call set_client() first.')

        return self._client

    @classmethod
    def set_client(cls,
                   client: aiosqlite.Connection,
                   ):
        """
        Set the SQLite client for the database.
        :param client: AsyncMongoClient instance.
        """
        cls._client = client

    async def close(self) -> None:
        """
        Close the SQLite connection.
        """
        if self._client is not None:
            await self._client.close()
        else:
            raise ValueError('SQLite client is not set. Cannot close connection.')

    @staticmethod
    def _get_column_names(prefix: ConceptPrefix):
        """
        Get the column names for the concept table of the given prefix.

        Return a list of column names based on the concept class fields. If a field
        has an alias, use the alias as the column name; otherwise, use the field name.
        :param prefix: The vocabulary prefix.
        :return: A list of column names.
        """
        vocabulary_config = get_vocabulary_config(prefix)
        concept_class: type[Concept] = vocabulary_config['conceptClass']
        alias_map = {py_name: (fld.alias or py_name)
                     for py_name, fld in concept_class.model_fields.items()}
        alias_cols = list(alias_map.values())

        return alias_cols

    async def _ensure_concept_table_exists(self, prefix: ConceptPrefix):
        """
        Ensure that the concept table for the given prefix exists in the database.
        :param prefix: The vocabulary prefix.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())

        alias_cols = self._get_column_names(prefix)

        col_defs = ', '.join([f'"{c}" TEXT' for c in alias_cols])

        try:
            await self.db.execute('BEGIN IMMEDIATE')
            async with self.db.execute(
                    'SELECT 1 FROM sqlite_master WHERE type="table" AND name=?',
                    (table_name,)
            ) as cur:
                exists = await cur.fetchone() is not None

            if not exists:
                # Table does not exist, create it first
                await self.db.execute(
                    f'CREATE TABLE "{table_name}" ({col_defs})'
                )
            await self.db.execute('COMMIT')
        except Exception as e:
            await self.db.execute('ROLLBACK')
            raise e

    async def _ensure_ngram_table_exists(self, prefix: ConceptPrefix):
        """
        Ensure that the nGram table for the given prefix exists in the database.
        :param prefix: The vocabulary prefix.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        ngram_table_name = f'{table_name}_ngrams'

        try:
            await self.db.execute('BEGIN IMMEDIATE')
            async with self.db.execute(
                    'SELECT 1 FROM sqlite_master WHERE type="table" AND name=?',
                    (ngram_table_name,)
            ) as cur:
                exists = await cur.fetchone() is not None

            if not exists:
                # Table does not exist, create it first
                await self.db.execute(
                    f'''
                    CREATE TABLE "{ngram_table_name}" (
                        "conceptId" TEXT,
                        "nGram" TEXT
                    )
                    '''
                )

                # Index on nGram for faster search
                await self.db.execute(
                    f'CREATE INDEX "idx_{ngram_table_name}_nGram" ON "{ngram_table_name}" ("nGram")'
                )
            await self.db.execute('COMMIT')
        except Exception as e:
            await self.db.execute('ROLLBACK')
            raise e

    async def _ensure_search_text_table_exists(self, prefix: ConceptPrefix):
        """
        Ensure that the search text table for the given prefix exists in the database.
        :param prefix: The vocabulary prefix.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        search_text_table_name = f'{table_name}_search_text'

        try:
            await self.db.execute('BEGIN IMMEDIATE')
            async with self.db.execute(
                    'SELECT 1 FROM sqlite_master WHERE type="table" AND name=?',
                    (search_text_table_name,)
            ) as cur:
                exists = await cur.fetchone() is not None

            if not exists:
                # Table does not exist, create it first
                await self.db.execute(
                    f'''
                    CREATE TABLE "{search_text_table_name}" (
                        "conceptId" TEXT,
                        "searchText" TEXT
                    )
                    '''
                )

                # Index on conceptId for faster lookup
                await self.db.execute(
                    f'CREATE INDEX "idx_{search_text_table_name}_conceptId" ON "{search_text_table_name}" ("conceptId")'
                )
            await self.db.execute('COMMIT')
        except Exception as e:
            await self.db.execute('ROLLBACK')
            raise e

    async def create_index(self,
                           prefix: ConceptPrefix,
                           field: str,
                           unique: bool = False,
                           overwrite: bool = False,
                           ):
        """
        Create an index on a specified field in the document database.
        :param prefix: The vocabulary prefix to create the index for.
        :param field: The field to index.
        :param unique: Whether the index should enforce uniqueness.
        :param overwrite: Whether to overwrite an existing index. If False, creating
            an index with the same field may raise an error, depending on the database
            implementation.
        :raises IndexCreationError: If there is an error creating the index.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        index_name = f'idx_{table_name}_{field}'

        await self._ensure_concept_table_exists(prefix)

        try:
            await self.db.execute('BEGIN IMMEDIATE')

            async with self.db.execute(f'PRAGMA table_info("{table_name}")') as cur:
                existing_cols = {row[1] async for row in cur}
            if field not in existing_cols:
                raise IndexCreationError(
                    f'Field "{field}" does not exist in table "{table_name}".'
                )

            # Check if index exists
            async with self.db.execute(
                'SELECT 1 FROM sqlite_master WHERE type="index" AND name=?',
                (index_name,)
            ) as cur:
                ix_exists = await cur.fetchone() is not None

            if ix_exists:
                if overwrite:
                    await self.db.execute(
                        f'DROP INDEX IF EXISTS "{index_name}"'
                    )
                else:
                    raise IndexCreationError(
                        f'Index "{index_name}" already exists in table "{table_name}".'
                    )

            unique_kw = 'UNIQUE ' if unique else ''
            create_ix_sql = f'CREATE {unique_kw}INDEX "{index_name}" ON "{table_name}" ("{field}")'
            await self.db.execute(create_ix_sql)

            await self.db.execute('COMMIT')
        except Exception as e:
            await self.db.execute('ROLLBACK')
            raise IndexCreationError(
                f'Failed to create index on {prefix.value}.{field}: {e}'
            ) from e

    async def delete_index(self,
                           prefix: ConceptPrefix,
                           field: str,
                           ):
        """
        Delete an index on a specified field in the document database.
        :param prefix: The vocabulary prefix to delete the index for.
        :param field: The field to delete the index on.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        index_name = f'idx_{table_name}_{field}'

        await self.db.execute(f'DROP INDEX IF EXISTS "{index_name}"')

    async def save_terms(self,
                         terms: list[Concept],
                         ):
        """
        Save a list of terms into the mongodb.
        :param terms: A list of Concept instances to save.
        """
        # Check if all table exists
        if not terms:
            return

        prefix = terms[0].prefix
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        ngram_table_name = f'{table_name}_ngrams'
        search_text_table_name = f'{table_name}_search_text'

        # Ensure all table exists
        await self._ensure_concept_table_exists(prefix)
        await self._ensure_ngram_table_exists(prefix)
        await self._ensure_search_text_table_exists(prefix)

        extra_data = await generate_extra_data(terms)

        concepts: list[tuple] = []
        n_grams: list[tuple] = []
        search_texts: list[tuple] = []

        alias_cols = self._get_column_names(prefix)

        for concept, (term_id, ngrams, search_text) in zip(terms, extra_data):
            doc = concept.model_dump()

            # Prepare the rows for insertion
            concept_row = [doc.get(col) for col in alias_cols]
            for col_index, value in enumerate(concept_row):
                if isinstance(value, (dict, list)):
                    concept_row[col_index] = json.dumps(value)

            concepts.append(tuple(concept_row))
            for ngram in ngrams:
                n_grams.append((term_id, ngram))
            search_texts.append((term_id, search_text))

        # Insert concepts
        placeholders = ', '.join(['?'] * len(alias_cols))
        column_names = ', '.join([f'"{col}"' for col in alias_cols])

        try:
            await self.db.execute('BEGIN IMMEDIATE')
            await self.db.executemany(
                f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})',
                concepts
            )
            # Insert nGrams
            await self.db.executemany(
                f'INSERT INTO "{ngram_table_name}" ("conceptId", "nGram") VALUES (?, ?)',
                n_grams
            )
            # Insert searchTexts
            await self.db.executemany(
                f'INSERT INTO "{search_text_table_name}" ("conceptId", "searchText") VALUES (?, ?)',
                search_texts
            )

            await self.db.execute('COMMIT')
        except Exception as e:
            await self.db.execute('ROLLBACK')
            raise e

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of terms for a given prefix in the document database.
        :param prefix: The vocabulary prefix to count documents for.
        :return: The number of terms/documents
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        await self._ensure_concept_table_exists(prefix)

        async with self.db.execute(f'SELECT COUNT(*) FROM "{table_name}"') as cursor:
            row = await cursor.fetchone()
            count = row[0] if row else 0

        return count

    async def get_terms_iter(self,
                             prefix: ConceptPrefix,
                             limit: int = 0,
                             ) -> AsyncIterator[Concept]:
        """
        Get an asynchronous iterator over all items for a given prefix in the document database.
        :param prefix: The vocabulary prefix to get documents for.
        :param limit: The maximum number of documents to retrieve. If 0, retrieve all documents.
        :return: An asynchronous iterator yielding Concept instances.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        await self._ensure_concept_table_exists(prefix)

        query = f'SELECT * FROM "{table_name}"'
        if limit > 0:
            query += f' LIMIT {limit}'

        cursor = await self.db.execute(query)
        alias_cols = self._get_column_names(prefix)

        async for row in cursor:
            # Deserialize the dumped values
            doc = {}
            for col, value in zip(alias_cols, row):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, (dict, list)):
                        doc[col] = parsed
                    else:
                        doc[col] = value
                except (json.JSONDecodeError, TypeError):
                    doc[col] = value

            concept = Concept.model_validate(doc)
            yield concept

    async def get_terms_by_ids_iter(self,
                                    prefix: ConceptPrefix,
                                    concept_ids: list[str],
                                    ) -> AsyncIterator[Concept]:
        """
        Get terms by their IDs for a given prefix in the document database as an async iterator.
        :param prefix: The vocabulary prefix to get documents for.
        :param concept_ids: A list of concept IDs to retrieve.
        :return: An asynchronous iterator yielding Concept instances.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        await self._ensure_concept_table_exists(prefix)
        alias_cols = self._get_column_names(prefix)

        placeholders = ', '.join(['?'] * len(concept_ids))
        query = f'SELECT * FROM "{table_name}" WHERE "termId" IN ({placeholders})'
        cursor = await self.db.execute(query, tuple(concept_ids))

        async for row in cursor:
            doc = {}
            for col, value in zip(alias_cols, row):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, (dict, list)):
                        doc[col] = parsed
                    else:
                        doc[col] = value
                except (json.JSONDecodeError, TypeError):
                    doc[col] = value

            concept = Concept.model_validate(doc)
            yield concept

    async def delete_all_for_label(self,
                                   prefix: ConceptPrefix,
                                   ):
        """
        Delete all documents/records for a given label in the document database.
        :param prefix: The vocabulary prefix to delete documents for.
        """
        table_name = re.sub(r'\W+', '_', prefix.value.lower())
        ngram_table_name = f'{table_name}_ngrams'
        search_text_table_name = f'{table_name}_search_text'

        try:
            await self.db.execute('BEGIN IMMEDIATE')
            # Drop all tables if exist
            await self.db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            await self.db.execute(f'DROP TABLE IF EXISTS "{ngram_table_name}"')
            await self.db.execute(f'DROP TABLE IF EXISTS "{search_text_table_name}"')
            await self.db.execute('COMMIT')
        except Exception as e:
            await self.db.execute('ROLLBACK')
            raise e

        # Recreate empty tables
        await self._ensure_concept_table_exists(prefix)
        await self._ensure_ngram_table_exists(prefix)
        await self._ensure_search_text_table_exists(prefix)

    async def auto_complete_iter(self,
                                 prefix: ConceptPrefix,
                                 query: str,
                                 limit: int = None,
                                 ) -> AsyncIterator[Concept]:
        """
        Run an auto-complete search query against the document database and return an async iterator.
        :param prefix: The vocabulary prefix to search within.
        :param query: The search query string.
        :param limit: The maximum number of results to return. If None, return all matches.
        :return: An asynchronous iterator yielding Concept instances matching the auto-complete query.
        """
