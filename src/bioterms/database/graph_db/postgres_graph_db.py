"""
PostgreSQL implementation of the GraphDatabase interface.

Unlike Neo4j's single unified property graph, this driver models the graph as plain relational
tables, queried with standard SQL (joins, recursive CTEs, and a precomputed transitive-closure
table for the hierarchy) rather than a graph query language or extension:

- Native SQL/PGQ (SQL:2023's property-graph query feature) only landed in PostgreSQL 19, which
  is still in beta at the time this driver was written, and its initial implementation covers
  only fixed-depth pattern matching -- it cannot express the unbounded ancestor/descendant
  traversal or shortest-path queries this interface needs. Extensions such as Apache AGE were
  deliberately avoided per the requirements this driver was built against.
- A plain relational schema works on any current PostgreSQL (tested against 18) and lets the
  hottest paths -- descendant/ancestor traversal and similarity threshold lookups -- be served
  by precomputed tables and straightforward indexed queries instead of graph traversal at query
  time.

Schema, at a glance (see `_ensure_prefix_schema`/`_ensure_shared_schema` for exact DDL):

- `graph_node_<prefix>` / `graph_edge_<prefix>` / `graph_closure_<prefix>`: one set of tables per
  `ConceptPrefix`, holding that vocabulary's own nodes, "internal" relationships (is_a, part_of,
  replaced_by, preceded_by, has_input, has_output, ohdsi_relationship, consider -- always
  same-prefix on both ends, matching how `save_vocabulary_graph` is actually called), and a
  precomputed transitive closure over the is_a/part_of edges specifically. Segmenting per prefix
  keeps each vocabulary's indexes small even though e.g. SNOMED (~1M concepts) and OHDSI (~10M
  concepts across 150+ sub-vocabularies) are enormous; the closure table turns the
  ancestor/descendant hot path from a traversal into an indexed lookup. OHDSI is not further
  segmented by sub-vocabulary here -- the OHDSI vocabulary loader does not currently capture
  which of its ~150 source vocabularies each concept came from, so there is no key to partition
  on without changing that loader too; this is a reasonable follow-up if OHDSI's own tables
  become a bottleneck in practice.
- `graph_annotation`: one table for all cross-vocabulary annotation edges (exact, broad, narrow,
  related, has_symbol, alias_symbol, previous_symbol, annotated_with), partitioned by source
  prefix. A single table (rather than one per prefix pair) is what makes `map_terms_iter`'s
  multi-hop, prefix-alternating traversal expressible as one recursive CTE at all; the
  partitioning still gives most of the per-prefix segmentation benefit.
- `graph_similarity`: one table for all similarity scores, also partitioned by source prefix.
  Neo4j stores every (method, corpus) score as a separate property on one `similar_to`
  relationship per concept pair; here each (concept_from, concept_to, method, corpus_prefix) is
  its own row, which is both simpler and lets threshold queries use a plain indexed range scan
  instead of Cypher's per-relationship property-key filtering.

Every abstract method here that could plausibly be called with a partially-populated `types`
array or malformed relationship type deliberately does not validate input `ConceptPrefix`/`str`
matches the underlying `ConceptRelationshipType`/`AnnotationType` enums; the graph tables index
those as plain TEXT, mirroring Neo4j's own untyped relationship-type/property model.
"""
import asyncio
import time
from typing import AsyncIterator, Iterable, Optional
from sqlalchemy import Float, Text, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
import networkx as nx

from bioterms.database.doc_db.sql_doc_db import safe_table_suffix
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod, ConceptRelationshipType, AnnotationType
from bioterms.etc.metrics import GRAPHDB_OP_DURATION, GRAPHDB_OP_TTFR, GRAPHDB_OP_ERRORS, \
    EXPAND_DESC_COUNT, MAP_COUNT, SIM_GROUPS, SIM_PER_GROUP, SIM_TOTAL
from bioterms.model.concept import Concept, GRAPH_NODE_EXTRA_PROPERTIES, GRAPH_NODE_EXTRA_PROPERTY_COLUMNS, \
    GRAPH_NODE_EXTRA_PROPERTY_SQL_TYPES
from bioterms.model.annotation import Annotation
from bioterms.model.concept_path import NodeInPath, ConceptPath
from bioterms.model.related_term import RelatedTerm
from bioterms.model.similar_term import SimilarTermWithScores, SimilarTermByPrefix, SimilarTerm, \
    SimilarTermAggregate
from bioterms.model.translated_term import TranslatedTerm
from .graph_db import ReactomeRepository, GraphDatabase


_HIERARCHY_REL_TYPES = (ConceptRelationshipType.IS_A.value, ConceptRelationshipType.PART_OF.value)

# "No corpus" is stored as an empty string rather than NULL: PostgreSQL unique constraints treat
# every NULL as distinct from every other NULL, so a NULL corpus_prefix column could not enforce
# "at most one score per (concept_from, concept_to, method, no corpus)".
_NO_CORPUS = ''


def _prefix_str(prefix: ConceptPrefix | str) -> str:
    """
    Normalise a prefix (ConceptPrefix or already-a-string, e.g. an OHDSI sub-vocabulary that
    doesn't have a ConceptPrefix of its own) to its string form.
    :param prefix: The prefix to normalise.
    :return: The string form of the prefix.
    """
    return prefix.value if isinstance(prefix, ConceptPrefix) else prefix


def _array_param(name: str, values: list) -> bindparam:
    """
    Build a bindparam for a Postgres TEXT[] parameter, so it can be used with ANY()/UNNEST() in
    raw SQL without the driver misinferring its type from a plain Python list.
    :param name: The bindparam name.
    :param values: The list of string values.
    :return: The bindparam.
    """
    return bindparam(name, value=values, type_=ARRAY(Text))


def _float_array_param(name: str, values: list[float]) -> bindparam:
    """
    Build a bindparam for a Postgres DOUBLE PRECISION[] parameter.
    :param name: The bindparam name.
    :param values: The list of float values.
    :return: The bindparam.
    """
    return bindparam(name, value=[float(v) for v in values], type_=ARRAY(Float))


class PostgresReactomeRepository(ReactomeRepository):
    """
    PostgreSQL implementation of the ReactomeRepository interface, operating entirely within
    the `graph_node_reactome`/`graph_edge_reactome` tables (Reactome's pathways, reactions, and
    the gene placeholder nodes it references are all modelled as REACTOME-prefixed nodes,
    distinguished by their `types` array, matching how `save_vocabulary_graph` always stores a
    vocabulary's internal edges with both endpoints in that same vocabulary's own tables).
    """

    def __init__(self,
                 engine: AsyncEngine,
                 ):
        """
        Initialise the PostgresReactomeRepository with a SQLAlchemy async engine.
        :param engine: The AsyncEngine to use for database connections.
        """
        self._engine = engine

    async def _related_by_type(self,
                               ids: list[str],
                               id_label: str,
                               source_type: str,
                               rel_type: str,
                               target_type: str,
                               direction: str,
                               ) -> list[RelatedTerm]:
        """
        Shared implementation for the Reactome repository's one-hop "related concepts of a
        given type, reached via a given relationship type and direction" queries.
        :param ids: The concept IDs to find related concepts for.
        :param id_label: Unused, kept for readability at call sites.
        :param source_type: The required `types` entry for the input concepts.
        :param rel_type: The relationship type to traverse.
        :param target_type: The required `types` entry for the related concepts.
        :param direction: "out" to follow the edge forward (source->target), "in" to follow it
            backward (target->source).
        :return: A list of RelatedTerm instances, one per input ID that exists with the
            required source_type (IDs that don't exist, or aren't of that type, are omitted).
        """
        if not ids:
            return []

        edge_join = (
            'e.source_id = n.concept_id' if direction == 'out' else 'e.target_id = n.concept_id'
        )
        related_col = 'e.target_id' if direction == 'out' else 'e.source_id'

        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(f"""
                    SELECT ids.concept_id, array_remove(array_agg(DISTINCT related.concept_id), NULL) AS related
                    FROM unnest(:ids) AS ids(concept_id)
                    JOIN graph_node_reactome n
                        ON n.concept_id = ids.concept_id AND :source_type = ANY(n.types)
                    LEFT JOIN graph_edge_reactome e
                        ON {edge_join} AND e.rel_type = :rel_type
                    LEFT JOIN graph_node_reactome related
                        ON related.concept_id = {related_col} AND :target_type = ANY(related.types)
                    GROUP BY ids.concept_id
                """).bindparams(_array_param('ids', ids)),
                {'source_type': source_type, 'rel_type': rel_type, 'target_type': target_type},
            )

            return [
                RelatedTerm(conceptId=row.concept_id, relatedConcepts=list(row.related))
                for row in result
            ]

    async def get_sub_pathways(self, pathway_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            pathway_ids, 'pathway_id', 'pathway', ConceptRelationshipType.PART_OF.value, 'pathway', 'in',
        )

    async def get_super_pathways(self, pathway_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            pathway_ids, 'pathway_id', 'pathway', ConceptRelationshipType.PART_OF.value, 'pathway', 'out',
        )

    async def get_reactions_in_pathway(self, pathway_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            pathway_ids, 'pathway_id', 'pathway', ConceptRelationshipType.PART_OF.value, 'reaction', 'in',
        )

    async def get_pathways_of_reaction(self, reaction_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            reaction_ids, 'reaction_id', 'reaction', ConceptRelationshipType.PART_OF.value, 'pathway', 'out',
        )

    async def get_preceding_reactions(self, reaction_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            reaction_ids, 'reaction_id', 'reaction', ConceptRelationshipType.PRECEDED_BY.value, 'reaction', 'out',
        )

    async def get_subsequent_reactions(self, reaction_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            reaction_ids, 'reaction_id', 'reaction', ConceptRelationshipType.PRECEDED_BY.value, 'reaction', 'in',
        )

    async def get_reaction_inputs(self, reaction_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            reaction_ids, 'reaction_id', 'reaction', ConceptRelationshipType.HAS_INPUT.value, 'gene', 'out',
        )

    async def get_reaction_outputs(self, reaction_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            reaction_ids, 'reaction_id', 'reaction', ConceptRelationshipType.HAS_OUTPUT.value, 'gene', 'out',
        )

    async def get_gene_input_reactions(self, gene_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            gene_ids, 'gene_id', 'gene', ConceptRelationshipType.HAS_INPUT.value, 'reaction', 'in',
        )

    async def get_gene_output_reactions(self, gene_ids: list[str]) -> list[RelatedTerm]:
        return await self._related_by_type(
            gene_ids, 'gene_id', 'gene', ConceptRelationshipType.HAS_OUTPUT.value, 'reaction', 'in',
        )


class PostgresGraphDatabase(GraphDatabase):
    """
    PostgreSQL implementation of the GraphDatabase interface. See the module docstring for the
    schema this drives.
    """

    _engine: AsyncEngine | None = None

    def __init__(self,
                 engine: AsyncEngine | None = None,
                 ):
        """
        Initialise the PostgreSQL graph database.
        :param engine: Optional AsyncEngine instance or None to use the class variable.
        """
        if engine is not None:
            self._engine = engine

        self._prefix_schema_ready: set[str] = set()
        self._shared_schema_ready = False

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

    async def close(self) -> None:
        """
        Dispose of the SQLAlchemy engine.
        """
        if self._engine is not None:
            await self._engine.dispose()

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    async def _ensure_prefix_schema(self,
                                    conn: AsyncConnection,
                                    prefix: ConceptPrefix,
                                    ) -> str:
        """
        Ensure the node/edge/closure tables for a vocabulary prefix exist.
        :param conn: The connection to execute DDL on.
        :param prefix: The vocabulary prefix.
        :return: The safe table-name suffix for this prefix (e.g. "hpo").
        """
        p = safe_table_suffix(prefix.value)
        if p in self._prefix_schema_ready:
            return p

        # GRAPH_NODE_EXTRA_PROPERTIES columns: every prefix's table gets every column (see
        # that constant's docstring) so a fresh CREATE TABLE and an ALTER on a table from
        # before these columns existed both converge on the same schema.
        extra_columns_ddl = ''.join(
            f', {GRAPH_NODE_EXTRA_PROPERTY_COLUMNS[prop]} {GRAPH_NODE_EXTRA_PROPERTY_SQL_TYPES[prop]}'
            for prop in GRAPH_NODE_EXTRA_PROPERTIES
        )
        await conn.execute(text(f'CREATE TABLE IF NOT EXISTS graph_node_{p} ('
                                 f'concept_id TEXT PRIMARY KEY, '
                                 f"types TEXT[] NOT NULL DEFAULT '{{}}'"
                                 f'{extra_columns_ddl})'))
        for prop in GRAPH_NODE_EXTRA_PROPERTIES:
            column = GRAPH_NODE_EXTRA_PROPERTY_COLUMNS[prop]
            sql_type = GRAPH_NODE_EXTRA_PROPERTY_SQL_TYPES[prop]
            await conn.execute(text(
                f'ALTER TABLE graph_node_{p} ADD COLUMN IF NOT EXISTS {column} {sql_type}'
            ))
            # Indexed for the same reason as Neo4j's per-property indexes: an unindexed
            # equality filter (e.g. organism_tax_id = '9606' at UniProt's 250M+-node scale)
            # would otherwise be a full table scan.
            await conn.execute(text(
                f'CREATE INDEX IF NOT EXISTS ix_graph_node_{p}_{column} ON graph_node_{p} ({column})'
            ))
        await conn.execute(text(f'CREATE TABLE IF NOT EXISTS graph_edge_{p} ('
                                 f'source_id TEXT NOT NULL, '
                                 f'target_id TEXT NOT NULL, '
                                 f'rel_type TEXT NOT NULL, '
                                 f'PRIMARY KEY (source_id, target_id, rel_type))'))
        await conn.execute(text(
            f'CREATE INDEX IF NOT EXISTS ix_graph_edge_{p}_target ON graph_edge_{p} (target_id, rel_type)'
        ))
        await conn.execute(text(f'CREATE TABLE IF NOT EXISTS graph_closure_{p} ('
                                 f'ancestor_id TEXT NOT NULL, '
                                 f'descendant_id TEXT NOT NULL, '
                                 f'depth INT NOT NULL, '
                                 f'PRIMARY KEY (ancestor_id, descendant_id))'))
        await conn.execute(text(
            f'CREATE INDEX IF NOT EXISTS ix_graph_closure_{p}_descendant '
            f'ON graph_closure_{p} (descendant_id)'
        ))

        self._prefix_schema_ready.add(p)
        return p

    async def _ensure_shared_schema(self,
                                    conn: AsyncConnection,
                                    ):
        """
        Ensure the shared, prefix-partitioned annotation and similarity tables exist, with one
        partition per known ConceptPrefix plus a default partition for arbitrary prefix strings
        (e.g. an unmapped OHDSI sub-vocabulary).
        :param conn: The connection to execute DDL on.
        """
        if self._shared_schema_ready:
            return

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS graph_annotation (
                prefix_from TEXT NOT NULL,
                concept_from TEXT NOT NULL,
                prefix_to TEXT NOT NULL,
                concept_to TEXT NOT NULL,
                rel_type TEXT NOT NULL,
                PRIMARY KEY (prefix_from, concept_from, prefix_to, concept_to, rel_type)
            ) PARTITION BY LIST (prefix_from)
        """))
        await conn.execute(text(
            'CREATE INDEX IF NOT EXISTS ix_graph_annotation_reverse ON graph_annotation (prefix_to, concept_to)'
        ))

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS graph_similarity (
                prefix_from TEXT NOT NULL,
                concept_from TEXT NOT NULL,
                prefix_to TEXT NOT NULL,
                concept_to TEXT NOT NULL,
                method TEXT NOT NULL,
                corpus_prefix TEXT NOT NULL DEFAULT '',
                score DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (prefix_from, concept_from, prefix_to, concept_to, method, corpus_prefix)
            ) PARTITION BY LIST (prefix_from)
        """))
        await conn.execute(text(
            'CREATE INDEX IF NOT EXISTS ix_graph_similarity_from_score '
            'ON graph_similarity (prefix_from, concept_from, score DESC)'
        ))
        await conn.execute(text(
            'CREATE INDEX IF NOT EXISTS ix_graph_similarity_to_score '
            'ON graph_similarity (prefix_to, concept_to, score DESC)'
        ))

        for prefix in ConceptPrefix:
            p = safe_table_suffix(prefix.value)
            await conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS graph_annotation_p_{p} "
                f"PARTITION OF graph_annotation FOR VALUES IN ('{prefix.value}')"
            ))
            await conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS graph_similarity_p_{p} "
                f"PARTITION OF graph_similarity FOR VALUES IN ('{prefix.value}')"
            ))
        await conn.execute(text(
            'CREATE TABLE IF NOT EXISTS graph_annotation_p_default PARTITION OF graph_annotation DEFAULT'
        ))
        await conn.execute(text(
            'CREATE TABLE IF NOT EXISTS graph_similarity_p_default PARTITION OF graph_similarity DEFAULT'
        ))

        self._shared_schema_ready = True

    async def create_index(self):
        """
        Ensure the shared, prefix-partitioned annotation/similarity tables exist. Cheap and
        idempotent, matching Neo4j's own create_index() (structural DDL only) -- unlike Neo4j,
        this does *not* rebuild any vocabulary's ancestor/descendant closure table, since
        `vocabulary.create_indexes()` calls this *before* loading a vocabulary's data, and a
        closure rebuild needs that vocabulary's edges to already exist. `save_vocabulary_graph`
        rebuilds the closure table for its own prefix once its edges are actually in place;
        call `refresh_closure_table()` directly if a prefix's hierarchy edges were changed some
        other way and its closure table needs to catch up.
        """
        async with self.engine.begin() as conn:
            await self._ensure_shared_schema(conn)

    async def refresh_closure_table(self,
                                    prefix: ConceptPrefix,
                                    ):
        """
        Rebuild one vocabulary prefix's ancestor/descendant closure table over its current
        is_a/part_of edges. `save_vocabulary_graph` already does this automatically for the
        prefix it just saved; call this directly only if that prefix's hierarchy edges were
        changed through some other path.
        :param prefix: The vocabulary prefix to rebuild the closure table for.
        """
        async with self.engine.begin() as conn:
            p = await self._ensure_prefix_schema(conn, prefix)
            await self._build_closure(conn, p)

    async def _build_closure(self,
                             conn: AsyncConnection,
                             p: str,
                             ):
        """
        (Re)build the ancestor/descendant transitive closure table for one vocabulary prefix,
        over its is_a/part_of edges.
        :param conn: The connection to execute on.
        :param p: The safe table-name suffix for the prefix.
        """
        await conn.execute(text(f'TRUNCATE graph_closure_{p}'))
        await conn.execute(text(f"""
            INSERT INTO graph_closure_{p} (ancestor_id, descendant_id, depth)
            WITH RECURSIVE closure(ancestor_id, descendant_id, depth) AS (
                SELECT target_id, source_id, 1
                FROM graph_edge_{p}
                WHERE rel_type IN :hierarchy_types
                UNION ALL
                SELECT e.target_id, c.descendant_id, c.depth + 1
                FROM closure c
                JOIN graph_edge_{p} e ON e.source_id = c.ancestor_id AND e.rel_type IN :hierarchy_types
                WHERE c.depth < :max_depth
            )
            SELECT ancestor_id, descendant_id, MIN(depth)
            FROM closure
            GROUP BY ancestor_id, descendant_id
        """).bindparams(bindparam('hierarchy_types', expanding=True)), {
            'hierarchy_types': list(_HIERARCHY_REL_TYPES),
            'max_depth': CONFIG.postgres_graph_closure_max_depth,
        })

    # ------------------------------------------------------------------
    # Vocabulary graph CRUD
    # ------------------------------------------------------------------

    async def save_vocabulary_graph(self,
                                    concepts: list[Concept] | Iterable[Concept],
                                    graph: nx.DiGraph | nx.MultiDiGraph | Iterable[tuple[str, str, Optional[str], Optional[str]]],
                                    consume_concepts: bool = False,
                                    ):
        """
        Save the vocabulary graph to the graph database.
        :param concepts: The concepts to save. May be a plain list, or any other (single-pass)
            iterable -- e.g. a generator streaming an offline dump file -- in which case only
            one batch's worth is ever held in memory at a time.
        :param graph: The vocabulary graph to save. Either an `nx.DiGraph`/`nx.MultiDiGraph`,
            or an iterable of `(source_id, target_id, relationship_type, relationship_key)`
            edge tuples in the same shape `edge_iter` produces -- see `edge_iter`.
        :param consume_concepts: Unused here (SQLAlchemy needs the full batch regardless); kept
            for interface compatibility.
        """
        from bioterms.etc.utils import batch_iterable, edge_iter, peek_first

        first_concept, concepts = peek_first(concepts)
        if first_concept is None:
            return
        prefix = first_concept.prefix

        async with self.engine.begin() as conn:
            p = await self._ensure_prefix_schema(conn, prefix)

            extra_columns = [GRAPH_NODE_EXTRA_PROPERTY_COLUMNS[prop] for prop in GRAPH_NODE_EXTRA_PROPERTIES]
            all_columns = ['concept_id', 'types'] + extra_columns
            update_set = ', '.join(f'{col} = EXCLUDED.{col}' for col in ['types'] + extra_columns)

            node_upsert = text(f"""
                INSERT INTO graph_node_{p} ({', '.join(all_columns)})
                VALUES ({', '.join(':' + col for col in all_columns)})
                ON CONFLICT (concept_id) DO UPDATE SET {update_set}
            """).bindparams(bindparam('types', type_=ARRAY(Text)))

            for batch in batch_iterable(concepts, consume=consume_concepts):
                rows = []
                for c in batch:
                    dumped = c.model_dump()
                    row = {'concept_id': c.concept_id, 'types': [t.value for t in c.concept_types]}
                    for prop in GRAPH_NODE_EXTRA_PROPERTIES:
                        row[GRAPH_NODE_EXTRA_PROPERTY_COLUMNS[prop]] = dumped.get(prop)
                    rows.append(row)
                await conn.execute(node_upsert, rows)

            bare_node_upsert = text(
                f'INSERT INTO graph_node_{p} (concept_id) VALUES (:concept_id) ON CONFLICT (concept_id) DO NOTHING'
            )
            edge_upsert = text(f"""
                INSERT INTO graph_edge_{p} (source_id, target_id, rel_type)
                VALUES (:source_id, :target_id, :rel_type)
                ON CONFLICT (source_id, target_id, rel_type) DO NOTHING
            """)

            for batch in batch_iterable(edge_iter(graph)):
                edge_rows = [
                    {'source_id': source, 'target_id': target, 'rel_type': rel_label or 'related_to'}
                    for source, target, rel_label, _rel_key in batch
                ]
                if not edge_rows:
                    continue

                node_ids = sorted({e['source_id'] for e in edge_rows} | {e['target_id'] for e in edge_rows})
                await conn.execute(bare_node_upsert, [{'concept_id': nid} for nid in node_ids])
                await conn.execute(edge_upsert, edge_rows)

            # Rebuilt here rather than left to create_index(): the generic vocabulary-load
            # pipeline (vocabulary.create_indexes()) calls create_index() *before* loading
            # data, since for Neo4j index creation is just a structural, order-independent
            # DDL statement. This driver's closure table is a full recompute of the
            # is_a/part_of transitive closure, which needs the edges above to already exist,
            # so it is (re)built here, once this call's edges are actually in place.
            await self._build_closure(conn, p)

    async def get_vocabulary_graph(self,
                                   prefix: ConceptPrefix,
                                   with_similarity: bool = False,
                                   ) -> nx.MultiDiGraph:
        """
        Retrieve the vocabulary graph from the graph database.
        :param prefix: The node prefix of the vocabulary to retrieve.
        :param with_similarity: Whether to include similarity relationships in the graph.
        :return: The vocabulary graph.
        """
        vocabulary_graph = nx.MultiDiGraph()
        p = safe_table_suffix(prefix.value)

        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, f'graph_node_{p}'):
                return vocabulary_graph

            nodes = await conn.execute(text(f'SELECT concept_id FROM graph_node_{p}'))
            for row in nodes:
                vocabulary_graph.add_node(row.concept_id)

            edges = await conn.execute(text(f'SELECT source_id, target_id, rel_type FROM graph_edge_{p}'))
            for row in edges:
                vocabulary_graph.add_edge(
                    row.source_id, row.target_id, label=ConceptRelationshipType(row.rel_type),
                )

            if with_similarity and await self._table_exists(conn, 'graph_similarity'):
                sims = await conn.execute(text(
                    'SELECT concept_from, concept_to FROM graph_similarity '
                    'WHERE prefix_from = :prefix AND prefix_to = :prefix'
                ), {'prefix': prefix.value})
                for row in sims:
                    vocabulary_graph.add_edge(row.concept_from, row.concept_to, label='similar_to')

        return vocabulary_graph

    @staticmethod
    async def _table_exists(conn: AsyncConnection, table_name: str) -> bool:
        """
        Check whether a table exists, without raising if it does not.
        :param conn: The connection to check on.
        :param table_name: The name of the table to check for.
        :return: True if the table exists, False otherwise.
        """
        result = await conn.execute(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': table_name})
        return bool(result.scalar())

    async def delete_vocabulary_graph(self,
                                      prefix: ConceptPrefix,
                                      ):
        """
        Delete the vocabulary graph from the graph database: the prefix's own node/edge/closure
        tables are dropped outright, and any annotation/similarity rows involving the prefix
        (in either direction) are removed.
        :param prefix: The node prefix of the vocabulary to delete.
        """
        p = safe_table_suffix(prefix.value)

        async with self.engine.begin() as conn:
            await conn.execute(text(f'DROP TABLE IF EXISTS graph_closure_{p}'))
            await conn.execute(text(f'DROP TABLE IF EXISTS graph_edge_{p}'))
            await conn.execute(text(f'DROP TABLE IF EXISTS graph_node_{p}'))
            self._prefix_schema_ready.discard(p)

            if await self._table_exists(conn, 'graph_annotation'):
                await conn.execute(
                    text('DELETE FROM graph_annotation WHERE prefix_from = :prefix OR prefix_to = :prefix'),
                    {'prefix': prefix.value},
                )
            if await self._table_exists(conn, 'graph_similarity'):
                await conn.execute(
                    text('DELETE FROM graph_similarity WHERE prefix_from = :prefix OR prefix_to = :prefix'),
                    {'prefix': prefix.value},
                )

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        """
        Count the number of nodes for a given prefix in the graph database.
        :param prefix: The vocabulary prefix to count nodes for.
        :return: The number of nodes with the given prefix.
        """
        p = safe_table_suffix(prefix.value)
        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, f'graph_node_{p}'):
                return 0
            result = await conn.execute(text(f'SELECT count(*) FROM graph_node_{p}'))
            return int(result.scalar_one())

    async def count_internal_relationships(self,
                                           prefix: ConceptPrefix,
                                           ) -> int:
        """
        Count the number of internal relationships within a vocabulary in the graph database.
        :param prefix: The vocabulary prefix to count relationships for
        :return: The number of internal relationships within the vocabulary.
        """
        p = safe_table_suffix(prefix.value)
        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, f'graph_edge_{p}'):
                return 0
            result = await conn.execute(text(f'SELECT count(*) FROM graph_edge_{p}'))
            return int(result.scalar_one())

    async def count_similarity_relationships(self,
                                             prefix_from: ConceptPrefix,
                                             prefix_to: ConceptPrefix,
                                             configurations: list[tuple[SimilarityMethod, ConceptPrefix | None]],
                                             ) -> list[tuple[SimilarityMethod, ConceptPrefix | None, int]]:
        """
        Count the number of similarity relationships between two vocabularies in the graph database,
        for each similarity method and corpus configuration.
        :param prefix_from: The source vocabulary prefix.
        :param prefix_to: The target vocabulary prefix.
        :param configurations: A list of tuples containing similarity methods and corpus prefixes
            (or None for intrinsic similarity).
        :return: A list of tuples containing the similarity method, corpus prefix,
            and the number of similarity relationships.
        """
        if not configurations:
            return []

        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, 'graph_similarity'):
                return [(method, corpus, 0) for method, corpus in configurations]

            methods = [m.value for m, _ in configurations]
            corpora = [c.value if c else _NO_CORPUS for _, c in configurations]

            result = await conn.execute(text("""
                SELECT cfg.method, cfg.corpus_prefix, count(s.score) AS relationship_count
                FROM UNNEST(:methods, :corpora) AS cfg(method, corpus_prefix)
                LEFT JOIN graph_similarity s
                    ON s.prefix_from = :prefix_from AND s.prefix_to = :prefix_to
                    AND s.method = cfg.method AND s.corpus_prefix = cfg.corpus_prefix
                GROUP BY cfg.method, cfg.corpus_prefix
            """).bindparams(_array_param('methods', methods), _array_param('corpora', corpora)), {
                'prefix_from': prefix_from.value,
                'prefix_to': prefix_to.value,
            })

            counts = {(row.method, row.corpus_prefix): row.relationship_count for row in result}

        return [
            (method, corpus, counts.get((method.value, corpus.value if corpus else _NO_CORPUS), 0))
            for method, corpus in configurations
        ]

    # ------------------------------------------------------------------
    # Annotations
    # ------------------------------------------------------------------

    async def save_annotations(self,
                               annotations: list[Annotation],
                               ):
        """
        Save a list of annotations into the graph database.
        :param annotations: A list of Annotation instances to save.
        """
        from bioterms.etc.utils import batch_iterable

        if not annotations:
            return

        async with self.engine.begin() as conn:
            await self._ensure_shared_schema(conn)

            for batch in batch_iterable(annotations):
                await conn.execute(text("""
                    INSERT INTO graph_annotation (prefix_from, concept_from, prefix_to, concept_to, rel_type)
                    SELECT * FROM UNNEST(:prefixes_from, :concepts_from, :prefixes_to, :concepts_to, :rel_types)
                    ON CONFLICT (prefix_from, concept_from, prefix_to, concept_to, rel_type) DO NOTHING
                """).bindparams(
                    _array_param('prefixes_from', [_prefix_str(a.prefix_from) for a in batch]),
                    _array_param('concepts_from', [a.concept_id_from for a in batch]),
                    _array_param('prefixes_to', [_prefix_str(a.prefix_to) for a in batch]),
                    _array_param('concepts_to', [a.concept_id_to for a in batch]),
                    _array_param('rel_types', [a.annotation_type.value for a in batch]),
                ))

    async def get_annotation_graph(self,
                                   prefix_1: ConceptPrefix,
                                   prefix_2: ConceptPrefix,
                                   ) -> nx.DiGraph:
        """
        Retrieve the annotation graph between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        :return: The annotation graph between the two vocabularies.
        """
        annotation_graph = nx.DiGraph()

        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, 'graph_annotation'):
                return annotation_graph

            result = await conn.execute(text("""
                SELECT prefix_from, concept_from, prefix_to, concept_to, rel_type
                FROM graph_annotation
                WHERE (prefix_from = :p1 AND prefix_to = :p2) OR (prefix_from = :p2 AND prefix_to = :p1)
            """), {'p1': prefix_1.value, 'p2': prefix_2.value})

            for row in result:
                annotation_graph.add_edge(
                    f'{row.prefix_from}:{row.concept_from}',
                    f'{row.prefix_to}:{row.concept_to}',
                    label=AnnotationType(row.rel_type),
                )

        return annotation_graph

    async def delete_annotations(self,
                                 prefix_1: ConceptPrefix,
                                 prefix_2: ConceptPrefix,
                                 ):
        """
        Delete annotations between two vocabularies from the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        """
        async with self.engine.begin() as conn:
            if not await self._table_exists(conn, 'graph_annotation'):
                return
            await conn.execute(text(
                'DELETE FROM graph_annotation WHERE prefix_from = :p1 AND prefix_to = :p2'
            ), {'p1': prefix_1.value, 'p2': prefix_2.value})

    async def count_annotations(self,
                                prefix_1: ConceptPrefix,
                                prefix_2: ConceptPrefix,
                                ) -> int:
        """
        Count the number of annotations between two vocabularies in the graph database.
        :param prefix_1: The first vocabulary prefix.
        :param prefix_2: The second vocabulary prefix.
        :return: The number of annotations between the two vocabularies.
        """
        async with self.engine.connect() as conn:
            if not await self._table_exists(conn, 'graph_annotation'):
                return 0
            result = await conn.execute(text(
                'SELECT count(*) FROM graph_annotation WHERE prefix_from = :p1 AND prefix_to = :p2'
            ), {'p1': prefix_1.value, 'p2': prefix_2.value})
            return int(result.scalar_one())

    # ------------------------------------------------------------------
    # Similarity
    # ------------------------------------------------------------------

    async def save_similarity_scores(self,
                                     prefix_from: ConceptPrefix,
                                     prefix_to: ConceptPrefix,
                                     similarity_scores: list[tuple[str, str, float]],
                                     similarity_method: SimilarityMethod,
                                     corpus_prefix: ConceptPrefix | None = None,
                                     ):
        """
        Save similarity scores between two vocabularies into the graph database.
        :param prefix_from: The source vocabulary prefix. Correspond to 'concept_from' in similarity_df.
        :param prefix_to: The target vocabulary prefix. Correspond to 'concept_to' in similarity_df.
        :param similarity_scores: A list of tuple containing similarity scores. In the format of:
            | concept_from | concept_to | similarity |
        :param similarity_method: The similarity method used to generate the scores.
        :param corpus_prefix: The corpus vocabulary prefix, if applicable.
        """
        from bioterms.etc.utils import batch_iterable

        if not similarity_scores:
            return

        corpus_value = corpus_prefix.value if corpus_prefix else _NO_CORPUS

        async with self.engine.begin() as conn:
            await self._ensure_shared_schema(conn)

            for batch in batch_iterable(similarity_scores):
                await conn.execute(text("""
                    INSERT INTO graph_similarity
                        (prefix_from, concept_from, prefix_to, concept_to, method, corpus_prefix, score)
                    SELECT :prefix_from, f.concept_from, :prefix_to, f.concept_to, :method, :corpus, f.score
                    FROM UNNEST(:concepts_from, :concepts_to, :scores) AS f(concept_from, concept_to, score)
                    ON CONFLICT (prefix_from, concept_from, prefix_to, concept_to, method, corpus_prefix)
                    DO UPDATE SET score = EXCLUDED.score
                """).bindparams(
                    _array_param('concepts_from', [s[0] for s in batch]),
                    _array_param('concepts_to', [s[1] for s in batch]),
                    _float_array_param('scores', [s[2] for s in batch]),
                ), {
                    'prefix_from': prefix_from.value,
                    'prefix_to': prefix_to.value,
                    'method': similarity_method.value,
                    'corpus': corpus_value,
                })

    # ------------------------------------------------------------------
    # Hierarchy traversal (ancestors / descendants)
    # ------------------------------------------------------------------

    async def _closure_lookup(self,
                              prefix: ConceptPrefix,
                              concept_ids: list[str],
                              max_depth: int | None,
                              limit: int | None,
                              direction: str,
                              ) -> AsyncIterator[RelatedTerm]:
        """
        Shared implementation for ancestor/descendant lookups against the closure table.
        :param prefix: The vocabulary prefix.
        :param concept_ids: The concept IDs to look up.
        :param max_depth: The maximum depth to include, or None for unbounded.
        :param limit: The maximum number of related concepts to return per input ID, or None.
        :param direction: "ancestors" or "descendants".
        :return: An async iterator of RelatedTerm, one per input concept ID (even if empty).
        """
        p = safe_table_suffix(prefix.value)
        lookup_col = 'descendant_id' if direction == 'ancestors' else 'ancestor_id'
        related_col = 'ancestor_id' if direction == 'ancestors' else 'descendant_id'

        depth_clause = 'AND c.depth <= :max_depth' if max_depth is not None else ''
        limit_clause = 'LIMIT :limit' if limit is not None else ''

        related_by_id: dict[str, list[str]] = {cid: [] for cid in concept_ids}

        async with self.engine.connect() as conn:
            if await self._table_exists(conn, f'graph_closure_{p}'):
                params = {'concept_ids': concept_ids}
                if max_depth is not None:
                    params['max_depth'] = max_depth
                if limit is not None:
                    params['limit'] = limit

                result = await conn.execute(text(f"""
                    SELECT ids.concept_id, related.{related_col}
                    FROM unnest(:concept_ids) AS ids(concept_id)
                    LEFT JOIN LATERAL (
                        SELECT {related_col} FROM graph_closure_{p} c
                        WHERE c.{lookup_col} = ids.concept_id {depth_clause}
                        ORDER BY c.depth
                        {limit_clause}
                    ) related ON true
                """).bindparams(_array_param('concept_ids', concept_ids)), params)

                for row in result:
                    value = getattr(row, related_col)
                    if value is not None:
                        related_by_id[row.concept_id].append(value)

        for cid in concept_ids:
            yield RelatedTerm(conceptId=cid, relatedConcepts=list(set(related_by_id[cid])))

    def trace_ancestors_iter(self,
                             prefix: ConceptPrefix,
                             concept_ids: list[str],
                             max_depth: int | None = None,
                             limit: int | None = None,
                             ) -> AsyncIterator[RelatedTerm]:
        """
        Trace the given terms to retrieve their ancestors up to the specified depth.
        :param prefix: The prefix of the concepts to trace.
        :param concept_ids: The list of concept IDs to trace.
        :param max_depth: The maximum depth to trace. If None, trace to all depths.
        :param limit: The maximum number of ancestors to return for each term. If None, return all.
        :return: An asynchronous iterator yielding RelatedTerm instances.
        """
        return self._closure_lookup(prefix, concept_ids, max_depth, limit, 'ancestors')

    async def expand_terms_iter(self,
                                prefix: ConceptPrefix,
                                concept_ids: list[str],
                                max_depth: int | None = None,
                                limit: int | None = None,
                                ) -> AsyncIterator[RelatedTerm]:
        """
        Expand the given terms to retrieve their descendants up to the specified depth.
        :param prefix: The prefix of the concepts to expand.
        :param concept_ids: The list of concept IDs to expand.
        :param max_depth: The maximum depth to expand. If None, expand to all depths.
        :param limit: The maximum number of descendants to return for each term. If None, return all.
        :return: An asynchronous iterator yielding RelatedTerm instances.
        """
        mode = 'unbounded' if max_depth is None else 'bounded'
        start = time.perf_counter()
        first = None
        result_label = 'ok'

        try:
            async for related in self._closure_lookup(prefix, concept_ids, max_depth, limit, 'descendants'):
                if first is None:
                    first = time.perf_counter()

                EXPAND_DESC_COUNT.labels(prefix=prefix.value, mode=mode).observe(len(related.related_concepts))
                yield related
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            GRAPHDB_OP_ERRORS.labels(
                backend='postgresql', op='expand', prefix=prefix.value, error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            GRAPHDB_OP_DURATION.labels(
                backend='postgresql', op='expand', prefix=prefix.value, mode=mode, result=result_label,
            ).observe(end - start)
            if first is not None:
                GRAPHDB_OP_TTFR.labels(
                    backend='postgresql', op='expand', prefix=prefix.value, mode=mode, result=result_label,
                ).observe(first - start)

    async def _one_hop_iter(self,
                            prefix: ConceptPrefix,
                            concept_ids: list[str],
                            rel_type: str,
                            direction: str,
                            ) -> AsyncIterator[RelatedTerm]:
        """
        Shared implementation for one-hop, single-relationship-type lookups within one prefix's
        own edge table (get_replaced_terms/get_replacing_terms).
        :param prefix: The vocabulary prefix.
        :param concept_ids: The concept IDs to look up.
        :param rel_type: The relationship type to follow.
        :param direction: "out" to follow source->target, "in" to follow target->source.
        :return: An async iterator of RelatedTerm, one per input concept ID (even if empty).
        """
        p = safe_table_suffix(prefix.value)
        lookup_col = 'source_id' if direction == 'out' else 'target_id'
        related_col = 'target_id' if direction == 'out' else 'source_id'

        related_by_id: dict[str, list[str]] = {cid: [] for cid in concept_ids}

        async with self.engine.connect() as conn:
            if await self._table_exists(conn, f'graph_edge_{p}'):
                result = await conn.execute(text(f"""
                    SELECT {lookup_col} AS concept_id, {related_col} AS related_id
                    FROM graph_edge_{p}
                    WHERE {lookup_col} = ANY(:concept_ids) AND rel_type = :rel_type
                """).bindparams(_array_param('concept_ids', concept_ids)), {'rel_type': rel_type})

                for row in result:
                    related_by_id[row.concept_id].append(row.related_id)

        for cid in concept_ids:
            yield RelatedTerm(conceptId=cid, relatedConcepts=list(set(related_by_id[cid])))

    def get_replaced_terms_iter(self,
                                prefix: ConceptPrefix,
                                concept_ids: list[str],
                                ) -> AsyncIterator[RelatedTerm]:
        """
        Get the concepts replaced by the given concept IDs.
        :param prefix: The prefix of the concepts to find replacements for.
        :param concept_ids: The list of concept IDs to find replacements for.
        :return: An asynchronous iterator yielding RelatedTerm instances.
        """
        return self._one_hop_iter(prefix, concept_ids, ConceptRelationshipType.REPLACED_BY.value, 'in')

    def get_replacing_terms_iter(self,
                                 prefix: ConceptPrefix,
                                 concept_ids: list[str],
                                 ) -> AsyncIterator[RelatedTerm]:
        """
        Get the concepts that replace the given concept IDs.
        :param prefix: The prefix of the concepts to find replacing terms for.
        :param concept_ids: The list of concept IDs to find replacing terms for.
        :return: An asynchronous iterator yielding RelatedTerm instances.
        """
        return self._one_hop_iter(prefix, concept_ids, ConceptRelationshipType.REPLACED_BY.value, 'out')

    # ------------------------------------------------------------------
    # Cross-vocabulary mapping
    # ------------------------------------------------------------------

    async def map_terms_iter(self,
                             prefix: ConceptPrefix,
                             target_prefix: ConceptPrefix,
                             concept_ids: list[str],
                             max_hops: int = 1,
                             limit: int | None = None,
                             ) -> AsyncIterator[RelatedTerm]:
        """
        Map terms from one vocabulary to another, via undirected annotation-type edges
        (annotated_with, has_symbol, exact, broad, narrow, related), never revisiting a
        vocabulary prefix within one path.
        :param prefix: The source prefix.
        :param target_prefix: The target prefix.
        :param concept_ids: The list of concept IDs to map.
        :param max_hops: The maximum number of mapping hops to consider.
        :param limit: The maximum number of mapped terms to return for each concept ID.
        :return: An asynchronous iterator yielding RelatedTerm instances.
        """
        start = time.perf_counter()
        first = None
        result_label = 'ok'
        rel_types = [
            AnnotationType.ANNOTATED_WITH.value, AnnotationType.HAS_SYMBOL.value,
            AnnotationType.EXACT.value, AnnotationType.BROAD.value,
            AnnotationType.NARROW.value, AnnotationType.RELATED.value,
        ]

        try:
            related_by_id: dict[str, set[str]] = {cid: set() for cid in concept_ids}

            async with self.engine.connect() as conn:
                if await self._table_exists(conn, 'graph_annotation'):
                    result = await conn.execute(text("""
                        WITH RECURSIVE path(root_id, cur_prefix, cur_id, visited_prefixes, hops) AS (
                            SELECT concept_id, :prefix, concept_id, ARRAY[:prefix], 0
                            FROM unnest(:concept_ids) AS concept_id
                          UNION ALL
                            SELECT p.root_id, next.prefix, next.id, p.visited_prefixes || next.prefix, p.hops + 1
                            FROM path p
                            JOIN LATERAL (
                                SELECT prefix_to AS prefix, concept_to AS id
                                FROM graph_annotation a
                                WHERE a.prefix_from = p.cur_prefix AND a.concept_from = p.cur_id
                                    AND a.rel_type = ANY(:rel_types)
                                UNION
                                SELECT prefix_from AS prefix, concept_from AS id
                                FROM graph_annotation a
                                WHERE a.prefix_to = p.cur_prefix AND a.concept_to = p.cur_id
                                    AND a.rel_type = ANY(:rel_types)
                            ) next ON true
                            WHERE p.hops < :max_hops
                                AND NOT (next.prefix = ANY(p.visited_prefixes))
                        )
                        SELECT DISTINCT root_id, cur_id
                        FROM path
                        WHERE cur_prefix = :target_prefix AND hops > 0
                    """).bindparams(
                        _array_param('concept_ids', concept_ids), _array_param('rel_types', rel_types),
                    ), {
                        'prefix': prefix.value,
                        'target_prefix': target_prefix.value,
                        'max_hops': max_hops,
                    })

                    for row in result:
                        related_by_id[row.root_id].add(row.cur_id)

            for cid in concept_ids:
                if first is None:
                    first = time.perf_counter()

                mapped = list(related_by_id[cid])
                if limit is not None:
                    mapped = mapped[:limit]

                MAP_COUNT.labels(prefix=prefix.value, target_prefix=target_prefix.value).observe(len(mapped))
                yield RelatedTerm(conceptId=cid, relatedConcepts=mapped)
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            GRAPHDB_OP_ERRORS.labels(
                backend='postgresql', op='map', prefix=prefix.value,
                target_prefix=target_prefix.value, error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            mode = 'bounded' if limit is not None else 'unbounded'
            GRAPHDB_OP_DURATION.labels(
                backend='postgresql', op='map', prefix=prefix.value,
                target_prefix=target_prefix.value, mode=mode, result=result_label,
            ).observe(end - start)
            if first is not None:
                GRAPHDB_OP_TTFR.labels(
                    backend='postgresql', op='map', prefix=prefix.value,
                    target_prefix=target_prefix.value, mode=mode, result=result_label,
                ).observe(first - start)

    # ------------------------------------------------------------------
    # Point-to-point path tracing
    # ------------------------------------------------------------------

    @staticmethod
    def _drop_detour_paths(paths: list[list[tuple[str, str]]]) -> list[list[tuple[str, str]]]:
        """
        Drop any path that is a superset of another, shorter path with node order preserved (a
        "detour") -- mirroring the Cypher detour filter in the Neo4j driver. A path q is a
        detour relative to p if every node of q appears in p, in the same relative order.
        :param paths: Candidate paths, each a list of (concept_id, prefix) node tuples.
        :return: The paths that are not detours of any other candidate.
        """
        def _is_ordered_subsequence(shorter, longer):
            idx = 0
            for node in shorter:
                found = False
                while idx < len(longer):
                    if longer[idx] == node:
                        found = True
                        idx += 1
                        break
                    idx += 1
                if not found:
                    return False
            return True

        keep = []
        for path in paths:
            if any(
                other is not path and len(other) < len(path) and _is_ordered_subsequence(other, path)
                for other in paths
            ):
                continue
            keep.append(path)
        return keep

    async def trace_term_iter(self,
                              prefix_start: ConceptPrefix,
                              prefix_end: ConceptPrefix,
                              id_start: str,
                              id_end: str,
                              relationship_type: AnnotationType | ConceptRelationshipType,
                              forward: bool | None = True,
                              max_depth: int = 12,
                              ) -> AsyncIterator[ConceptPath]:
        """
        Trace one or more paths between two terms, connected through nodes of any prefix via a
        single relationship type (searched across both this driver's per-prefix internal edge
        tables and the shared annotation table, since the relationship type alone doesn't say
        which one it lives in).

        Returns all available paths without repeating sequence; if a path is a subset of
        another path with order preserved, only the shorter path is returned.
        :param prefix_start: The prefix of the starting concept.
        :param prefix_end: The prefix of the ending concept.
        :param id_start: The ID of the starting concept.
        :param id_end: The ID of the ending concept.
        :param relationship_type: The type of relationship to trace through.
        :param forward: If True, direction must be start->end; if False, end->start; if None,
            direction is ignored and only the shortest path is returned.
        :param max_depth: The maximum depth to trace.
        :return: An asynchronous iterator yielding ConceptPath instances.
        """
        paths = await self._find_paths(
            prefix_start.value, id_start, prefix_end.value, id_end,
            relationship_type.value, isinstance(relationship_type, ConceptRelationshipType), forward, max_depth,
        )

        for path in paths:
            yield ConceptPath(
                startConceptId=id_start,
                endConceptId=id_end,
                startPrefix=prefix_start.value,
                endPrefix=prefix_end.value,
                length=len(path),
                nodes=[NodeInPath(conceptId=cid, prefix=pfx) for cid, pfx in path],
            )

    async def trace_term_aggregate_iter(self,
                                        trace_queries: list[tuple[
                                            ConceptPrefix, str, ConceptPrefix, str,
                                            ConceptRelationshipType | AnnotationType, bool | None, int,
                                        ]]
                                        ) -> AsyncIterator[ConceptPath]:
        """
        Trace multiple paths between multiple pairs of terms.
        :param trace_queries: A list of tuples containing:
            (prefix_start, id_start, prefix_end, id_end, relationship_type, forward, max_depth)
        :return: An asynchronous iterator yielding ConceptPath instances.
        """
        for prefix_start, id_start, prefix_end, id_end, relationship_type, forward, max_depth in trace_queries:
            paths = await self._find_paths(
                prefix_start.value, id_start, prefix_end.value, id_end, relationship_type.value,
                isinstance(relationship_type, ConceptRelationshipType), forward, max_depth,
            )
            for path in paths:
                yield ConceptPath(
                    startConceptId=id_start,
                    endConceptId=id_end,
                    startPrefix=prefix_start.value,
                    endPrefix=prefix_end.value,
                    length=len(path),
                    nodes=[NodeInPath(conceptId=cid, prefix=pfx) for cid, pfx in path],
                )

    async def _find_paths(self,
                          prefix_start: str,
                          id_start: str,
                          prefix_end: str,
                          id_end: str,
                          rel_type: str,
                          is_internal: bool,
                          forward: bool | None,
                          max_depth: int,
                          ) -> list[list[tuple[str, str]]]:
        """
        Find simple paths (no repeated node) between two specific (prefix, concept_id) nodes,
        following one relationship type.

        A relationship type lives in exactly one store: `ConceptRelationshipType` values are
        always "internal" (both endpoints of every such edge share one vocabulary's own
        `graph_edge_<prefix>` table -- matching how `save_vocabulary_graph` always writes them),
        so if prefix_start != prefix_end no path can possibly exist and this returns immediately
        without querying. `AnnotationType` values always live in the shared, genuinely
        cross-prefix `graph_annotation` table.
        :param prefix_start: The prefix of the starting concept.
        :param id_start: The ID of the starting concept.
        :param prefix_end: The prefix of the ending concept.
        :param id_end: The ID of the ending concept.
        :param rel_type: The relationship type to traverse.
        :param is_internal: Whether rel_type is a ConceptRelationshipType (graph_edge_<prefix>)
            as opposed to an AnnotationType (graph_annotation).
        :param forward: True for start->end only, False for end->start only, None for either
            direction (only the shortest path(s) are returned in that case).
        :param max_depth: The maximum path length (edge count) to consider.
        :return: A list of paths, each a list of (concept_id, prefix) tuples from start to end,
            already filtered for detours, and sorted shortest-first.
        """
        directed = forward is not None
        want_forward = forward is not False
        params = {
            'id_start': id_start,
            'id_end': id_end,
            'rel_type': rel_type,
            'max_depth': max_depth,
            'forward_ok': want_forward or not directed,
            'backward_ok': (not want_forward) or not directed,
        }

        if is_internal:
            if prefix_start != prefix_end:
                return []

            async with self.engine.connect() as conn:
                p = safe_table_suffix(prefix_start)
                if not await self._table_exists(conn, f'graph_edge_{p}'):
                    return []

                rows = await conn.execute(text(f"""
                    WITH RECURSIVE frontier(cur_id, path_ids, depth) AS (
                        SELECT :id_start, ARRAY[:id_start], 0
                      UNION ALL
                        SELECT next.id, f.path_ids || next.id, f.depth + 1
                        FROM frontier f
                        JOIN LATERAL (
                            SELECT target_id AS id FROM graph_edge_{p}
                            WHERE source_id = f.cur_id AND rel_type = :rel_type AND (:forward_ok)
                            UNION ALL
                            SELECT source_id AS id FROM graph_edge_{p}
                            WHERE target_id = f.cur_id AND rel_type = :rel_type AND (:backward_ok)
                        ) next ON true
                        WHERE f.depth < :max_depth AND NOT (next.id = ANY(f.path_ids))
                    )
                    SELECT path_ids FROM frontier WHERE cur_id = :id_end AND depth > 0
                """), params)

                candidates = [[(cid, prefix_start) for cid in row.path_ids] for row in rows]
        else:
            async with self.engine.connect() as conn:
                if not await self._table_exists(conn, 'graph_annotation'):
                    return []

                rows = await conn.execute(text("""
                    WITH RECURSIVE frontier(cur_prefix, cur_id, path_ids, path_prefixes, depth) AS (
                        SELECT :prefix_start, :id_start, ARRAY[:id_start], ARRAY[:prefix_start], 0
                      UNION ALL
                        SELECT next.prefix, next.id,
                            f.path_ids || next.id, f.path_prefixes || next.prefix, f.depth + 1
                        FROM frontier f
                        JOIN LATERAL (
                            SELECT prefix_to AS prefix, concept_to AS id FROM graph_annotation
                            WHERE prefix_from = f.cur_prefix AND concept_from = f.cur_id AND rel_type = :rel_type
                                AND (:forward_ok)
                            UNION ALL
                            SELECT prefix_from AS prefix, concept_from AS id FROM graph_annotation
                            WHERE prefix_to = f.cur_prefix AND concept_to = f.cur_id AND rel_type = :rel_type
                                AND (:backward_ok)
                        ) next ON true
                        WHERE f.depth < :max_depth
                            AND NOT (next.id = ANY(f.path_ids) AND next.prefix = f.cur_prefix)
                    )
                    SELECT path_ids, path_prefixes
                    FROM frontier
                    WHERE cur_prefix = :prefix_end AND cur_id = :id_end AND depth > 0
                """), {**params, 'prefix_start': prefix_start, 'prefix_end': prefix_end})

                candidates = [list(zip(row.path_ids, row.path_prefixes)) for row in rows]

        if not candidates:
            return []

        if not directed:
            shortest_len = min(len(p) for p in candidates)
            return [p for p in candidates if len(p) == shortest_len][:1]

        filtered = self._drop_detour_paths(candidates)
        filtered.sort(key=len)
        return filtered

    # ------------------------------------------------------------------
    # Similarity lookups
    # ------------------------------------------------------------------

    async def get_similar_terms_aggregate_iter(self,
                                               prefix: ConceptPrefix,
                                               similarity_queries: list[tuple[str, float]],
                                               ) -> AsyncIterator[SimilarTermAggregate]:
        """
        Get similar terms for a list of concept IDs, returning only the highest similarity score
        per similar term regardless of method/corpus.
        :param prefix: The prefix of the concepts to find similar terms for.
        :param similarity_queries: A list of (concept_id, threshold) tuples.
        :return: An asynchronous iterator yielding SimilarTermAggregate instances.
        """
        if not similarity_queries:
            return

        by_id: dict[str, list[tuple[str, float]]] = {cid: [] for cid, _ in similarity_queries}

        async with self.engine.connect() as conn:
            if await self._table_exists(conn, 'graph_similarity'):
                result = await conn.execute(text("""
                    SELECT q.concept_id, m.other_id, max(m.score) AS highest_score
                    FROM unnest(:concept_ids, :thresholds) AS q(concept_id, threshold)
                    JOIN LATERAL (
                        SELECT concept_to AS other_id, score FROM graph_similarity
                        WHERE prefix_from = :prefix AND concept_from = q.concept_id
                            AND prefix_to = :prefix AND score >= q.threshold
                        UNION ALL
                        SELECT concept_from AS other_id, score FROM graph_similarity
                        WHERE prefix_to = :prefix AND concept_to = q.concept_id
                            AND prefix_from = :prefix AND score >= q.threshold
                    ) m ON true
                    GROUP BY q.concept_id, m.other_id
                """).bindparams(
                    _array_param('concept_ids', [cid for cid, _ in similarity_queries]),
                    _float_array_param('thresholds', [t for _, t in similarity_queries]),
                ), {'prefix': prefix.value})

                for row in result:
                    by_id[row.concept_id].append((row.other_id, row.highest_score))

        for cid, _ in similarity_queries:
            yield SimilarTermAggregate(conceptId=cid, similarConcepts=by_id[cid])

    async def get_similar_terms_iter(self,
                                     prefix: ConceptPrefix,
                                     concept_ids: list[str],
                                     threshold: float = 1.0,
                                     same_prefix: bool = True,
                                     corpus_prefix: ConceptPrefix | None = None,
                                     method: SimilarityMethod | None = None,
                                     limit: int | None = None,
                                     ) -> AsyncIterator[SimilarTerm]:
        """
        Get similar terms for the given concept IDs.
        :param prefix: The prefix of the concepts to find similar terms for.
        :param concept_ids: The list of concept IDs to find similar terms for.
        :param threshold: The similarity threshold to filter similar terms.
        :param same_prefix: Whether to only consider similar terms within the same prefix.
        :param corpus_prefix: The corpus prefix that was used to calculate the similarity score.
        :param method: The similarity method to use.
        :param limit: The maximum number of similar terms to return for each concept ID.
        :return: An asynchronous iterator yielding SimilarTerm instances.
        """
        variant = 'same_prefix' if same_prefix else 'cross_prefix'
        start = time.perf_counter()
        first = None
        result_label = 'ok'

        try:
            # concept_id -> prefix -> other_id -> {key: score}
            grouped: dict[str, dict[str, dict[str, dict[str, float]]]] = {
                cid: {} for cid in concept_ids
            }

            async with self.engine.connect() as conn:
                if await self._table_exists(conn, 'graph_similarity'):
                    prefix_filter = '' if not same_prefix else 'AND m.other_prefix = :prefix'
                    result = await conn.execute(text(f"""
                        SELECT q.concept_id, m.other_prefix, m.other_id, m.method, m.corpus_prefix, m.score
                        FROM unnest(:concept_ids) AS q(concept_id)
                        JOIN LATERAL (
                            SELECT prefix_to AS other_prefix, concept_to AS other_id, method, corpus_prefix, score
                            FROM graph_similarity
                            WHERE prefix_from = :prefix AND concept_from = q.concept_id AND score >= :threshold
                                AND (:method IS NULL OR method = :method)
                                AND (:corpus IS NULL OR corpus_prefix = :corpus)
                            UNION ALL
                            SELECT prefix_from AS other_prefix, concept_from AS other_id, method, corpus_prefix, score
                            FROM graph_similarity
                            WHERE prefix_to = :prefix AND concept_to = q.concept_id AND score >= :threshold
                                AND (:method IS NULL OR method = :method)
                                AND (:corpus IS NULL OR corpus_prefix = :corpus)
                        ) m ON true
                        WHERE true {prefix_filter}
                    """).bindparams(
                        _array_param('concept_ids', concept_ids),
                        bindparam('method', type_=Text),
                        bindparam('corpus', type_=Text),
                    ), {
                        'prefix': prefix.value,
                        'threshold': threshold,
                        'method': method.value if method else None,
                        'corpus': corpus_prefix.value if corpus_prefix else None,
                    })

                    for row in result:
                        by_prefix = grouped[row.concept_id].setdefault(row.other_prefix, {})
                        by_id = by_prefix.setdefault(row.other_id, {})
                        key = f'{row.method}:{row.corpus_prefix}' if row.corpus_prefix else row.method
                        by_id[key] = max(by_id.get(key, row.score), row.score)

            for cid in concept_ids:
                if first is None:
                    first = time.perf_counter()

                groups: list[SimilarTermByPrefix] = []
                total = 0
                for other_prefix in sorted(grouped[cid].keys()):
                    concepts_by_score = sorted(
                        grouped[cid][other_prefix].items(),
                        key=lambda kv: max(kv[1].values()), reverse=True,
                    )
                    if limit is not None:
                        concepts_by_score = concepts_by_score[:limit]

                    similar_concepts = [
                        SimilarTermWithScores(conceptId=other_id, similarity_scores=scores)
                        for other_id, scores in concepts_by_score
                    ]
                    SIM_PER_GROUP.labels(prefix=prefix.value, variant=variant).observe(len(similar_concepts))
                    total += len(similar_concepts)

                    groups.append(SimilarTermByPrefix(
                        prefix=ConceptPrefix(other_prefix), similarConcepts=similar_concepts,
                    ))

                if not groups:
                    continue

                SIM_GROUPS.labels(prefix=prefix.value, variant=variant).observe(len(groups))
                SIM_TOTAL.labels(prefix=prefix.value, variant=variant).observe(total)
                yield SimilarTerm(conceptId=cid, similarGroups=groups)
        except asyncio.CancelledError:
            result_label = 'cancelled'
            raise
        except Exception as e:
            result_label = 'error'
            GRAPHDB_OP_ERRORS.labels(
                backend='postgresql', op='get_similar_terms', prefix=prefix.value, error_type=type(e).__name__,
            ).inc()
            raise
        finally:
            end = time.perf_counter()
            GRAPHDB_OP_DURATION.labels(
                backend='postgresql', op='get_similar_terms', prefix=prefix.value, mode=variant, result=result_label,
            ).observe(end - start)
            if first is not None:
                GRAPHDB_OP_TTFR.labels(
                    backend='postgresql', op='get_similar_terms', prefix=prefix.value,
                    mode=variant, result=result_label,
                ).observe(first - start)

    async def translate_terms_iter(self,
                                   original_ids: list[str],
                                   original_prefix: ConceptPrefix,
                                   constraint_ids: dict[ConceptPrefix, set[str]],
                                   threshold: float = 1.0,
                                   limit: int | None = None,
                                   ) -> AsyncIterator[TranslatedTerm]:
        """
        Translate terms to a subset of the constraint vocabulary, based on similarity scores.
        :param original_ids: The list of original concept IDs to translate.
        :param original_prefix: The prefix of the original concepts.
        :param constraint_ids: A dictionary mapping constraint vocabulary prefixes to sets of concept IDs.
        :param threshold: The similarity threshold to filter translations.
        :param limit: The maximum number of translations to return for each original concept ID.
        :return: An asynchronous iterator yielding TranslatedTerm instances.
        """
        if not original_ids or not constraint_ids:
            return

        by_id: dict[str, list[tuple[str, str, float]]] = {cid: [] for cid in original_ids}

        # Flattened to two parallel arrays -- one (prefix, concept_id) pair per row -- rather
        # than one array-of-arrays per prefix, so this can bind as plain 1-D TEXT[] parameters.
        constraint_prefixes: list[str] = []
        constraint_concept_ids: list[str] = []
        for prefix, ids in constraint_ids.items():
            for cid in ids:
                constraint_prefixes.append(prefix.value)
                constraint_concept_ids.append(cid)

        async with self.engine.connect() as conn:
            if constraint_concept_ids and await self._table_exists(conn, 'graph_similarity'):
                result = await conn.execute(text("""
                    WITH constraints AS (
                        SELECT * FROM unnest(:constraint_prefixes, :constraint_concept_ids)
                            AS c(constraint_prefix, constraint_id)
                    )
                    SELECT q.original_id, m.other_prefix, m.other_id, max(m.score) AS best_score
                    FROM unnest(:original_ids) AS q(original_id)
                    JOIN LATERAL (
                        SELECT prefix_to AS other_prefix, concept_to AS other_id, score
                        FROM graph_similarity s
                        JOIN constraints c ON c.constraint_prefix = s.prefix_to AND c.constraint_id = s.concept_to
                        WHERE s.prefix_from = :original_prefix AND s.concept_from = q.original_id
                            AND s.score >= :threshold
                        UNION ALL
                        SELECT prefix_from AS other_prefix, concept_from AS other_id, score
                        FROM graph_similarity s
                        JOIN constraints c ON c.constraint_prefix = s.prefix_from AND c.constraint_id = s.concept_from
                        WHERE s.prefix_to = :original_prefix AND s.concept_to = q.original_id
                            AND s.score >= :threshold
                    ) m ON true
                    GROUP BY q.original_id, m.other_prefix, m.other_id
                """).bindparams(
                    _array_param('original_ids', original_ids),
                    _array_param('constraint_prefixes', constraint_prefixes),
                    _array_param('constraint_concept_ids', constraint_concept_ids),
                ), {'original_prefix': original_prefix.value, 'threshold': threshold})

                for row in result:
                    by_id[row.original_id].append((row.other_id, row.other_prefix, row.best_score))

        for cid in original_ids:
            translations = sorted(by_id[cid], key=lambda t: t[2], reverse=True)
            if limit is not None:
                translations = translations[:limit]
            for other_id, other_prefix, score in translations:
                yield TranslatedTerm(conceptId=other_id, prefix=other_prefix, score=score)

    @property
    def reactome(self) -> PostgresReactomeRepository:
        """
        Get the Reactome repository interface.
        :return: PostgresReactomeRepository instance.
        """
        return PostgresReactomeRepository(engine=self.engine)
