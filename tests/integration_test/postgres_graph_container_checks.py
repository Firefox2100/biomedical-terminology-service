"""
Integration test suite for PostgresGraphDatabase, run against a real, ephemeral PostgreSQL
container via testcontainers. Given how much of this driver is raw SQL (recursive CTEs, list
partitioning, array binding), a mocked unit test would only prove the query text has the right
shape, not that PostgreSQL actually accepts and correctly executes it -- this is what actually
validates it.

Like the other tests/integration_test modules, this tier is intentionally NOT named
test_*.py/*_test.py, so a bare `pytest` run does not pick it up. Run it explicitly:

    pytest tests/integration_test/postgres_graph_container_checks.py -v

Requires: a working Docker daemon reachable from this host.
"""
import os

import networkx as nx
import pytest
import pytest_asyncio

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from bioterms.database.graph_db.postgres_graph_db import PostgresGraphDatabase
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import (
    AnnotationType,
    ConceptPrefix,
    ConceptRelationshipType,
    ConceptType,
    SimilarityMethod,
)
from bioterms.model.annotation import Annotation
from bioterms.model.concept.concept import Concept, GRAPH_NODE_EXTRA_PROPERTIES, \
    GRAPH_NODE_EXTRA_PROPERTY_COLUMNS
from bioterms.model.concept import OhdsiConcept, UniProtConcept

POSTGRES_IMAGE = 'postgres:18.6'


def _docker_available() -> bool:
    try:
        import docker
        client = docker.from_env()
        try:
            client.ping()
            return True
        finally:
            client.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason='Docker is not available in this environment')


@pytest.fixture(scope='session')
def postgres_container():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(image=POSTGRES_IMAGE, driver='asyncpg') as container:
        yield container


@pytest_asyncio.fixture
async def graph_db(postgres_container):
    engine = create_async_engine(postgres_container.get_connection_url())

    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))
        for (table_name,) in result:
            await conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

    db = PostgresGraphDatabase(engine=engine)
    try:
        yield db
    finally:
        await db.close()


def make_concept(prefix: ConceptPrefix, concept_id: str, types: list[ConceptType] = None) -> Concept:
    return Concept(prefix=prefix, conceptId=concept_id, conceptTypes=types or [])


def hpo_hierarchy_graph() -> nx.MultiDiGraph:
    """
    child -is_a-> parent, a small diamond: leaf -> mid1 -> root, leaf -> mid2 -> root.
    """
    g = nx.MultiDiGraph()
    g.add_edge('HP:leaf', 'HP:mid1', key='is_a', label=ConceptRelationshipType.IS_A)
    g.add_edge('HP:leaf', 'HP:mid2', key='is_a', label=ConceptRelationshipType.IS_A)
    g.add_edge('HP:mid1', 'HP:root', key='is_a', label=ConceptRelationshipType.IS_A)
    g.add_edge('HP:mid2', 'HP:root', key='is_a', label=ConceptRelationshipType.IS_A)
    return g


@pytest.mark.asyncio
async def test_save_and_get_vocabulary_graph(graph_db):
    concepts = [
        make_concept(ConceptPrefix.HPO, 'HP:leaf'),
        make_concept(ConceptPrefix.HPO, 'HP:mid1'),
        make_concept(ConceptPrefix.HPO, 'HP:mid2'),
        make_concept(ConceptPrefix.HPO, 'HP:root'),
    ]
    await graph_db.save_vocabulary_graph(concepts, hpo_hierarchy_graph())

    assert await graph_db.count_terms(ConceptPrefix.HPO) == 4
    assert await graph_db.count_internal_relationships(ConceptPrefix.HPO) == 4

    g = await graph_db.get_vocabulary_graph(ConceptPrefix.HPO)
    assert set(g.nodes) == {'HP:leaf', 'HP:mid1', 'HP:mid2', 'HP:root'}
    assert g.has_edge('HP:leaf', 'HP:mid1')
    assert g.has_edge('HP:mid1', 'HP:root')


@pytest.mark.asyncio
async def test_save_vocabulary_graph_writes_extra_properties(graph_db):
    ohdsi_concepts = [
        OhdsiConcept(prefix=ConceptPrefix.OHDSI, conceptId='1', sourceVocabularyId='SNOMED'),
        OhdsiConcept(prefix=ConceptPrefix.OHDSI, conceptId='2'),
    ]
    ohdsi_graph = nx.MultiDiGraph()
    for c in ohdsi_concepts:
        ohdsi_graph.add_node(c.concept_id)
    await graph_db.save_vocabulary_graph(ohdsi_concepts, ohdsi_graph)

    uniprot_concepts = [
        UniProtConcept(
            prefix=ConceptPrefix.UNIPROT, conceptId='P68104',
            reviewed=True, organismTaxId='9606', organismName='Homo sapiens',
        ),
    ]
    uniprot_graph = nx.MultiDiGraph()
    uniprot_graph.add_node('P68104')
    await graph_db.save_vocabulary_graph(uniprot_concepts, uniprot_graph)

    async with graph_db.engine.connect() as conn:
        result = await conn.execute(text(
            'SELECT concept_id, source_vocabulary_id, reviewed, organism_tax_id, organism_name '
            'FROM graph_node_ohdsi ORDER BY concept_id'
        ))
        rows = {row.concept_id: row for row in result}
        assert rows['1'].source_vocabulary_id == 'SNOMED'
        assert rows['1'].reviewed is None
        assert rows['2'].source_vocabulary_id is None

        result = await conn.execute(text(
            'SELECT reviewed, organism_tax_id, organism_name FROM graph_node_uniprot WHERE concept_id = :id'
        ), {'id': 'P68104'})
        row = result.one()
        assert row.reviewed is True
        assert row.organism_tax_id == '9606'
        assert row.organism_name == 'Homo sapiens'


@pytest.mark.asyncio
async def test_extra_property_columns_are_indexed(graph_db):
    await graph_db.save_vocabulary_graph(
        [make_concept(ConceptPrefix.HPO, 'HP:leaf')],
        nx.MultiDiGraph(),
    )

    async with graph_db.engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'graph_node_hpo'"
        ))
        index_names = {row.indexname for row in result}

    for prop in GRAPH_NODE_EXTRA_PROPERTIES:
        column = GRAPH_NODE_EXTRA_PROPERTY_COLUMNS[prop]
        assert f'ix_graph_node_hpo_{column}' in index_names


@pytest.mark.asyncio
async def test_ensure_prefix_schema_migrates_pre_existing_table(graph_db):
    # Simulate a table created before GRAPH_NODE_EXTRA_PROPERTIES existed.
    async with graph_db.engine.begin() as conn:
        await conn.execute(text(
            "CREATE TABLE graph_node_mondo (concept_id TEXT PRIMARY KEY, types TEXT[] NOT NULL DEFAULT '{}')"
        ))

    async with graph_db.engine.begin() as conn:
        await graph_db._ensure_prefix_schema(conn, ConceptPrefix.MONDO)

    async with graph_db.engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'graph_node_mondo'"
        ))
        columns = {row.column_name for row in result}

    for prop in GRAPH_NODE_EXTRA_PROPERTIES:
        assert GRAPH_NODE_EXTRA_PROPERTY_COLUMNS[prop] in columns


@pytest.mark.asyncio
async def test_save_vocabulary_graph_creates_bare_nodes_for_edge_only_endpoints(graph_db):
    # HP:orphan_parent only appears as an edge endpoint, not in the concepts list -- mirrors
    # Neo4j's implicit MERGE-creates-missing-node behaviour.
    concepts = [make_concept(ConceptPrefix.HPO, 'HP:child')]
    g = nx.MultiDiGraph()
    g.add_edge('HP:child', 'HP:orphan_parent', key='is_a', label=ConceptRelationshipType.IS_A)

    await graph_db.save_vocabulary_graph(concepts, g)

    assert await graph_db.count_terms(ConceptPrefix.HPO) == 2


@pytest.mark.asyncio
async def test_closure_ancestors_and_descendants(graph_db):
    concepts = [
        make_concept(ConceptPrefix.HPO, cid)
        for cid in ('HP:leaf', 'HP:mid1', 'HP:mid2', 'HP:root')
    ]
    await graph_db.save_vocabulary_graph(concepts, hpo_hierarchy_graph())
    await graph_db.create_index()

    ancestors = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf', 'HP:root'])
    by_id = {a.concept_id: set(a.related_concepts) for a in ancestors}
    assert by_id['HP:leaf'] == {'HP:mid1', 'HP:mid2', 'HP:root'}
    assert by_id['HP:root'] == set()

    descendants = await graph_db.expand_terms(ConceptPrefix.HPO, ['HP:root', 'HP:leaf'])
    by_id = {d.concept_id: set(d.related_concepts) for d in descendants}
    assert by_id['HP:root'] == {'HP:mid1', 'HP:mid2', 'HP:leaf'}
    assert by_id['HP:leaf'] == set()

    # Bounded depth: leaf -> mid1/mid2 only (depth 1), root not included.
    bounded = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf'], max_depth=1)
    assert set(bounded[0].related_concepts) == {'HP:mid1', 'HP:mid2'}

    # Per-term limit.
    limited = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf'], limit=1)
    assert len(limited[0].related_concepts) == 1


@pytest.mark.asyncio
async def test_closure_handles_densely_polyhierarchical_graph(graph_db):
    """
    A small fully-connected bipartite chain (every node in layer N is_a every node in
    layer N+1) -- the shape of graph that made a naive `UNION ALL` recursive-CTE closure
    combinatorially enumerate every path before deduplicating, rather than each
    (ancestor, descendant) pair once. Correctness (not just that it doesn't blow up) is what
    matters here: every node in an earlier layer must reach every node in every later layer,
    each exactly once, at the depth of the layer gap between them.
    """
    layers, width = 4, 5
    concepts = [
        make_concept(ConceptPrefix.HPO, f'L{layer}_{i}')
        for layer in range(layers)
        for i in range(width)
    ]
    g = nx.MultiDiGraph()
    for layer in range(layers - 1):
        for i in range(width):
            for j in range(width):
                g.add_edge(f'L{layer}_{i}', f'L{layer + 1}_{j}', key='is_a', label=ConceptRelationshipType.IS_A)

    await graph_db.save_vocabulary_graph(concepts, g)

    # Layer 0 nodes should have every node in layers 1-3 as an ancestor (is_a points from
    # child to parent, so "ancestor" here means everything reachable by following is_a
    # forward): width * (layers - 1) = 15 ancestors for an L0 node.
    ancestors = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['L0_0'])
    assert len(ancestors[0].related_concepts) == width * (layers - 1)

    # A last-layer node has no ancestors at all.
    ancestors_last = await graph_db.trace_ancestors(ConceptPrefix.HPO, [f'L{layers - 1}_0'])
    assert ancestors_last[0].related_concepts == []

    # Bounded to depth 1, an L0 node's ancestors are exactly layer 1 (not layers 2-3).
    bounded = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['L0_0'], max_depth=1)
    assert set(bounded[0].related_concepts) == {f'L1_{i}' for i in range(width)}


@pytest.mark.asyncio
async def test_closure_depth_cap_falls_back_to_live_traversal_beyond_it(graph_db, monkeypatch):
    """
    With `postgres_graph_closure_depth` set below the chain's real depth, the closure table
    itself must only contain pairs up to that cap -- but ancestor/descendant queries deeper than
    the cap, or unbounded, must still return the full, correct answer via the live traversal
    fallback in `_closure_lookup`. A query fully within the cap must still hit the closure table
    (same correctness expectation, just a different code path).
    """
    monkeypatch.setattr(CONFIG, 'postgres_graph_closure_depth', 2)

    # A 5-node straight chain, depth 4 end-to-end: n0 -is_a-> n1 -is_a-> ... -is_a-> n4.
    concepts = [make_concept(ConceptPrefix.HPO, f'n{i}') for i in range(5)]
    g = nx.MultiDiGraph()
    for i in range(4):
        g.add_edge(f'n{i}', f'n{i + 1}', key='is_a', label=ConceptRelationshipType.IS_A)

    await graph_db.save_vocabulary_graph(concepts, g)

    engine = graph_db.engine
    async with engine.connect() as conn:
        max_depth_in_table = await conn.scalar(text('SELECT max(depth) FROM graph_closure_hpo'))
    assert max_depth_in_table == 2, 'closure table should stop materialising at the configured depth cap'

    # Within the cap: served by the closure table, must still be correct.
    bounded = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['n0'], max_depth=2)
    assert set(bounded[0].related_concepts) == {'n1', 'n2'}

    # Deeper than the cap: closure table alone would be missing n3/n4, live fallback must
    # supply the full correct answer.
    deeper = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['n0'], max_depth=4)
    assert set(deeper[0].related_concepts) == {'n1', 'n2', 'n3', 'n4'}

    # Unbounded: same expectation, via the live fallback's safety-bound depth.
    unbounded = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['n0'])
    assert set(unbounded[0].related_concepts) == {'n1', 'n2', 'n3', 'n4'}

    # Descendants direction, deeper than the cap, from the other end of the chain.
    descendants = await graph_db.expand_terms(ConceptPrefix.HPO, ['n4'], max_depth=4)
    assert set(descendants[0].related_concepts) == {'n0', 'n1', 'n2', 'n3'}

    # A limit beyond the cap still caps the number of results returned, via the live fallback.
    limited = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['n0'], max_depth=4, limit=1)
    assert len(limited[0].related_concepts) == 1


@pytest.mark.asyncio
async def test_save_vocabulary_graph_keeps_nodes_and_edges_if_closure_build_fails(graph_db, monkeypatch):
    """
    Nodes and edges are committed as their own batches, ahead of the final closure rebuild --
    a failure in the closure step (which is what actually happened against a real OHDSI-sized
    graph: the old closure query exhausted the server's temp-file disk) must not roll back the
    node/edge data that was already saved, so a retry only has to redo the closure, not
    potentially hours of node/edge upserts.
    """
    async def failing_build_closure(self, conn, p):
        raise RuntimeError('simulated closure failure')

    monkeypatch.setattr(type(graph_db), '_build_closure', failing_build_closure)

    concepts = [make_concept(ConceptPrefix.HPO, cid) for cid in ('HP:leaf', 'HP:mid1', 'HP:mid2', 'HP:root')]

    with pytest.raises(RuntimeError, match='simulated closure failure'):
        await graph_db.save_vocabulary_graph(concepts, hpo_hierarchy_graph())

    assert await graph_db.count_terms(ConceptPrefix.HPO) == 4

    graph = await graph_db.get_vocabulary_graph(ConceptPrefix.HPO)
    assert graph.number_of_edges() == 4

    # Retrying (without the monkeypatched failure) succeeds and only needed to redo the
    # closure -- the nodes/edges from the failed attempt are reused via ON CONFLICT.
    monkeypatch.undo()
    await graph_db.save_vocabulary_graph(concepts, hpo_hierarchy_graph())
    ancestors = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf'])
    assert set(ancestors[0].related_concepts) == {'HP:mid1', 'HP:mid2', 'HP:root'}


@pytest.mark.asyncio
async def test_get_replaced_and_replacing_terms(graph_db):
    concepts = [make_concept(ConceptPrefix.HPO, cid) for cid in ('HP:old', 'HP:new')]
    g = nx.MultiDiGraph()
    g.add_edge('HP:old', 'HP:new', key='replaced_by', label=ConceptRelationshipType.REPLACED_BY)
    await graph_db.save_vocabulary_graph(concepts, g)

    replacing = await graph_db.get_replacing_terms(ConceptPrefix.HPO, ['HP:old'])
    assert replacing[0].related_concepts == ['HP:new']

    replaced = await graph_db.get_replaced_terms(ConceptPrefix.HPO, ['HP:new'])
    assert replaced[0].related_concepts == ['HP:old']

    # A term with no replacement relationship still yields an (empty) RelatedTerm.
    none_result = await graph_db.get_replacing_terms(ConceptPrefix.HPO, ['HP:new'])
    assert none_result[0].related_concepts == []


@pytest.mark.asyncio
async def test_get_relationship_edges_filters_by_type(graph_db):
    concepts = [make_concept(ConceptPrefix.HPO, cid) for cid in ('HP:a', 'HP:b', 'HP:c', 'HP:d')]
    g = nx.MultiDiGraph()
    g.add_edge('HP:a', 'HP:b', key='replaced_by', label=ConceptRelationshipType.REPLACED_BY)
    g.add_edge('HP:c', 'HP:d', key='replaced_by', label=ConceptRelationshipType.REPLACED_BY)
    g.add_edge('HP:a', 'HP:c', key='is_a', label=ConceptRelationshipType.IS_A)
    await graph_db.save_vocabulary_graph(concepts, g)

    edges = [e async for e in graph_db.get_relationship_edges(ConceptPrefix.HPO, ConceptRelationshipType.REPLACED_BY)]
    assert sorted(edges) == [('HP:a', 'HP:b'), ('HP:c', 'HP:d')]

    is_a_edges = [e async for e in graph_db.get_relationship_edges(ConceptPrefix.HPO, ConceptRelationshipType.IS_A)]
    assert is_a_edges == [('HP:a', 'HP:c')]

    part_of_edges = [e async for e in graph_db.get_relationship_edges(ConceptPrefix.HPO, ConceptRelationshipType.PART_OF)]
    assert part_of_edges == []


@pytest.mark.asyncio
async def test_delete_vocabulary_graph(graph_db):
    concepts = [make_concept(ConceptPrefix.HPO, 'HP:leaf')]
    await graph_db.save_vocabulary_graph(concepts, nx.MultiDiGraph())
    assert await graph_db.count_terms(ConceptPrefix.HPO) == 1

    await graph_db.delete_vocabulary_graph(ConceptPrefix.HPO)
    assert await graph_db.count_terms(ConceptPrefix.HPO) == 0

    # Re-saving after delete must work (tables get recreated).
    await graph_db.save_vocabulary_graph(concepts, nx.MultiDiGraph())
    assert await graph_db.count_terms(ConceptPrefix.HPO) == 1


@pytest.mark.asyncio
async def test_annotations(graph_db):
    annotations = [
        Annotation(
            prefixFrom=ConceptPrefix.HGNC, conceptIdFrom='HGNC:1',
            prefixTo=ConceptPrefix.MONDO, conceptIdTo='MONDO:1',
            annotationType=AnnotationType.EXACT,
        ),
        Annotation(
            prefixFrom=ConceptPrefix.HGNC, conceptIdFrom='HGNC:2',
            prefixTo=ConceptPrefix.MONDO, conceptIdTo='MONDO:2',
            annotationType=AnnotationType.BROAD,
        ),
    ]
    await graph_db.save_annotations(annotations)

    assert await graph_db.count_annotations(ConceptPrefix.HGNC, ConceptPrefix.MONDO) == 2

    g = await graph_db.get_annotation_graph(ConceptPrefix.HGNC, ConceptPrefix.MONDO)
    assert g.has_edge('hgnc:HGNC:1', 'mondo:MONDO:1')

    await graph_db.delete_annotations(ConceptPrefix.HGNC, ConceptPrefix.MONDO)
    assert await graph_db.count_annotations(ConceptPrefix.HGNC, ConceptPrefix.MONDO) == 0


@pytest.mark.asyncio
async def test_map_terms_multi_hop(graph_db):
    # HGNC:1 -exact-> MONDO:1 -broad-> SNOMED:1 : a 2-hop mapping HGNC -> SNOMED.
    await graph_db.save_annotations([
        Annotation(
            prefixFrom=ConceptPrefix.HGNC, conceptIdFrom='HGNC:1',
            prefixTo=ConceptPrefix.MONDO, conceptIdTo='MONDO:1',
            annotationType=AnnotationType.EXACT,
        ),
        Annotation(
            prefixFrom=ConceptPrefix.MONDO, conceptIdFrom='MONDO:1',
            prefixTo=ConceptPrefix.SNOMED, conceptIdTo='SNOMED:1',
            annotationType=AnnotationType.BROAD,
        ),
    ])

    one_hop = await graph_db.map_terms(ConceptPrefix.HGNC, ConceptPrefix.SNOMED, ['HGNC:1'], max_hops=1)
    assert one_hop[0].related_concepts == []

    two_hop = await graph_db.map_terms(ConceptPrefix.HGNC, ConceptPrefix.SNOMED, ['HGNC:1'], max_hops=2)
    assert two_hop[0].related_concepts == ['SNOMED:1']

    # Undirected: also works from MONDO looking back to HGNC.
    to_hgnc = await graph_db.map_terms(ConceptPrefix.MONDO, ConceptPrefix.HGNC, ['MONDO:1'], max_hops=1)
    assert to_hgnc[0].related_concepts == ['HGNC:1']


@pytest.mark.asyncio
async def test_similarity_scores_and_lookups(graph_db):
    await graph_db.save_similarity_scores(
        ConceptPrefix.HPO, ConceptPrefix.HPO,
        [('HP:1', 'HP:2', 0.9), ('HP:1', 'HP:3', 0.5)],
        SimilarityMethod.CO_ANNOTATION,
    )
    await graph_db.save_similarity_scores(
        ConceptPrefix.HPO, ConceptPrefix.MONDO,
        [('HP:1', 'MONDO:9', 0.8)],
        SimilarityMethod.RELEVANCE,
        corpus_prefix=ConceptPrefix.HGNC,
    )

    counts = await graph_db.count_similarity_relationships(
        ConceptPrefix.HPO, ConceptPrefix.HPO,
        [(SimilarityMethod.CO_ANNOTATION, None), (SimilarityMethod.RELEVANCE, None)],
    )
    counts_by_method = {m: (c, n) for m, c, n in counts}
    assert counts_by_method[SimilarityMethod.CO_ANNOTATION] == (None, 2)
    assert counts_by_method[SimilarityMethod.RELEVANCE] == (None, 0)

    same_prefix = await graph_db.get_similar_terms(ConceptPrefix.HPO, ['HP:1'], threshold=0.6)
    assert len(same_prefix) == 1
    assert same_prefix[0].similar_groups[0].prefix == ConceptPrefix.HPO
    ids = {c.concept_id for c in same_prefix[0].similar_groups[0].similar_concepts}
    assert ids == {'HP:2'}  # HP:3 (0.5) filtered out by threshold

    cross_prefix = await graph_db.get_similar_terms(ConceptPrefix.HPO, ['HP:1'], threshold=0.1, same_prefix=False)
    prefixes = {g.prefix for g in cross_prefix[0].similar_groups}
    assert ConceptPrefix.MONDO in prefixes

    aggregate = await graph_db.get_similar_terms_aggregate(ConceptPrefix.HPO, [('HP:1', 0.1)])
    aggregate_ids = {cid for cid, _ in aggregate[0].similar_concepts}
    assert aggregate_ids == {'HP:2', 'HP:3'}


@pytest.mark.asyncio
async def test_translate_terms(graph_db):
    await graph_db.save_similarity_scores(
        ConceptPrefix.HPO, ConceptPrefix.MONDO,
        [('HP:1', 'MONDO:1', 0.9), ('HP:1', 'MONDO:2', 0.3)],
        SimilarityMethod.CO_ANNOTATION,
    )

    translated = []
    async for t in graph_db.translate_terms_iter(
        original_ids=['HP:1'],
        original_prefix=ConceptPrefix.HPO,
        constraint_ids={ConceptPrefix.MONDO: {'MONDO:1', 'MONDO:2'}},
        threshold=0.5,
    ):
        translated.append(t)

    assert len(translated) == 1
    assert translated[0].concept_id == 'MONDO:1'
    assert translated[0].score == pytest.approx(0.9)


@pytest.mark.asyncio
async def test_trace_term_internal_relationship(graph_db):
    concepts = [
        make_concept(ConceptPrefix.HPO, cid)
        for cid in ('HP:leaf', 'HP:mid1', 'HP:mid2', 'HP:root')
    ]
    await graph_db.save_vocabulary_graph(concepts, hpo_hierarchy_graph())

    paths = []
    async for path in graph_db.trace_term_iter(
        prefix_start=ConceptPrefix.HPO, prefix_end=ConceptPrefix.HPO,
        id_start='HP:leaf', id_end='HP:root',
        relationship_type=ConceptRelationshipType.IS_A, forward=True, max_depth=5,
    ):
        paths.append(path)

    assert len(paths) == 2  # via mid1 and via mid2
    for p in paths:
        assert p.nodes[0].concept_id == 'HP:leaf'
        assert p.nodes[-1].concept_id == 'HP:root'
        assert p.length == 3

    # Different prefixes with an internal-only relationship type: no path possible.
    no_paths = []
    async for path in graph_db.trace_term_iter(
        prefix_start=ConceptPrefix.HPO, prefix_end=ConceptPrefix.MONDO,
        id_start='HP:leaf', id_end='MONDO:1',
        relationship_type=ConceptRelationshipType.IS_A, forward=True, max_depth=5,
    ):
        no_paths.append(path)
    assert no_paths == []


@pytest.mark.asyncio
async def test_trace_term_annotation_relationship_shortest(graph_db):
    await graph_db.save_annotations([
        Annotation(
            prefixFrom=ConceptPrefix.HGNC, conceptIdFrom='HGNC:1',
            prefixTo=ConceptPrefix.MONDO, conceptIdTo='MONDO:1',
            annotationType=AnnotationType.EXACT,
        ),
        Annotation(
            prefixFrom=ConceptPrefix.MONDO, conceptIdFrom='MONDO:1',
            prefixTo=ConceptPrefix.SNOMED, conceptIdTo='SNOMED:1',
            annotationType=AnnotationType.EXACT,
        ),
    ])

    paths = []
    async for path in graph_db.trace_term_iter(
        prefix_start=ConceptPrefix.HGNC, prefix_end=ConceptPrefix.SNOMED,
        id_start='HGNC:1', id_end='SNOMED:1',
        relationship_type=AnnotationType.EXACT, forward=None, max_depth=5,
    ):
        paths.append(path)

    assert len(paths) == 1
    assert [n.concept_id for n in paths[0].nodes] == ['HGNC:1', 'MONDO:1', 'SNOMED:1']


@pytest.mark.asyncio
async def test_trace_term_aggregate(graph_db):
    concepts = [
        make_concept(ConceptPrefix.HPO, cid)
        for cid in ('HP:leaf', 'HP:mid1', 'HP:root')
    ]
    g = nx.MultiDiGraph()
    g.add_edge('HP:leaf', 'HP:mid1', key='is_a', label=ConceptRelationshipType.IS_A)
    g.add_edge('HP:mid1', 'HP:root', key='is_a', label=ConceptRelationshipType.IS_A)
    await graph_db.save_vocabulary_graph(concepts, g)

    results = await graph_db.trace_term_aggregate([
        (ConceptPrefix.HPO, 'HP:leaf', ConceptPrefix.HPO, 'HP:root',
         ConceptRelationshipType.IS_A, True, 5),
    ])
    assert len(results) == 1
    assert results[0].length == 3


@pytest.mark.asyncio
async def test_reactome_repository(graph_db):
    concepts = [
        make_concept(ConceptPrefix.REACTOME, 'R-super', [ConceptType.PATHWAY]),
        make_concept(ConceptPrefix.REACTOME, 'R-sub', [ConceptType.PATHWAY]),
        make_concept(ConceptPrefix.REACTOME, 'R-reaction1', [ConceptType.REACTION]),
        make_concept(ConceptPrefix.REACTOME, 'R-reaction2', [ConceptType.REACTION]),
        make_concept(ConceptPrefix.REACTOME, 'R-gene1', [ConceptType.GENE]),
    ]
    g = nx.MultiDiGraph()
    g.add_edge('R-sub', 'R-super', key='part_of', label=ConceptRelationshipType.PART_OF)
    g.add_edge('R-reaction1', 'R-sub', key='part_of', label=ConceptRelationshipType.PART_OF)
    g.add_edge('R-reaction2', 'R-reaction1', key='preceded_by', label=ConceptRelationshipType.PRECEDED_BY)
    g.add_edge('R-reaction1', 'R-gene1', key='has_input', label=ConceptRelationshipType.HAS_INPUT)
    g.add_edge('R-reaction1', 'R-gene1', key='has_output', label=ConceptRelationshipType.HAS_OUTPUT)
    await graph_db.save_vocabulary_graph(concepts, g)

    reactome = graph_db.reactome

    sub = await reactome.get_sub_pathways(['R-super'])
    assert sub[0].related_concepts == ['R-sub']

    sup = await reactome.get_super_pathways(['R-sub'])
    assert sup[0].related_concepts == ['R-super']

    reactions = await reactome.get_reactions_in_pathway(['R-sub'])
    assert reactions[0].related_concepts == ['R-reaction1']

    pathways = await reactome.get_pathways_of_reaction(['R-reaction1'])
    assert pathways[0].related_concepts == ['R-sub']

    preceding = await reactome.get_preceding_reactions(['R-reaction2'])
    assert preceding[0].related_concepts == ['R-reaction1']

    subsequent = await reactome.get_subsequent_reactions(['R-reaction1'])
    assert subsequent[0].related_concepts == ['R-reaction2']

    inputs = await reactome.get_reaction_inputs(['R-reaction1'])
    assert inputs[0].related_concepts == ['R-gene1']

    outputs = await reactome.get_reaction_outputs(['R-reaction1'])
    assert outputs[0].related_concepts == ['R-gene1']

    input_reactions = await reactome.get_gene_input_reactions(['R-gene1'])
    assert input_reactions[0].related_concepts == ['R-reaction1']

    output_reactions = await reactome.get_gene_output_reactions(['R-gene1'])
    assert output_reactions[0].related_concepts == ['R-reaction1']

    # A pathway_id that doesn't exist / isn't a pathway is simply omitted.
    missing = await reactome.get_sub_pathways(['does-not-exist'])
    assert missing == []


@pytest.mark.asyncio
async def test_count_similarity_relationships_with_corpus(graph_db):
    await graph_db.save_similarity_scores(
        ConceptPrefix.HPO, ConceptPrefix.MONDO,
        [('HP:1', 'MONDO:9', 0.8)],
        SimilarityMethod.RELEVANCE,
        corpus_prefix=ConceptPrefix.HGNC,
    )

    counts = await graph_db.count_similarity_relationships(
        ConceptPrefix.HPO, ConceptPrefix.MONDO,
        [
            (SimilarityMethod.RELEVANCE, ConceptPrefix.HGNC),
            (SimilarityMethod.RELEVANCE, ConceptPrefix.SNOMED),  # wrong corpus -> 0
            (SimilarityMethod.RELEVANCE, None),  # no corpus at all -> 0 (this one has one)
        ],
    )
    by_corpus = {corpus: n for _, corpus, n in counts}
    assert by_corpus[ConceptPrefix.HGNC] == 1
    assert by_corpus[ConceptPrefix.SNOMED] == 0
    assert by_corpus[None] == 0


@pytest.mark.asyncio
async def test_count_on_nonexistent_prefix_returns_zero(graph_db):
    assert await graph_db.count_terms(ConceptPrefix.SNOMED) == 0
    assert await graph_db.count_internal_relationships(ConceptPrefix.SNOMED) == 0
    assert await graph_db.count_annotations(ConceptPrefix.SNOMED, ConceptPrefix.MONDO) == 0


@pytest.mark.asyncio
async def test_trace_ancestors_works_without_calling_create_index(graph_db):
    # save_vocabulary_graph must rebuild the closure table itself: vocabulary.create_indexes()
    # calls create_index() *before* loading a vocabulary's data (fine for Neo4j's structural,
    # order-independent index DDL, but not for a closure table that needs edges to exist), so
    # ancestor/descendant lookups must already work right after save_vocabulary_graph alone.
    concepts = [make_concept(ConceptPrefix.HPO, cid) for cid in ('HP:leaf', 'HP:root')]
    g = nx.MultiDiGraph()
    g.add_edge('HP:leaf', 'HP:root', key='is_a', label=ConceptRelationshipType.IS_A)
    await graph_db.save_vocabulary_graph(concepts, g)

    ancestors = await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf'])
    assert ancestors[0].related_concepts == ['HP:root']


@pytest.mark.asyncio
async def test_refresh_closure_table_after_manual_edge_change(graph_db):
    concepts = [make_concept(ConceptPrefix.HPO, cid) for cid in ('HP:leaf', 'HP:root', 'HP:newer_root')]
    g = nx.MultiDiGraph()
    g.add_edge('HP:leaf', 'HP:root', key='is_a', label=ConceptRelationshipType.IS_A)
    await graph_db.save_vocabulary_graph(concepts, g)
    assert (await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf']))[0].related_concepts == ['HP:root']

    # Simulate a hierarchy edge added some other way than save_vocabulary_graph.
    async with graph_db.engine.begin() as conn:
        await conn.execute(text(
            "INSERT INTO graph_edge_hpo (source_id, target_id, rel_type) VALUES ('HP:root', 'HP:newer_root', 'is_a')"
        ))

    # Not reflected yet...
    stale = (await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf']))[0].related_concepts
    assert 'HP:newer_root' not in stale

    # ...until the closure table is explicitly refreshed.
    await graph_db.refresh_closure_table(ConceptPrefix.HPO)
    fresh = (await graph_db.trace_ancestors(ConceptPrefix.HPO, ['HP:leaf']))[0].related_concepts
    assert set(fresh) == {'HP:root', 'HP:newer_root'}


@pytest.mark.asyncio
async def test_get_vocabulary_graph_with_similarity(graph_db):
    concepts = [make_concept(ConceptPrefix.HPO, cid) for cid in ('HP:1', 'HP:2')]
    await graph_db.save_vocabulary_graph(concepts, nx.MultiDiGraph())
    await graph_db.save_similarity_scores(
        ConceptPrefix.HPO, ConceptPrefix.HPO, [('HP:1', 'HP:2', 0.9)], SimilarityMethod.CO_ANNOTATION,
    )

    without_sim = await graph_db.get_vocabulary_graph(ConceptPrefix.HPO, with_similarity=False)
    assert not without_sim.has_edge('HP:1', 'HP:2')

    with_sim = await graph_db.get_vocabulary_graph(ConceptPrefix.HPO, with_similarity=True)
    assert with_sim.has_edge('HP:1', 'HP:2')
