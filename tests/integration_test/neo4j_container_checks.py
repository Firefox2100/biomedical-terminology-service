"""
Integration test suite for Neo4jGraphDatabase, run against a real, ephemeral Neo4j
container via testcontainers -- as opposed to tests/unit_test/database/test_neo4j_graph_db.py,
which only mocks the driver and checks the *shape* of the generated query text.

These tests exist specifically to validate the native-Cypher rewrite (removing all APOC
usage in favour of dynamic relationship types, CALL {...} IN TRANSACTIONS, quantified
path patterns / SHORTEST, and the native WHEN/THEN/ELSE conditional clause) actually
executes correctly against a real server -- deliberately WITHOUT the APOC plugin
installed, so a stray apoc.* call would fail loudly instead of silently.

Like tests/load_test, this tier is intentionally NOT named test_*.py/*_test.py, so a
bare `pytest` run does not pick it up (starting a container per run would be slow and
requires a working Docker daemon). Run it explicitly:

    pytest tests/integration_test/neo4j_container_checks.py -v

Requires: a working Docker daemon reachable from this host, and network access the
first time to pull the neo4j image (already present locally is fine/faster).
"""
import os

import networkx as nx
import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

from bioterms.database.graph_db.neo4j_graph_db import Neo4jGraphDatabase
from bioterms.etc.enums import (
    AnnotationType,
    ConceptPrefix,
    ConceptRelationshipType,
    ConceptType,
    SimilarityMethod,
)
from bioterms.model.annotation import Annotation
from bioterms.model.concept.concept import Concept

# Pinned to the version this repo actually deploys (see docker-compose.yaml / the running
# bts-neo4j dev container) rather than a floating `latest` tag, for reproducibility.
NEO4J_IMAGE = 'neo4j:2026.06.0'


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
def neo4j_container():
    from testcontainers.neo4j import Neo4jContainer

    with Neo4jContainer(image=NEO4J_IMAGE, password='test-password-1234') as container:
        yield container


@pytest_asyncio.fixture
async def graph_db(neo4j_container):
    """
    A Neo4jGraphDatabase backed by a fresh async driver against the shared container.

    The driver is opened/closed per test function (rather than shared at session scope)
    because pytest-asyncio's default (function-scoped) event loop would otherwise be
    reused across an AsyncDriver created in a different loop. The graph is wiped before
    each test for isolation; the container itself is only started once per session.
    """
    driver = AsyncGraphDatabase.driver(
        neo4j_container.get_connection_url(),
        auth=(neo4j_container.username, neo4j_container.password),
    )
    db = Neo4jGraphDatabase(client=driver)

    try:
        await db.create_index()
        async with driver.session() as session:
            await session.run('MATCH (n) DETACH DELETE n')

        yield db
    finally:
        await driver.close()


def make_concept(prefix: ConceptPrefix,
                 concept_id: str,
                 concept_type: ConceptType = ConceptType.GENE,
                 ) -> Concept:
    return Concept(
        conceptId=concept_id,
        prefix=prefix,
        label=f'{prefix.value}:{concept_id}',
        conceptTypes=[concept_type],
    )


def make_graph(*edges: tuple[str, str, ConceptRelationshipType]) -> nx.MultiDiGraph:
    graph = nx.MultiDiGraph()
    for source, target, label in edges:
        graph.add_edge(source, target, label=label)
    return graph


# ---------------------------------------------------------------------------------------
# save/get/delete vocabulary graph -- dynamic relationship types + CALL...IN TRANSACTIONS
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_and_get_vocabulary_graph_round_trip(graph_db):
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b', 'c')]
    graph = make_graph(
        ('a', 'b', ConceptRelationshipType.IS_A),
        ('b', 'c', ConceptRelationshipType.PART_OF),
    )

    await graph_db.save_vocabulary_graph(concepts, graph)

    result = await graph_db.get_vocabulary_graph(ConceptPrefix.MONDO)

    assert set(result.nodes) == {'a', 'b', 'c'}
    edges = {(u, v, data['label']) for u, v, data in result.edges(data=True)}
    assert edges == {
        ('a', 'b', ConceptRelationshipType.IS_A),
        ('b', 'c', ConceptRelationshipType.PART_OF),
    }

    assert await graph_db.count_terms(ConceptPrefix.MONDO) == 3
    assert await graph_db.count_internal_relationships(ConceptPrefix.MONDO) == 2


@pytest.mark.asyncio
async def test_delete_vocabulary_graph_removes_nodes_and_relationships(graph_db):
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b')]
    graph = make_graph(('a', 'b', ConceptRelationshipType.IS_A))
    await graph_db.save_vocabulary_graph(concepts, graph)

    assert await graph_db.count_terms(ConceptPrefix.MONDO) == 2

    await graph_db.delete_vocabulary_graph(ConceptPrefix.MONDO)

    assert await graph_db.count_terms(ConceptPrefix.MONDO) == 0
    assert await graph_db.count_internal_relationships(ConceptPrefix.MONDO) == 0


# ---------------------------------------------------------------------------------------
# annotations -- dynamic relationship type + SET rel += props
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_query_and_delete_annotations(graph_db):
    concepts_from = [make_concept(ConceptPrefix.HGNC, 'H1')]
    concepts_to = [make_concept(ConceptPrefix.MONDO, 'M1')]
    await graph_db.save_vocabulary_graph(concepts_from, nx.MultiDiGraph())
    await graph_db.save_vocabulary_graph(concepts_to, nx.MultiDiGraph())

    annotation = Annotation(
        conceptIdFrom='H1',
        prefixFrom=ConceptPrefix.HGNC,
        conceptIdTo='M1',
        prefixTo=ConceptPrefix.MONDO,
        annotationType=AnnotationType.EXACT,
        properties={'evidence': 'manual'},
    )
    await graph_db.save_annotations([annotation])

    assert await graph_db.count_annotations(ConceptPrefix.HGNC, ConceptPrefix.MONDO) == 1

    annotation_graph = await graph_db.get_annotation_graph(ConceptPrefix.HGNC, ConceptPrefix.MONDO)
    assert ('hgnc:H1', 'mondo:M1') in annotation_graph.edges
    edge_data = annotation_graph.edges['hgnc:H1', 'mondo:M1']
    assert edge_data['label'] == AnnotationType.EXACT
    assert edge_data['evidence'] == 'manual'
    exact_edges = [edge async for edge in graph_db.get_annotation_edges(
        ConceptPrefix.HGNC, ConceptPrefix.MONDO, AnnotationType.EXACT,
    )]
    assert exact_edges == [('hgnc', 'H1', 'mondo', 'M1', AnnotationType.EXACT)]

    await graph_db.delete_annotations(ConceptPrefix.HGNC, ConceptPrefix.MONDO)
    assert await graph_db.count_annotations(ConceptPrefix.HGNC, ConceptPrefix.MONDO) == 0


# ---------------------------------------------------------------------------------------
# similarity scores -- MERGE + SET rel[prop] (no dynamic type needed, but validates the
# apoc.merge.relationship removal didn't change the on-match/on-create semantics)
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_similarity_scores_and_count_by_method(graph_db):
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b')]
    await graph_db.save_vocabulary_graph(concepts, nx.MultiDiGraph())

    await graph_db.save_similarity_scores(
        ConceptPrefix.MONDO, ConceptPrefix.MONDO,
        [('a', 'b', 0.8)], SimilarityMethod.RELEVANCE,
    )

    counts = await graph_db.count_similarity_relationships(
        ConceptPrefix.MONDO, ConceptPrefix.MONDO,
        [(SimilarityMethod.RELEVANCE, None), (SimilarityMethod.CO_ANNOTATION, None)],
    )
    counts_by_method = {method: count for method, _, count in counts}
    assert counts_by_method[SimilarityMethod.RELEVANCE] == 1
    assert counts_by_method[SimilarityMethod.CO_ANNOTATION] == 0


# ---------------------------------------------------------------------------------------
# ancestor/descendant traversal -- native bounded/unbounded variable-length patterns
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trace_ancestors_bounded_vs_unbounded(graph_db):
    # a -is_a-> b -is_a-> c -is_a-> d
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b', 'c', 'd')]
    graph = make_graph(
        ('a', 'b', ConceptRelationshipType.IS_A),
        ('b', 'c', ConceptRelationshipType.IS_A),
        ('c', 'd', ConceptRelationshipType.IS_A),
    )
    await graph_db.save_vocabulary_graph(concepts, graph)

    bounded = await graph_db.trace_ancestors(ConceptPrefix.MONDO, ['a'], max_depth=2)
    assert set(bounded[0].related_concepts) == {'b', 'c'}

    unbounded = await graph_db.trace_ancestors(ConceptPrefix.MONDO, ['a'], max_depth=None)
    assert set(unbounded[0].related_concepts) == {'b', 'c', 'd'}


@pytest.mark.asyncio
async def test_expand_terms_bounded_vs_unbounded(graph_db):
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b', 'c', 'd')]
    graph = make_graph(
        ('a', 'b', ConceptRelationshipType.IS_A),
        ('b', 'c', ConceptRelationshipType.IS_A),
        ('c', 'd', ConceptRelationshipType.IS_A),
    )
    await graph_db.save_vocabulary_graph(concepts, graph)

    bounded = await graph_db.expand_terms(ConceptPrefix.MONDO, ['d'], max_depth=2)
    assert set(bounded[0].related_concepts) == {'b', 'c'}

    unbounded = await graph_db.expand_terms(ConceptPrefix.MONDO, ['d'], max_depth=None)
    assert set(unbounded[0].related_concepts) == {'a', 'b', 'c'}


# ---------------------------------------------------------------------------------------
# map_terms_iter -- target prefix bound directly on the pattern's end node, native
# distinctness check (no apoc.coll.toSet)
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_map_terms_respects_max_hops_and_finds_cross_vocabulary_target(graph_db):
    # hgnc:H1 -annotated_with-> gene:G1 -exact-> mondo:M1  (2 hops, 3 distinct prefixes)
    await graph_db.save_vocabulary_graph([make_concept(ConceptPrefix.HGNC, 'H1')], nx.MultiDiGraph())
    await graph_db.save_vocabulary_graph([make_concept(ConceptPrefix.HGNC_SYMBOL, 'G1')], nx.MultiDiGraph())
    await graph_db.save_vocabulary_graph([make_concept(ConceptPrefix.MONDO, 'M1')], nx.MultiDiGraph())

    await graph_db.save_annotations([
        Annotation(
            conceptIdFrom='H1', prefixFrom=ConceptPrefix.HGNC,
            conceptIdTo='G1', prefixTo=ConceptPrefix.HGNC_SYMBOL,
            annotationType=AnnotationType.ANNOTATED_WITH,
        ),
        Annotation(
            conceptIdFrom='G1', prefixFrom=ConceptPrefix.HGNC_SYMBOL,
            conceptIdTo='M1', prefixTo=ConceptPrefix.MONDO,
            annotationType=AnnotationType.EXACT,
        ),
    ])

    # A source with zero mappings within max_hops produces no row at all (an aggregation
    # with an implicit "group by src" over zero upstream rows yields zero groups) -- callers
    # are expected to default missing concept IDs to [], as ConceptLoaderByAnnotatedConcepts
    # does; map_terms() itself does not pad in a RelatedTerm(related_concepts=[]) entry.
    too_short = await graph_db.map_terms(
        ConceptPrefix.HGNC, ConceptPrefix.MONDO, ['H1'], max_hops=1,
    )
    assert too_short == []

    found = await graph_db.map_terms(
        ConceptPrefix.HGNC, ConceptPrefix.MONDO, ['H1'], max_hops=2,
    )
    assert found[0].related_concepts == ['M1']


# ---------------------------------------------------------------------------------------
# trace_term_iter -- SHORTEST 1 / directed quantified patterns / detour elimination
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trace_term_iter_forward_backward_and_undirected(graph_db):
    # a -is_a-> b -is_a-> c
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b', 'c')]
    graph = make_graph(
        ('a', 'b', ConceptRelationshipType.IS_A),
        ('b', 'c', ConceptRelationshipType.IS_A),
    )
    await graph_db.save_vocabulary_graph(concepts, graph)

    forward_paths = [
        p async for p in graph_db.trace_term_iter(
            ConceptPrefix.MONDO, ConceptPrefix.MONDO, 'a', 'c',
            ConceptRelationshipType.IS_A, forward=True, max_depth=5,
        )
    ]
    assert len(forward_paths) == 1
    assert [n.concept_id for n in forward_paths[0].nodes] == ['a', 'b', 'c']

    backward_paths = [
        p async for p in graph_db.trace_term_iter(
            ConceptPrefix.MONDO, ConceptPrefix.MONDO, 'a', 'c',
            ConceptRelationshipType.IS_A, forward=False, max_depth=5,
        )
    ]
    assert backward_paths == []  # edges only run a->b->c, not the reverse

    undirected_paths = [
        p async for p in graph_db.trace_term_iter(
            ConceptPrefix.MONDO, ConceptPrefix.MONDO, 'a', 'c',
            ConceptRelationshipType.IS_A, forward=None, max_depth=5,
        )
    ]
    assert len(undirected_paths) == 1
    assert [n.concept_id for n in undirected_paths[0].nodes] == ['a', 'b', 'c']


@pytest.mark.asyncio
async def test_trace_term_iter_eliminates_detour_paths(graph_db):
    # direct a-is_a->c, plus a longer detour a-is_a->x-is_a->c: the detour must be dropped
    # because the direct path's node sequence [a, c] is an ordered subsequence of [a, x, c].
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'c', 'x')]
    graph = make_graph(
        ('a', 'c', ConceptRelationshipType.IS_A),
        ('a', 'x', ConceptRelationshipType.IS_A),
        ('x', 'c', ConceptRelationshipType.IS_A),
    )
    await graph_db.save_vocabulary_graph(concepts, graph)

    paths = [
        p async for p in graph_db.trace_term_iter(
            ConceptPrefix.MONDO, ConceptPrefix.MONDO, 'a', 'c',
            ConceptRelationshipType.IS_A, forward=True, max_depth=5,
        )
    ]

    assert len(paths) == 1
    assert [n.concept_id for n in paths[0].nodes] == ['a', 'c']


# ---------------------------------------------------------------------------------------
# trace_term_aggregate_iter -- the highest-risk rewrite: native WHEN/THEN/ELSE inside a
# scoped CALL subquery, batching mixed forward=True/False/None and mixed max_depth in one
# UNWIND query.
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trace_term_aggregate_iter_batches_mixed_directions_and_depths(graph_db):
    # mondo: a -is_a-> b -is_a-> c        (used for forward=True and forward=False rows)
    # snomed: x -is_a-> y                  (used for the forward=None/shortest row)
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b', 'c')]
    concepts += [make_concept(ConceptPrefix.SNOMED, cid) for cid in ('x', 'y')]
    graph = make_graph(
        ('a', 'b', ConceptRelationshipType.IS_A),
        ('b', 'c', ConceptRelationshipType.IS_A),
    )
    await graph_db.save_vocabulary_graph(
        [c for c in concepts if c.prefix == ConceptPrefix.MONDO], graph,
    )
    await graph_db.save_vocabulary_graph(
        [c for c in concepts if c.prefix == ConceptPrefix.SNOMED],
        make_graph(('x', 'y', ConceptRelationshipType.IS_A)),
    )

    results = [
        p async for p in graph_db.trace_term_aggregate_iter([
            (ConceptPrefix.MONDO, 'a', ConceptPrefix.MONDO, 'c', ConceptRelationshipType.IS_A, True, 5),
            (ConceptPrefix.MONDO, 'a', ConceptPrefix.MONDO, 'c', ConceptRelationshipType.IS_A, False, 5),
            (ConceptPrefix.SNOMED, 'x', ConceptPrefix.SNOMED, 'y', ConceptRelationshipType.IS_A, None, 3),
        ])
    ]

    by_end = {(r.start_concept_id, r.end_concept_id, r.length): r for r in results}

    forward_hit = [r for r in results if r.start_concept_id == 'a' and r.end_concept_id == 'c']
    assert len(forward_hit) == 1
    assert [n.concept_id for n in forward_hit[0].nodes] == ['a', 'b', 'c']

    backward_hit = [r for r in results if r.start_concept_id == 'a' and r.end_concept_id == 'c'
                    and r is not forward_hit[0]]
    # forward=False for a->c should find nothing (no reverse edges), so only the
    # forward=True row's result should be present for (a, c)
    assert len(forward_hit) == 1 and len(results) == 2

    shortest_hit = [r for r in results if r.start_concept_id == 'x' and r.end_concept_id == 'y']
    assert len(shortest_hit) == 1
    assert [n.concept_id for n in shortest_hit[0].nodes] == ['x', 'y']


@pytest.mark.asyncio
async def test_trace_term_aggregate_iter_empty_batch(graph_db):
    results = [p async for p in graph_db.trace_term_aggregate_iter([])]
    assert results == []


# ---------------------------------------------------------------------------------------
# similarity queries -- properties(r) + reduce()-based max, map building moved to Python
# ---------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_similarity_queries_filter_by_threshold_and_method(graph_db):
    concepts = [make_concept(ConceptPrefix.MONDO, cid) for cid in ('a', 'b')]
    concepts.append(make_concept(ConceptPrefix.SNOMED, 's1'))
    await graph_db.save_vocabulary_graph(
        [c for c in concepts if c.prefix == ConceptPrefix.MONDO], nx.MultiDiGraph(),
    )
    await graph_db.save_vocabulary_graph(
        [c for c in concepts if c.prefix == ConceptPrefix.SNOMED], nx.MultiDiGraph(),
    )

    await graph_db.save_similarity_scores(
        ConceptPrefix.MONDO, ConceptPrefix.MONDO, [('a', 'b', 0.9)], SimilarityMethod.RELEVANCE,
    )
    await graph_db.save_similarity_scores(
        ConceptPrefix.MONDO, ConceptPrefix.MONDO, [('a', 'b', 0.4)],
        SimilarityMethod.CO_ANNOTATION, corpus_prefix=ConceptPrefix.MONDO,
    )
    await graph_db.save_similarity_scores(
        ConceptPrefix.MONDO, ConceptPrefix.SNOMED, [('a', 's1', 0.6)], SimilarityMethod.RELEVANCE,
    )

    # aggregate: only keys >= threshold count towards the highest score
    aggregate = await graph_db.get_similar_terms_aggregate(ConceptPrefix.MONDO, [('a', 0.5)])
    assert aggregate[0].concept_id == 'a'
    assert aggregate[0].similar_concepts == [('b', 0.9)]

    # cross-prefix, filtered to the 'relevance' method specifically (excludes the
    # co-annotation:mondo property even though it technically lives on the same edge)
    grouped = await graph_db.get_similar_terms(
        ConceptPrefix.MONDO, ['a'], threshold=0.3, same_prefix=False, method=SimilarityMethod.RELEVANCE,
    )
    groups_by_prefix = {g.prefix: g for g in grouped[0].similar_groups}
    assert groups_by_prefix[ConceptPrefix.MONDO].similar_concepts[0].similarity_scores == {'relevance': 0.9}
    assert groups_by_prefix[ConceptPrefix.SNOMED].similar_concepts[0].similarity_scores == {'relevance': 0.6}


@pytest.mark.asyncio
async def test_translate_terms_uses_reduce_for_max_score(graph_db):
    await graph_db.save_vocabulary_graph([make_concept(ConceptPrefix.MONDO, 'a')], nx.MultiDiGraph())
    await graph_db.save_vocabulary_graph([make_concept(ConceptPrefix.SNOMED, 's1')], nx.MultiDiGraph())

    await graph_db.save_similarity_scores(
        ConceptPrefix.MONDO, ConceptPrefix.SNOMED, [('a', 's1', 0.6)], SimilarityMethod.RELEVANCE,
    )

    translated = [
        t async for t in graph_db.translate_terms_iter(
            ['a'], ConceptPrefix.MONDO, {ConceptPrefix.SNOMED: {'s1'}}, threshold=0.3,
        )
    ]

    assert len(translated) == 1
    assert translated[0].concept_id == 's1'
    assert translated[0].prefix == ConceptPrefix.SNOMED
    assert translated[0].score == pytest.approx(0.6)
