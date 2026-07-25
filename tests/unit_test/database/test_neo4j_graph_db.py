import os

import pytest

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')

from bioterms.database.graph_db import neo4j_graph_db as m
from bioterms.database.graph_db.neo4j_graph_db import Neo4jGraphDatabase
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, SimilarityMethod


class FakeResult:
    """An async-iterable, empty query result."""

    def __aiter__(self):
        async def _empty():
            return
            yield  # pragma: no cover - never reached, makes this an async generator

        return _empty()

    async def single(self):
        return None


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeClient:
    def session(self):
        return FakeSession()


@pytest.fixture
def captured(monkeypatch):
    """Capture every (query, parameters) pair sent to the driver instead of executing it."""
    calls = []

    async def fake_execute(query, session, parameters=None, backoff_retries=3):
        calls.append((query, parameters or {}))
        return FakeResult()

    monkeypatch.setattr(m, '_execute_query_with_retry', fake_execute)
    return calls


@pytest.fixture
def db():
    return Neo4jGraphDatabase(client=FakeClient())


def assert_no_apoc(query: str):
    assert 'apoc' not in query.lower(), f'query still references APOC:\n{query}'


@pytest.mark.asyncio
async def test_trace_ancestors_bounded_uses_native_variable_length(db, captured):
    async for _ in db.trace_ancestors_iter(ConceptPrefix.MONDO, ['a'], max_depth=3, limit=5):
        pass

    query, params = captured[0]
    assert_no_apoc(query)
    assert '*1..3' in query
    assert 'depth' not in params  # depth is now baked into the query text, not a parameter


@pytest.mark.asyncio
async def test_expand_terms_bounded_uses_native_variable_length(db, captured):
    async for _ in db.expand_terms_iter(ConceptPrefix.MONDO, ['a'], max_depth=4):
        pass

    query, _ = captured[0]
    assert_no_apoc(query)
    assert '*1..4' in query


@pytest.mark.asyncio
async def test_map_terms_binds_target_prefix_and_checks_distinct_prefixes(db, captured):
    async for _ in db.map_terms_iter(
        ConceptPrefix.HGNC, ConceptPrefix.MONDO, ['A1'], max_hops=2, limit=10,
    ):
        pass

    query, params = captured[0]
    assert_no_apoc(query)
    assert '*1..2' in query
    assert 'target_prefix' in params
    # distinctness check must not depend on apoc.coll.toSet
    assert 'range(0, size(prefixes)' in query


@pytest.mark.parametrize('forward', [True, False, None])
@pytest.mark.asyncio
async def test_trace_term_iter_all_directions(db, captured, forward):
    async for _ in db.trace_term_iter(
        ConceptPrefix.MONDO, ConceptPrefix.SNOMED, 'a', 'b',
        ConceptRelationshipType.IS_A, forward=forward, max_depth=5,
    ):
        pass

    query, params = captured[0]
    assert_no_apoc(query)
    assert params['rel_type'] == 'is_a'
    assert '*1..5' in query

    if forward is None:
        assert 'SHORTEST 1' in query
    else:
        # detour-elimination reduce must use the native head()/range() index lookup
        assert 'head(' in query


@pytest.mark.asyncio
async def test_trace_term_aggregate_handles_mixed_directions_in_one_batch(db, captured):
    async for _ in db.trace_term_aggregate_iter([
        (ConceptPrefix.MONDO, 'a', ConceptPrefix.SNOMED, 'b', ConceptRelationshipType.IS_A, True, 5),
        (ConceptPrefix.MONDO, 'a', ConceptPrefix.SNOMED, 'c', ConceptRelationshipType.IS_A, False, 3),
        (ConceptPrefix.MONDO, 'a', ConceptPrefix.SNOMED, 'd', ConceptRelationshipType.IS_A, None, 7),
    ]):
        pass

    assert len(captured) == 1  # still a single batched query, not one per row
    query, params = captured[0]
    assert_no_apoc(query)
    assert 'WHEN q.forward IS NULL THEN' in query
    assert 'SHORTEST 1' in query
    # the match-time depth bound is the max across the batch (7), enforced per-row afterwards
    assert '*1..7' in query
    assert 'length(p) <= q.max_depth' in query
    assert params['queries'][0]['rel_type'] == 'is_a'


@pytest.mark.asyncio
async def test_trace_term_aggregate_empty_batch_short_circuits(db, captured):
    results = [x async for x in db.trace_term_aggregate_iter([])]
    assert results == []
    assert captured == []


@pytest.mark.asyncio
async def test_get_similar_terms_aggregate_uses_properties_and_reduce(db, captured):
    async for _ in db.get_similar_terms_aggregate_iter(ConceptPrefix.MONDO, [('a', 0.5)]):
        pass

    query, _ = captured[0]
    assert_no_apoc(query)
    assert 'properties(r)' in query
    assert 'reduce(' in query


@pytest.mark.asyncio
async def test_get_similar_terms_moves_map_building_to_python(db, captured):
    async for _ in db.get_similar_terms_iter(
        ConceptPrefix.MONDO, ['a'], threshold=0.5, same_prefix=True,
        method=SimilarityMethod.RELEVANCE, limit=5,
    ):
        pass

    query, _ = captured[0]
    assert_no_apoc(query)
    assert 'score_pairs' in query


def test_build_similar_term_record_reconstructs_dict_from_pairs():
    record = {
        'concept_id': 'a',
        'similar_prefix': 'mondo',
        'similar_concepts': [
            {'id': 'b', 'score_pairs': [['relevance', 0.9], ['co_annotation', 0.7]]},
        ],
    }

    concept_id, group, size = Neo4jGraphDatabase._build_similar_term_record(record)

    assert concept_id == 'a'
    assert size == 1
    assert group.similar_concepts[0].similarity_scores == {'relevance': 0.9, 'co_annotation': 0.7}


@pytest.mark.asyncio
async def test_translate_terms_drops_unused_map_and_uses_reduce(db, captured):
    async for _ in db.translate_terms_iter(
        ['a'], ConceptPrefix.MONDO, {ConceptPrefix.SNOMED: {'x'}}, threshold=0.5, limit=3,
    ):
        pass

    query, _ = captured[0]
    assert_no_apoc(query)
    assert 'properties(r)' in query
    assert 'reduce(' in query


@pytest.mark.asyncio
async def test_save_vocabulary_graph_uses_dynamic_relationship_type(db, captured):
    import networkx as nx
    from bioterms.model.concept.hgnc import HgncConcept

    graph = nx.MultiDiGraph()
    graph.add_edge('A1', 'A2', label=ConceptRelationshipType.IS_A)

    concept = HgncConcept(
        conceptId='A1',
        prefix=ConceptPrefix.HGNC,
        label='Test',
        conceptTypes=['gene'],
    )

    await db.save_vocabulary_graph([concept], graph)

    edge_query = captured[-1][0]
    assert_no_apoc(edge_query)
    assert '[rel:$(rel_label)]' in edge_query


@pytest.mark.asyncio
async def test_save_annotations_uses_dynamic_relationship_type(db, captured):
    from bioterms.model.annotation import Annotation

    annotation = Annotation(
        conceptIdFrom='A1',
        prefixFrom=ConceptPrefix.HGNC,
        conceptIdTo='B1',
        prefixTo=ConceptPrefix.MONDO,
        annotationType='annotated_with',
    )

    await db.save_annotations([annotation])

    query, _ = captured[0]
    assert_no_apoc(query)
    assert '[rel:$(rel_type)]' in query


@pytest.mark.asyncio
async def test_save_similarity_scores_no_longer_uses_apoc(db, captured):
    await db.save_similarity_scores(
        ConceptPrefix.MONDO, ConceptPrefix.SNOMED,
        [('a', 'b', 0.8)], SimilarityMethod.RELEVANCE,
    )

    query, _ = captured[0]
    assert_no_apoc(query)
    assert '[rel:similar_to]' in query


@pytest.mark.asyncio
async def test_delete_vocabulary_graph_uses_call_in_transactions(db, captured):
    await db.delete_vocabulary_graph(ConceptPrefix.MONDO)

    assert len(captured) == 2
    for query, _ in captured:
        assert_no_apoc(query)
        assert 'IN TRANSACTIONS OF 50000 ROWS' in query
