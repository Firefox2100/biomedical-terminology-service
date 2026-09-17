from collections.abc import Iterable
from typing import Optional

import networkx as nx
import pytest

from bioterms.database.doc_db.doc_db import DocumentDatabase, SearchQuery, normalise_search_query
from bioterms.database.graph_db.graph_db import GraphDatabase
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.model.concept import Concept


def test_normalise_search_query_is_shared_and_deterministic():
    query = normalise_search_query('  (Heart) "failure"  ')

    assert query.words == ['heart', 'failure']
    assert query.compact == 'heartfailure'


class RecordingDocumentDatabase(DocumentDatabase):
    _backend_name = 'test'

    def __init__(self):
        self.search_query = None

    async def _auto_complete_iter(self, prefix, search_query, limit, model_class):
        self.search_query = search_query
        yield model_class(prefix=prefix, conceptId='1', label='one')


RecordingDocumentDatabase.__abstractmethods__ = frozenset()


@pytest.mark.asyncio
async def test_autocomplete_template_normalises_before_backend_execution():
    database = RecordingDocumentDatabase()

    results = [item async for item in database.auto_complete_iter(
        ConceptPrefix.HPO, '(Heart) failure', limit=3,
    )]

    assert [item.concept_id for item in results] == ['1']
    assert database.search_query == SearchQuery(
        clean='heart failure', words=['heart', 'failure'], compact='heartfailure',
    )


class RecordingGraphDatabase(GraphDatabase):
    def __init__(self):
        self.saved = None

    async def _save_vocabulary_graph(self,
                                     prefix: ConceptPrefix,
                                     concepts: Iterable[Concept],
                                     edges: Iterable[tuple[str, str, Optional[str], Optional[str]]],
                                     consume_concepts: bool,
                                     ) -> None:
        self.saved = prefix, list(concepts), list(edges), consume_concepts


RecordingGraphDatabase.__abstractmethods__ = frozenset()


@pytest.mark.asyncio
async def test_graph_save_template_normalises_prefix_and_edges():
    database = RecordingGraphDatabase()
    concept = Concept(prefix=ConceptPrefix.HPO, conceptId='1', label='one')
    graph = nx.MultiDiGraph()
    graph.add_edge('1', '2', label=ConceptRelationshipType.IS_A)

    await database.save_vocabulary_graph([concept], graph, consume_concepts=True)

    prefix, concepts, edges, consume = database.saved
    assert prefix == ConceptPrefix.HPO
    assert concepts == [concept]
    assert edges == [('1', '2', 'is_a', 0)]
    assert consume is True


@pytest.mark.asyncio
async def test_graph_save_template_skips_empty_concepts():
    database = RecordingGraphDatabase()

    await database.save_vocabulary_graph([], nx.MultiDiGraph())

    assert database.saved is None
