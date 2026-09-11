import inspect

from bioterms.database.graph_db.neo4j_graph_db import Neo4jGraphDatabase
from bioterms.model.concept import GRAPH_NODE_EXTRA_PROPERTIES


def test_save_vocabulary_graph_sets_extra_properties_dynamically():
    source = inspect.getsource(Neo4jGraphDatabase.save_vocabulary_graph)

    # Generic dynamic-property mechanism: only present (non-null) keys from
    # GRAPH_NODE_EXTRA_PROPERTIES are ever written, so a vocabulary that doesn't populate a
    # given field (e.g. every non-OHDSI concept's sourceVocabularyId) is a guaranteed no-op.
    assert 'concept[k] IS NOT NULL' in source
    assert 'SET n[k] = concept[k]' in source
    assert "'extraProperties': GRAPH_NODE_EXTRA_PROPERTIES" in source


def test_graph_node_extra_properties_list_is_stable_and_includes_known_fields():
    # Regression guard: sourceVocabularyId (OHDSI) and reviewed (UniProt) must stay
    # promotable to real Neo4j node properties.
    assert 'sourceVocabularyId' in GRAPH_NODE_EXTRA_PROPERTIES
    assert 'reviewed' in GRAPH_NODE_EXTRA_PROPERTIES


def test_create_index_indexes_every_extra_property():
    # Without an index, filtering on e.g. organismTaxId (UniProt's whole point -- scoping
    # the full, multi-organism release down to human) is a full node scan at 250M+ nodes.
    source = inspect.getsource(Neo4jGraphDatabase.create_index)

    assert 'for property_name in GRAPH_NODE_EXTRA_PROPERTIES' in source
    assert 'CREATE INDEX concept_{property_name}_index' in source
    for property_name in GRAPH_NODE_EXTRA_PROPERTIES:
        assert property_name  # sanity: list isn't empty, loop actually has work to do
