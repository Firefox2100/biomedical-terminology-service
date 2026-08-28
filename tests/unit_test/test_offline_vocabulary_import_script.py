import runpy
from pathlib import Path

import pytest


SCRIPT = runpy.run_path(
    str(Path(__file__).parents[2] / 'scripts' / 'load_offline_vocabulary.py'),
    run_name='offline_vocabulary_import_script',
)


def test_graph_edge_query_has_pipeline_boundary_before_procedure_call():
    query = ' '.join(SCRIPT['GRAPH_EDGE_UPSERT_QUERY'].split())
    assert (
        'MERGE (target:Concept {id: edge.target, prefix: $prefix}) '
        'WITH source, target, edge '
        'CALL apoc.merge.relationship'
    ) in query
    assert 'coll.distinct' not in query


def test_only_file_resolves_bare_filename_under_offline_directory(tmp_path):
    offline_dir = tmp_path / 'offline'
    offline_dir.mkdir()
    expected = offline_dir / 'hpo-relevance-ordo.similarity.dump'
    assert SCRIPT['resolve_only_file'](Path(expected.name), offline_dir) == expected


def test_only_file_similarity_filename_is_validated():
    method, corpus = SCRIPT['parse_similarity_filename'](
        Path('hpo-relevance-ordo.similarity.dump'),
        SCRIPT['ConceptPrefix'].HPO,
    )
    assert method.value == 'relevance'
    assert corpus.value == 'ordo'


def test_only_file_accepts_matching_embedding_dump():
    classify = SCRIPT['classify_only_file']
    prefix = SCRIPT['ConceptPrefix'].HPO
    assert classify(prefix, Path('hpo.embed.dump')) == 'embedding'
    assert classify(prefix, Path('hpo-relevance-ordo.similarity.dump')) == 'similarity'

    with pytest.raises(ValueError, match='hpo.embed.dump'):
        classify(prefix, Path('mondo.embed.dump'))


def test_graph_node_query_sets_extra_properties_dynamically():
    query = ' '.join(SCRIPT['GRAPH_NODE_UPSERT_QUERY'].split())
    assert 'node[k] IS NOT NULL' in query
    assert 'SET n[k] = node[k]' in query


def test_graph_node_extra_properties_list_is_reexported():
    from bioterms.model.concept import GRAPH_NODE_EXTRA_PROPERTIES
    assert SCRIPT['GRAPH_NODE_EXTRA_PROPERTIES'] == GRAPH_NODE_EXTRA_PROPERTIES


def test_node_row_extra_property_columns_are_parsed_positionally():
    # Mirrors the row-building loop in load_graph(): each GRAPH_NODE_EXTRA_PROPERTIES entry
    # occupies a fixed column starting at index 2, in list order; a short row (pre-existing
    # dumps, or a vocabulary that never populates later fields) must default missing columns
    # to None rather than erroring.
    extra_properties = SCRIPT['GRAPH_NODE_EXTRA_PROPERTIES']

    def parse(row):
        node = {}
        for offset, key in enumerate(extra_properties):
            column = 2 + offset
            raw_value = row[column] if len(row) > column else ''
            if not raw_value:
                node[key] = None
            elif key == 'reviewed':
                node[key] = raw_value == 'True'
            else:
                node[key] = raw_value
        return node

    full_row = ['123', "['Concept']", 'SNOMED', 'True', '9606', 'Homo sapiens']
    short_row = ['456', "['Concept']", 'SNOMED']

    assert parse(full_row) == {
        'sourceVocabularyId': 'SNOMED',
        'reviewed': True,
        'organismTaxId': '9606',
        'organismName': 'Homo sapiens',
    }
    assert parse(short_row) == {
        'sourceVocabularyId': 'SNOMED',
        'reviewed': None,
        'organismTaxId': None,
        'organismName': None,
    }
