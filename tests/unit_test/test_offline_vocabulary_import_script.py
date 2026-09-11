from pathlib import Path

import pytest

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.model.concept import GRAPH_NODE_EXTRA_PROPERTIES
from bioterms.similarity import _parse_similarity_dump_filename
from bioterms.vocabulary import _read_offline_graph, restore_vocabulary


def test_read_offline_graph_parses_relationship_type_and_key(tmp_path):
    graph_path = tmp_path / 'hpo.graph.dump'
    graph_path.write_text('1,2,is_a,rel-1\n')

    graph = _read_offline_graph(str(graph_path))

    assert graph.number_of_edges() == 1
    edge_data = list(graph.edges(data=True, keys=True))[0]
    assert edge_data[0] == '1'
    assert edge_data[1] == '2'
    assert edge_data[2] == 'rel-1'
    assert edge_data[3]['label'] == ConceptRelationshipType.IS_A


def test_read_offline_graph_accepts_rows_without_optional_columns(tmp_path):
    graph_path = tmp_path / 'hpo.graph.dump'
    graph_path.write_text('1,2\n')

    graph = _read_offline_graph(str(graph_path))

    assert graph.number_of_edges() == 1
    edge_data = list(graph.edges(data=True, keys=True))[0]
    assert edge_data[3]['label'] is None


def test_similarity_filename_is_validated_against_target_prefix():
    method, corpus = _parse_similarity_dump_filename(
        Path('hpo-relevance-ordo.similarity.dump'),
        ConceptPrefix.HPO,
    )
    assert method.value == 'relevance'
    assert corpus == ConceptPrefix.ORDO

    with pytest.raises(ValueError, match='Unexpected similarity filename'):
        _parse_similarity_dump_filename(
            Path('mondo-relevance-ordo.similarity.dump'),
            ConceptPrefix.HPO,
        )


@pytest.mark.asyncio
async def test_restore_requires_doc_and_graph_dumps(tmp_path):
    with pytest.raises(ValueError, match='Missing required offline dump file'):
        await restore_vocabulary(
            ConceptPrefix.HPO,
            offline_dir=tmp_path,
        )


def test_node_row_extra_property_columns_are_parsed_positionally():
    # Mirrors the row-building loop in write_graph_to_file(): each GRAPH_NODE_EXTRA_PROPERTIES
    # entry occupies a fixed column starting at index 2, and short rows default missing columns.
    extra_properties = GRAPH_NODE_EXTRA_PROPERTIES

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
