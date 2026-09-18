import networkx as nx
import pandas as pd
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptRelationshipType, ConceptStatus
import bioterms.vocabulary.ncit as ncit


def test_build_ncit_concept_uses_first_synonym_as_label_and_builds_parents():
    graph = nx.DiGraph()
    row = pd.Series({
        'code': 'C1', 'synonyms': 'Preferred|Alias one|Alias two',
        'definition': 'Definition', 'concept_status': 'Obsolete_Concept',
        'parents': 'C2|C3',
    })

    concept = ncit._build_ncit_concept(row, graph)

    assert concept.label == 'Preferred'
    assert concept.synonyms == ['Alias one', 'Alias two']
    assert concept.definition == 'Definition'
    assert concept.status == ConceptStatus.DEPRECATED
    assert set(graph.edges) == {('C1', 'C2'), ('C1', 'C3')}
    assert all(data['label'] == ConceptRelationshipType.IS_A
               for _, _, data in graph.edges(data=True))


@pytest.mark.asyncio
async def test_ncit_offline_load_reads_flat_release(monkeypatch, tmp_path):
    release_dir = tmp_path / 'ncit'
    release_dir.mkdir()
    (release_dir / 'thesaurus.txt').write_text(
        'C1\tiri\tC2\tPreferred|Alias\tDefinition\tDisplay\tActive\tType\tSubset\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    captured = {}

    async def capture_concepts(prefix, concepts, **_kwargs):
        captured['concepts'] = concepts

    async def capture_graph(prefix, concepts, vocabulary_graph):
        captured['graph'] = vocabulary_graph

    monkeypatch.setattr(ncit, 'write_concepts_to_file', capture_concepts)
    monkeypatch.setattr(ncit, 'write_graph_to_file', capture_graph)

    await ncit.load_vocabulary_from_file(offline=True, build_search_index=False)

    assert [concept.concept_id for concept in captured['concepts']] == ['C1']
    assert captured['graph'].has_edge('C1', 'C2')
