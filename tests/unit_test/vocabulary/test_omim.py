import networkx as nx
import pandas as pd
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptRelationshipType, ConceptStatus
import bioterms.vocabulary.omim as omim


def test_build_omim_concept_normalises_iris_and_moved_from_relationships():
    graph = nx.DiGraph()
    row = pd.Series({
        'Class ID': 'http://purl.bioontology.org/ontology/OMIM/100100',
        'Preferred Label': 'Example disease', 'Synonyms': 'Alias one|Alias two',
        'Obsolete': True,
        'Parents': 'http://purl.bioontology.org/ontology/OMIM/100000',
        'Moved from': '100099|100098',
    })

    concept = omim._build_omim_concept(row, graph)

    assert concept.concept_id == '100100'
    assert concept.synonyms == ['Alias one', 'Alias two']
    assert concept.status == ConceptStatus.DEPRECATED
    assert graph.edges['100100', '100000']['label'] == ConceptRelationshipType.IS_A
    assert graph.edges['100099', '100100']['label'] == ConceptRelationshipType.REPLACED_BY
    assert graph.edges['100098', '100100']['label'] == ConceptRelationshipType.REPLACED_BY


@pytest.mark.asyncio
async def test_omim_offline_load_reads_csv_release(monkeypatch, tmp_path):
    release_dir = tmp_path / 'omim'
    release_dir.mkdir()
    pd.DataFrame([{
        'Class ID': 'http://purl.bioontology.org/ontology/OMIM/100100',
        'Preferred Label': 'Example disease', 'Synonyms': 'Alias', 'Obsolete': False,
        'Parents': 'http://purl.bioontology.org/ontology/OMIM/100000',
        'Moved from': pd.NA,
    }]).to_csv(release_dir / 'omim.csv', index=False)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    captured = {}

    async def capture_concepts(prefix, concepts, **_kwargs):
        captured['concepts'] = concepts

    async def capture_graph(prefix, concepts, vocabulary_graph):
        captured['graph'] = vocabulary_graph

    monkeypatch.setattr(omim, 'write_concepts_to_file', capture_concepts)
    monkeypatch.setattr(omim, 'write_graph_to_file', capture_graph)

    await omim.load_vocabulary_from_file(offline=True, build_search_index=False)

    assert [concept.concept_id for concept in captured['concepts']] == ['100100']
    assert captured['graph'].has_edge('100100', '100000')
