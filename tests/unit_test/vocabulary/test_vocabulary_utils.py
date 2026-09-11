import csv

import pytest
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept import OhdsiConcept
from bioterms.vocabulary.utils import write_graph_to_file


@pytest.mark.asyncio
async def test_write_graph_to_file_includes_source_vocabulary_id_column(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    concepts = [
        OhdsiConcept(prefix=ConceptPrefix.OHDSI, conceptId='1', sourceVocabularyId='SNOMED'),
        OhdsiConcept(prefix=ConceptPrefix.OHDSI, conceptId='2'),
    ]
    graph = nx.MultiDiGraph()
    for concept in concepts:
        graph.add_node(concept.concept_id)

    await write_graph_to_file(
        prefix=ConceptPrefix.OHDSI,
        concepts=concepts,
        vocabulary_graph=graph,
    )

    node_id_path = tmp_path / 'offline' / 'ohdsi.node_ids.dump'
    with open(node_id_path, newline='') as f:
        rows = list(csv.reader(f))

    rows_by_id = {row[0]: row for row in rows}
    assert rows_by_id['1'][2] == 'SNOMED'
    assert rows_by_id['2'][2] == ''
