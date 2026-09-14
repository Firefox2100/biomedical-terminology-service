import csv
import json

import pytest
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept, OhdsiConcept
from bioterms.vocabulary.utils import write_concepts_to_file, write_graph_to_file


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


def make_concept(concept_id, label, synonyms=None):
    return Concept(
        conceptTypes=[], prefix=ConceptPrefix.HPO, conceptId=concept_id, label=label,
        synonyms=synonyms, status=ConceptStatus.ACTIVE,
    )


@pytest.mark.asyncio
async def test_write_concepts_to_file_includes_search_index_by_default(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    concepts = [make_concept('HP:1', 'Fever', synonyms=['Pyrexia'])]
    await write_concepts_to_file(prefix=ConceptPrefix.HPO, concepts=concepts)

    lines = (tmp_path / 'offline' / 'hpo.doc.dump').read_text().strip().split('\n')
    payload = json.loads(lines[0])
    assert 'nGrams' in payload and payload['nGrams']
    assert payload['searchText']


@pytest.mark.asyncio
async def test_write_concepts_to_file_no_index_skips_search_fields(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    concepts = [make_concept('HP:1', 'Fever', synonyms=['Pyrexia'])]
    await write_concepts_to_file(prefix=ConceptPrefix.HPO, concepts=concepts, build_search_index=False)

    lines = (tmp_path / 'offline' / 'hpo.doc.dump').read_text().strip().split('\n')
    payload = json.loads(lines[0])
    assert 'nGrams' not in payload
    assert 'searchText' not in payload
    # The fields actually needed for restore/embedding are still fully present.
    assert payload['conceptId'] == 'HP:1'
    assert payload['label'] == 'Fever'
    assert payload['synonyms'] == ['Pyrexia']
