import networkx as nx
import pandas as pd
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptRelationshipType, ConceptStatus
import bioterms.vocabulary.hgnc as hgnc
import bioterms.vocabulary.hgnc_symbol as hgnc_symbol


def test_withdrawn_hgnc_concept_uses_internal_ids_and_previous_symbol():
    graph = nx.DiGraph()
    row = pd.Series({
        'HGNC_ID': 'HGNC:99',
        'WITHDRAWN_SYMBOL': 'OLD1',
        'MERGED_INTO_REPORT(S) (i.e HGNC_ID|SYMBOL|STATUS)': (
            'HGNC:5|A1BG|Approved, HGNC:6|A2BG|Approved'
        ),
    })

    concept, annotation = hgnc._build_hgnc_withdrawn_concept(row, graph)

    assert concept.concept_id == '99'
    assert concept.status == ConceptStatus.DEPRECATED
    assert set(graph.edges) == {('99', '5'), ('99', '6')}
    assert all(data['label'] == ConceptRelationshipType.REPLACED_BY
               for _, _, data in graph.edges(data=True))
    assert annotation.concept_id_from == '99'
    assert annotation.concept_id_to == 'OLD1'
    assert annotation.annotation_type == AnnotationType.PREVIOUS_SYMBOL


def test_hgnc_symbol_builder_prefers_sortable_location_and_preserves_names():
    row = pd.Series({
        'hgnc_id': 'HGNC:5', 'symbol': 'A1BG', 'name': 'alpha-1-B glycoprotein',
        'alias_symbol': 'ABG', 'alias_name': 'alpha-1B-glycoprotein',
        'location_sortable': '19q13.43', 'location': '19q13.4', 'status': 'Approved',
    })

    concept, annotations = hgnc._build_hgnc_symbol_concept(row)

    assert concept.location == '19q13.43'
    assert concept.definition == 'alpha-1-B glycoprotein'
    assert concept.synonyms == ['ABG', 'alpha-1B-glycoprotein']
    assert {annotation.annotation_type for annotation in annotations} == {
        AnnotationType.ALIAS_SYMBOL, AnnotationType.HAS_SYMBOL,
    }


@pytest.mark.asyncio
async def test_gene_symbol_vocabulary_distinguishes_active_aliases_and_withdrawn_symbols(
    monkeypatch, tmp_path,
):
    hgnc_dir = tmp_path / 'hgnc'
    hgnc_dir.mkdir()
    pd.DataFrame([
        {'symbol': 'A1BG', 'alias_symbol': 'ABG|OLD1'},
        {'symbol': 'A2BG', 'alias_symbol': pd.NA},
    ]).to_csv(hgnc_dir / 'symbol.txt', sep='\t', index=False)
    pd.DataFrame([
        {'WITHDRAWN_SYMBOL': 'OLD1', 'STATUS': 'Symbol Withdrawn'},
        {'WITHDRAWN_SYMBOL': 'REMOVED', 'STATUS': 'Entry Withdrawn'},
    ]).to_csv(hgnc_dir / 'withdrawn.txt', sep='\t', index=False)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    captured = {}

    async def capture_concepts(prefix, concepts, **_kwargs):
        captured['concepts'] = concepts

    async def capture_graph(prefix, concepts, vocabulary_graph):
        captured['graph'] = vocabulary_graph

    monkeypatch.setattr(hgnc_symbol, 'write_concepts_to_file', capture_concepts)
    monkeypatch.setattr(hgnc_symbol, 'write_graph_to_file', capture_graph)

    await hgnc_symbol.load_vocabulary_from_file(
        offline=True, build_search_index=False,
    )

    concepts = {concept.concept_id: concept for concept in captured['concepts']}
    assert set(concepts) == {'A1BG', 'A2BG', 'ABG', 'OLD1'}
    assert concepts['OLD1'].status == ConceptStatus.DEPRECATED
    assert all(concepts[symbol].status == ConceptStatus.ACTIVE
               for symbol in ('A1BG', 'A2BG', 'ABG'))
    # Write-side graph buffers deliberately do not duplicate concept IDs as graph nodes;
    # database writers receive the concept list separately.
    assert len(captured['graph']) == 0
