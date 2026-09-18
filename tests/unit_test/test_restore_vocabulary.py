from pathlib import Path

import pytest

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.concept.ohdsi import OhdsiConcept
from bioterms.model.concept.uniprot import UniProtConcept
from bioterms.similarity import _parse_similarity_dump_filename
from bioterms.vocabulary import (
    _iter_offline_graph_edges,
    _iter_offline_node_ids,
    restore_vocabulary,
)


def test_iter_offline_graph_edges_parses_relationship_type_and_key(tmp_path):
    graph_path = tmp_path / 'hpo.graph.dump'
    graph_path.write_text('1,2,is_a,rel-1\n')

    assert list(_iter_offline_graph_edges(str(graph_path))) == [
        ('1', '2', 'is_a', 'rel-1'),
    ]


def test_iter_offline_graph_edges_accepts_rows_without_optional_columns(tmp_path):
    graph_path = tmp_path / 'hpo.graph.dump'
    graph_path.write_text('1,2\n')

    assert list(_iter_offline_graph_edges(str(graph_path))) == [
        ('1', '2', None, None),
    ]


def test_iter_offline_node_ids_parses_extra_properties(tmp_path):
    ohdsi_path = tmp_path / 'ohdsi.node_ids.dump'
    ohdsi_path.write_text("123,[],SNOMED\n")
    uniprot_path = tmp_path / 'uniprot.node_ids.dump'
    uniprot_path.write_text("P1,,,True,9606,Homo sapiens\nP2,[]\n")

    ohdsi_node = next(_iter_offline_node_ids(
        str(ohdsi_path), ConceptPrefix.OHDSI, OhdsiConcept,
    ))
    uniprot_nodes = list(_iter_offline_node_ids(
        str(uniprot_path), ConceptPrefix.UNIPROT, UniProtConcept,
    ))

    assert ohdsi_node.source_vocabulary_id == 'SNOMED'
    assert uniprot_nodes[0].reviewed is True
    assert uniprot_nodes[0].organism_tax_id == '9606'
    assert uniprot_nodes[0].organism_name == 'Homo sapiens'
    assert uniprot_nodes[1].reviewed is None
    assert uniprot_nodes[1].organism_tax_id is None
    assert uniprot_nodes[1].organism_name is None


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
        await restore_vocabulary(ConceptPrefix.HPO, offline_dir=tmp_path)
