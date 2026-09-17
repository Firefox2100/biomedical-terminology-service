import gzip

import pytest

from bioterms.annotation import gene_uniprot, reactome_uniprot
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix


class FakeGraphDb:
    def __init__(self):
        self.annotations = []

    async def count_terms(self, prefix):
        return 1

    async def count_annotations(self, prefix_1, prefix_2):
        return 0

    async def save_annotations(self, annotations):
        self.annotations.extend(annotations)


@pytest.mark.asyncio
async def test_explicit_uniprot_gene_load_reparses_release(monkeypatch, tmp_path):
    uniprot_dir = tmp_path / 'uniprot'
    uniprot_dir.mkdir()
    record = """\
ID   TEST_HUMAN Reviewed; 10 AA.
AC   P12345;
DE   RecName: Full=Test protein;
OS   Homo sapiens (Human).
OX   NCBI_TaxID=9606;
DR   HGNC; HGNC:1; TEST1.
//
"""
    for filename, content in (
        ('uniprot_sprot.dat.gz', record),
        ('uniprot_trembl.dat.gz', ''),
    ):
        with gzip.open(uniprot_dir / filename, 'wt', encoding='utf-8') as stream:
            stream.write(content)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await gene_uniprot.load_annotation_from_file(graph_db=graph_db)

    assert len(graph_db.annotations) == 1
    annotation = graph_db.annotations[0]
    assert annotation.prefix_from == ConceptPrefix.UNIPROT
    assert annotation.prefix_to == ConceptPrefix.HGNC_SYMBOL
    assert annotation.concept_id_from == 'P12345'
    assert annotation.concept_id_to == 'TEST1'
    assert annotation.annotation_type == AnnotationType.HAS_SYMBOL


@pytest.mark.asyncio
async def test_explicit_reactome_uniprot_load(monkeypatch, tmp_path):
    reactome_dir = tmp_path / 'reactome'
    reactome_dir.mkdir()
    (reactome_dir / 'gene_mapping.csv').write_text(
        'gene_id,symbol\nR-HSA-1,P68104\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await reactome_uniprot.load_annotation_from_file(graph_db=graph_db)

    assert len(graph_db.annotations) == 1
    annotation = graph_db.annotations[0]
    assert annotation.prefix_from == ConceptPrefix.REACTOME
    assert annotation.prefix_to == ConceptPrefix.UNIPROT
    assert annotation.concept_id_from == 'R-HSA-1'
    assert annotation.concept_id_to == 'P68104'
    assert annotation.annotation_type == AnnotationType.EXACT
