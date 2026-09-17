import gzip

import pytest

from bioterms.annotation import gene_omim, gene_uniprot, hgnc_reactome, ncit_reactome, \
    omim_reactome, reactome_uniprot
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
async def test_omim_gene_download_reuses_omim_release(monkeypatch):
    received_client = object()
    calls = []

    async def fake_download_vocabulary(download_client=None):
        calls.append(download_client)

    monkeypatch.setattr(gene_omim, 'download_vocabulary', fake_download_vocabulary)

    await gene_omim.download_annotation(download_client=received_client)

    assert calls == [received_client]


@pytest.mark.asyncio
async def test_explicit_omim_gene_load_uses_mapping_in_omim_csv(monkeypatch, tmp_path):
    omim_dir = tmp_path / 'omim'
    omim_dir.mkdir()
    (omim_dir / 'omim.csv').write_text(
        'Class ID,Gene Symbol\n'
        'http://purl.bioontology.org/ontology/OMIM/100100,ADA|ADA2\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await gene_omim.load_annotation_from_file(graph_db=graph_db)

    assert [
        (annotation.concept_id_from, annotation.concept_id_to)
        for annotation in graph_db.annotations
    ] == [('100100', 'ADA'), ('100100', 'ADA2')]


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
    (reactome_dir / 'uniprot_mapping.csv').write_text(
        'reactome_id,external_id\nR-HSA-1,P68104\n'
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
    assert annotation.properties == {'source': 'Reactome ReferenceEntity'}


@pytest.mark.asyncio
async def test_reactome_reference_entity_hgnc_and_omim_loaders(monkeypatch, tmp_path):
    reactome_dir = tmp_path / 'reactome'
    reactome_dir.mkdir()
    (reactome_dir / 'hgnc_mapping.csv').write_text(
        'reactome_id,external_id\nR-HSA-1,3189\n'
    )
    (reactome_dir / 'omim_mapping.csv').write_text(
        'reactome_id,external_id\nR-HSA-1,130590\n'
    )
    (reactome_dir / 'ncit_mapping.csv').write_text(
        'reactome_id,external_id\nR-ALL-2,C119619\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await hgnc_reactome.load_annotation_from_file(graph_db=graph_db)
    await omim_reactome.load_annotation_from_file(graph_db=graph_db)
    await ncit_reactome.load_annotation_from_file(graph_db=graph_db)

    assert [(a.prefix_to, a.concept_id_to) for a in graph_db.annotations] == [
        (ConceptPrefix.HGNC, '3189'),
        (ConceptPrefix.OMIM, '130590'),
        (ConceptPrefix.NCIT, 'C119619'),
    ]
    assert graph_db.annotations[0].properties is None
    assert graph_db.annotations[1].properties == {'source': 'Reactome ReferenceEntity'}
    assert graph_db.annotations[2].properties == {'source': 'Reactome ReferenceEntity'}
