import gzip

import pytest

from bioterms.annotation import gene_hpo, gene_omim, gene_uniprot, hgnc_reactome, hpo_omim, \
    hpo_ordo, ncit_reactome, omim_reactome, reactome_uniprot
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


def _write_hpoa(path):
    path.write_text(
        '#version: 2026-09-02\n'
        'database_id\tdisease_name\tqualifier\thpo_id\treference\tevidence\tonset\t'
        'frequency\tsex\tmodifier\taspect\tbiocuration\n'
        'OMIM:1\tDisease one\t\tHP:0000001\tPMID:1\tPCS\tHP:0003577\t1/2\t'
        'FEMALE\tHP:0012825\tP\tHPO:test[2026-01-01]\n'
        'MIM:2\tDisease two\t\tHP:0000002\tOMIM:2\tTAS\t\t\t\t\tP\t'
        'HPO:test[2026-01-01]\n'
        'ORPHA:3\tDisease three\tNOT\tHP:0000003\tORPHA:3\tTAS\t\t\t\t\tP\t'
        'ORPHA:test[2026-01-01]\n'
    )


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
async def test_hpo_omim_loads_hpoa_in_release_direction_with_metadata(monkeypatch, tmp_path):
    hpo_dir = tmp_path / 'hpo'
    hpo_dir.mkdir()
    _write_hpoa(hpo_dir / 'phenotype.hpoa')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await hpo_omim.load_annotation_from_file(graph_db=graph_db)

    assert [(a.concept_id_from, a.concept_id_to) for a in graph_db.annotations] == [
        ('0000001', '1'), ('0000002', '2'),
    ]
    annotation = graph_db.annotations[0]
    assert annotation.prefix_from == ConceptPrefix.HPO
    assert annotation.prefix_to == ConceptPrefix.OMIM
    assert annotation.properties == {
        'reference': 'PMID:1',
        'evidence': 'PCS',
        'onset': 'HP:0003577',
        'frequency': '1/2',
        'sex': 'FEMALE',
        'modifier': 'HP:0012825',
        'aspect': 'P',
        'biocuration': 'HPO:test[2026-01-01]',
        'source': 'phenotype.hpoa',
    }


@pytest.mark.asyncio
async def test_hpo_ordo_keeps_hpoa_opposite_to_hoom(monkeypatch, tmp_path):
    hpo_dir = tmp_path / 'hpo'
    hpo_dir.mkdir()
    _write_hpoa(hpo_dir / 'phenotype.hpoa')
    hoom_dir = tmp_path / 'hoom'
    hoom_dir.mkdir()
    (hoom_dir / 'hoom_orphanet.owl').write_text('')

    class EmptyOntology:
        def load(self):
            return self

        def classes(self):
            return []

    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    monkeypatch.setattr(hpo_ordo, 'get_ontology', lambda _: EmptyOntology())
    graph_db = FakeGraphDb()

    await hpo_ordo.load_annotation_from_file(graph_db=graph_db)

    assert len(graph_db.annotations) == 1
    annotation = graph_db.annotations[0]
    assert annotation.prefix_from == ConceptPrefix.HPO
    assert annotation.concept_id_from == '0000003'
    assert annotation.prefix_to == ConceptPrefix.ORDO
    assert annotation.concept_id_to == '3'
    assert annotation.properties['source'] == 'phenotype.hpoa'
    assert annotation.properties['qualifier'] == 'NOT'


@pytest.mark.asyncio
async def test_hpo_gene_projection_uses_hpo_direction_and_filters_hpoa_negation(
    monkeypatch, tmp_path,
):
    hpo_dir = tmp_path / 'hpo'
    hpo_dir.mkdir()
    _write_hpoa(hpo_dir / 'phenotype.hpoa')
    (hpo_dir / 'gene_mapping.txt').write_text(
        'ncbi_gene_id\tgene_symbol\thpo_id\thpo_name\tfrequency\tdisease_id\n'
        '1\tGENE1\tHP:0000001\tAll\t1/2\tOMIM:1\n'
        '2\tGENE2\tHP:0000003\tFeature\t-\tORPHA:3\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await gene_hpo.load_annotation_from_file(graph_db=graph_db)

    assert len(graph_db.annotations) == 1
    annotation = graph_db.annotations[0]
    assert annotation.prefix_from == ConceptPrefix.HPO
    assert annotation.concept_id_from == '0000001'
    assert annotation.prefix_to == ConceptPrefix.HGNC_SYMBOL
    assert annotation.concept_id_to == 'GENE1'


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
