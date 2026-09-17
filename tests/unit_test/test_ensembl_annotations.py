import pandas as pd
import pytest

from bioterms.annotation import ensembl_gene, ensembl_omim, ensembl_reactome, ensembl_uniprot
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import discover_latest_numbered_release
from bioterms.vocabulary import get_vocabulary_license


class FakeGraphDb:
    def __init__(self):
        self.annotations = []

    async def count_terms(self, prefix):
        return 1

    async def count_annotations(self, prefix_1, prefix_2):
        return 0

    async def save_annotations(self, annotations):
        self.annotations.extend(annotations)


def test_mapping_publishers_document_mapping_licences():
    ensembl_license = get_vocabulary_license(ConceptPrefix.ENSEMBL)
    hgnc_license = get_vocabulary_license(ConceptPrefix.HGNC)
    reactome_license = get_vocabulary_license(ConceptPrefix.REACTOME)

    assert 'Ensembl-to-UniProt TSV' in ensembl_license
    assert 'BioMart MIM projection' in ensembl_license
    assert 'ensembl_gene_id' in hgnc_license
    assert 'Ensembl2Reactome.txt' in reactome_license


@pytest.mark.asyncio
async def test_ensembl_release_discovery_uses_highest_number():
    class Response:
        text = '<a href="release-115/"></a><a href="release-116/"></a>'

        def raise_for_status(self):
            pass

    class Client:
        async def get(self, url):
            return Response()

    release, url = await discover_latest_numbered_release(
        'https://ftp.ensembl.org/pub/', Client(),
    )

    assert release == 116
    assert url == 'https://ftp.ensembl.org/pub/release-116/'


@pytest.mark.asyncio
async def test_hgnc_mapping_is_independent_and_uses_hgnc_crosswalk(monkeypatch, tmp_path):
    path = tmp_path / 'ensembl' / 'mapping'
    path.mkdir(parents=True)
    pd.DataFrame([
        {'ensembl_gene_id': 'ENSG1', 'symbol': 'GENE1'},
        {'ensembl_gene_id': None, 'symbol': 'NOPE'},
    ]).to_csv(path / 'hgnc.tsv', sep='\t', index=False)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph = FakeGraphDb()

    await ensembl_gene.load_annotation_from_file(graph)

    assert [(a.concept_id_from, a.concept_id_to, a.annotation_type) for a in graph.annotations] == [
        ('ENSG1', 'GENE1', AnnotationType.HAS_SYMBOL),
    ]


@pytest.mark.asyncio
async def test_uniprot_mapping_targets_proteins_and_excludes_isoforms(monkeypatch, tmp_path):
    path = tmp_path / 'ensembl' / 'mapping'
    path.mkdir(parents=True)
    pd.DataFrame([
        {'gene_stable_id': 'ENSG1', 'transcript_stable_id': 'ENST1',
         'protein_stable_id': 'ENSP1', 'xref': 'P1', 'db_name': 'Uniprot/SWISSPROT',
         'info_type': 'DIRECT', 'source_identity': '100', 'xref_identity': '100',
         'linkage_type': '-'},
        {'gene_stable_id': 'ENSG1', 'transcript_stable_id': 'ENST1',
         'protein_stable_id': 'ENSP1', 'xref': 'P1-2', 'db_name': 'Uniprot_isoform',
         'info_type': 'DIRECT', 'source_identity': '-', 'xref_identity': '-',
         'linkage_type': '-'},
    ]).to_csv(path / 'uniprot.tsv', sep='\t', index=False)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph = FakeGraphDb()

    await ensembl_uniprot.load_annotation_from_file(graph)

    assert len(graph.annotations) == 1
    assert graph.annotations[0].concept_id_from == 'ENSP1'
    assert graph.annotations[0].concept_id_to == 'P1'
    assert graph.annotations[0].annotation_type == AnnotationType.EXACT
    assert graph.annotations[0].properties['source'] == 'Ensembl UniProt TSV'
    assert graph.annotations[0].properties['externalDatabase'] == 'Uniprot/SWISSPROT'


@pytest.mark.asyncio
async def test_reactome_mapping_filters_to_human_ensembl_ids(monkeypatch, tmp_path):
    path = tmp_path / 'ensembl' / 'mapping'
    path.mkdir(parents=True)
    (path / 'reactome.tsv').write_text(
        'ENSG1\tR-HSA-1\turl\tPathway\tTAS\tHomo sapiens\n'
        'ENSG2\tR-MMU-1\turl\tPathway\tIEA\tMus musculus\n'
        'P12345\tR-HSA-2\turl\tPathway\tIEA\tHomo sapiens\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph = FakeGraphDb()

    await ensembl_reactome.load_annotation_from_file(graph)

    assert [(a.concept_id_from, a.concept_id_to) for a in graph.annotations] == [('R-HSA-1', 'ENSG1')]
    assert graph.annotations[0].prefix_from == ConceptPrefix.REACTOME
    assert graph.annotations[0].prefix_to == ConceptPrefix.ENSEMBL
    assert graph.annotations[0].properties['source'] == 'Reactome Ensembl2Reactome'


@pytest.mark.asyncio
async def test_omim_mapping_distinguishes_gene_and_morbid_links(monkeypatch, tmp_path):
    path = tmp_path / 'ensembl' / 'mapping'
    path.mkdir(parents=True)
    pd.DataFrame([
        {'ensembl_gene_id': 'ENSG1', 'mim_gene_accession': '100100',
         'mim_morbid_accession': '200200'},
    ]).to_csv(path / 'omim.tsv', sep='\t', index=False)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph = FakeGraphDb()

    await ensembl_omim.load_annotation_from_file(graph)

    assert {(a.concept_id_to, a.annotation_type) for a in graph.annotations} == {
        ('100100', AnnotationType.EXACT),
        ('200200', AnnotationType.ANNOTATED_WITH),
    }
    assert all(a.prefix_from == ConceptPrefix.ENSEMBL for a in graph.annotations)
    assert all(a.properties['source'] == 'Ensembl BioMart' for a in graph.annotations)
    assert {a.properties['mapping'] for a in graph.annotations} == {
        'mim_gene_accession', 'mim_morbid_accession',
    }
