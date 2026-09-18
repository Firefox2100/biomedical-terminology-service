import pandas as pd
import pytest

from bioterms.annotation import ensembl_hgnc, hgnc_omim, hgnc_uniprot
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.vocabulary import get_vocabulary_license
from bioterms.vocabulary.hgnc import _build_hgnc_symbol_concept
from bioterms.vocabulary import hgnc_symbol


class FakeGraphDb:
    def __init__(self):
        self.annotations = []

    async def count_terms(self, prefix):
        return 1

    async def count_annotations(self, prefix_1, prefix_2):
        return 0

    async def save_annotations(self, annotations):
        self.annotations.extend(annotations)


def _write_hgnc_release(path):
    pd.DataFrame([
        {
            'hgnc_id': 'HGNC:5',
            'symbol': 'A1BG',
            'ensembl_gene_id': 'ENSG1',
            'uniprot_ids': 'P1|P2',
            'omim_id': '100100|100101',
        },
        {
            'hgnc_id': 'HGNC:6',
            'symbol': 'A2BG',
            'ensembl_gene_id': '',
            'uniprot_ids': '',
            'omim_id': '',
        },
    ]).to_csv(path, sep='\t', index=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('module', 'prefix', 'expected_ids', 'annotation_type'),
    [
        (ensembl_hgnc, ConceptPrefix.ENSEMBL, ['ENSG1'], AnnotationType.EXACT),
        (hgnc_uniprot, ConceptPrefix.UNIPROT, ['P1', 'P2'], AnnotationType.ANNOTATED_WITH),
        (hgnc_omim, ConceptPrefix.OMIM, ['100100', '100101'], AnnotationType.EXACT),
    ],
)
async def test_hgnc_release_mappings_are_hgnc_directed(
    monkeypatch, tmp_path, module, prefix, expected_ids, annotation_type,
):
    hgnc_dir = tmp_path / 'hgnc'
    hgnc_dir.mkdir()
    _write_hgnc_release(hgnc_dir / 'symbol.txt')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await module.load_annotation_from_file(graph_db)

    assert [a.concept_id_to for a in graph_db.annotations] == expected_ids
    assert all(a.prefix_from == ConceptPrefix.HGNC for a in graph_db.annotations)
    assert all(a.concept_id_from == '5' for a in graph_db.annotations)
    assert all(a.prefix_to == prefix for a in graph_db.annotations)
    assert all(a.annotation_type == annotation_type for a in graph_db.annotations)
    assert all(a.properties is None for a in graph_db.annotations)


def test_hgnc_builtin_symbol_edges_use_internal_hgnc_ids():
    row = pd.Series({
        'hgnc_id': 'HGNC:5',
        'symbol': 'A1BG',
        'name': 'alpha-1-B glycoprotein',
        'alias_symbol': 'ABG|GAB',
        'alias_name': float('nan'),
        'location_sortable': '19q13.43',
        'location': '19q13.43',
        'status': 'Approved',
    })

    concept, annotations = _build_hgnc_symbol_concept(row)

    assert concept.concept_id == '5'
    assert {a.concept_id_from for a in annotations} == {'5'}
    assert {(a.concept_id_to, a.annotation_type) for a in annotations} == {
        ('A1BG', AnnotationType.HAS_SYMBOL),
        ('ABG', AnnotationType.ALIAS_SYMBOL),
        ('GAB', AnnotationType.ALIAS_SYMBOL),
    }
    assert all(a.prefix_to == ConceptPrefix.HGNC_SYMBOL for a in annotations)


def test_hgnc_license_covers_published_cross_references():
    license_text = get_vocabulary_license(ConceptPrefix.HGNC)

    assert 'Ensembl genes, UniProt entries, and OMIM gene entries' in license_text
    assert '`uniprot_ids` and `omim_id`' in license_text


@pytest.mark.asyncio
async def test_gene_symbol_download_reuses_hgnc_release(monkeypatch):
    received_client = object()
    calls = []

    async def fake_download(download_client=None):
        calls.append(download_client)

    monkeypatch.setattr('bioterms.vocabulary.hgnc.download_vocabulary', fake_download)

    await hgnc_symbol.download_vocabulary(download_client=received_client)

    assert calls == [received_client]
