import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType
import bioterms.vocabulary.ensembl as ensembl


def test_load_hgnc_ensembl_symbol_lookup_reads_crosswalk(monkeypatch, tmp_path):
    hgnc_dir = tmp_path / 'hgnc'
    hgnc_dir.mkdir()
    (hgnc_dir / 'symbol.txt').write_text(
        'hgnc_id\tsymbol\tensembl_gene_id\n'
        'HGNC:5\tA1BG\tENSG00000121410\n'
        'HGNC:6\tA1BG-AS1\t\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    lookup = ensembl._load_hgnc_ensembl_symbol_lookup()

    assert lookup == {'ENSG00000121410': 'A1BG'}


def test_load_hgnc_ensembl_symbol_lookup_missing_file_returns_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    lookup = ensembl._load_hgnc_ensembl_symbol_lookup()

    assert lookup == {}


def test_handle_gene_feature_tags_edge_matching_hgnc_crosswalk():
    import networkx as nx

    attributes = {'gene_id': 'ENSG00000121410', 'gene_name': 'A1BG', 'gene_biotype': 'protein_coding'}
    row = pd.Series({'start': 100, 'end': 200, 'seqname': '19'})
    genes = {}
    graph = nx.DiGraph()
    annotations = []
    lookup = {'ENSG00000121410': 'A1BG'}

    ensembl._handle_gene_feature(attributes, row, genes, graph, annotations, lookup)

    assert len(annotations) == 1
    assert annotations[0].annotation_type == AnnotationType.HAS_SYMBOL
    assert annotations[0].properties == {'derivation': 'hgnc_ensembl_gene_id_xref'}


def test_handle_gene_feature_does_not_tag_when_symbol_mismatches_hgnc():
    import networkx as nx

    attributes = {'gene_id': 'ENSG00000121410', 'gene_name': 'SOME_OTHER_NAME', 'gene_biotype': 'protein_coding'}
    row = pd.Series({'start': 100, 'end': 200, 'seqname': '19'})
    genes = {}
    graph = nx.DiGraph()
    annotations = []
    lookup = {'ENSG00000121410': 'A1BG'}

    ensembl._handle_gene_feature(attributes, row, genes, graph, annotations, lookup)

    assert len(annotations) == 1
    assert annotations[0].properties is None


def test_handle_gene_feature_does_not_tag_when_gene_not_in_lookup():
    import networkx as nx

    attributes = {'gene_id': 'ENSG_UNKNOWN', 'gene_name': 'FOO', 'gene_biotype': 'protein_coding'}
    row = pd.Series({'start': 100, 'end': 200, 'seqname': '19'})
    genes = {}
    graph = nx.DiGraph()
    annotations = []

    ensembl._handle_gene_feature(attributes, row, genes, graph, annotations, {})

    assert len(annotations) == 1
    assert annotations[0].properties is None


def test_handle_gene_feature_defaults_lookup_to_empty():
    import networkx as nx

    attributes = {'gene_id': 'ENSG_X', 'gene_name': 'FOO', 'gene_biotype': 'protein_coding'}
    row = pd.Series({'start': 100, 'end': 200, 'seqname': '19'})
    genes = {}
    graph = nx.DiGraph()
    annotations = []

    # No hgnc_ensembl_lookup argument at all -- must not raise.
    ensembl._handle_gene_feature(attributes, row, genes, graph, annotations)

    assert len(annotations) == 1
    assert annotations[0].properties is None
