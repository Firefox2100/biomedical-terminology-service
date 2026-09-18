import networkx as nx
import pandas as pd

from bioterms.etc.enums import ConceptType
import bioterms.vocabulary.ensembl as ensembl


def test_gene_feature_is_first_class_and_does_not_emit_annotation():
    genes = {}
    graph = nx.DiGraph()
    ensembl._handle_gene_feature(
        {
            'gene_id': 'ENSG1', 'gene_version': '3', 'gene_name': 'GENE1',
            'gene_biotype': 'protein_coding', 'gene_source': 'ensembl_havana',
        },
        pd.Series({'start': 10, 'end': 90, 'seqname': '1', 'strand': '+',
                   'source': 'ensembl_havana'}),
        genes,
        graph,
    )

    concept = genes['ENSG1']
    assert concept.concept_types == [ConceptType.GENE]
    assert concept.version == '3'
    assert concept.strand == '+'
    assert concept.source == 'ensembl_havana'
    assert list(graph.nodes) == ['ENSG1']


def test_transcript_exon_and_protein_relationships_and_metadata():
    graph = nx.DiGraph()
    transcripts = {}
    exons = {}
    proteins = {}
    row = pd.Series({'start': 20, 'end': 40, 'seqname': '1', 'strand': '-',
                     'source': 'havana'})
    attributes = {
        'gene_id': 'ENSG1', 'transcript_id': 'ENST1', 'transcript_version': '2',
        'transcript_name': 'GENE1-201', 'transcript_biotype': 'protein_coding',
        'transcript_source': 'havana', 'transcript_support_level': '1',
        'exon_id': 'ENSE1', 'exon_version': '4',
        'protein_id': 'ENSP1', 'protein_version': '5',
    }

    ensembl._handle_transcript_feature(attributes, row, transcripts, graph)
    ensembl._handle_exon_feature(attributes, row, exons, graph)
    ensembl._handle_cds_feature(attributes, row, proteins, graph)
    ensembl._handle_cds_feature(
        attributes,
        pd.Series({'start': 60, 'end': 80, 'seqname': '1', 'strand': '-', 'source': 'havana'}),
        proteins,
        graph,
    )

    assert transcripts['ENST1'].concept_types == [ConceptType.TRANSCRIPT]
    assert transcripts['ENST1'].transcript_support_level == '1'
    assert exons['ENSE1'].concept_types == [ConceptType.EXON]
    assert proteins['ENSP1'].concept_types == [ConceptType.PROTEIN]
    assert (proteins['ENSP1'].start, proteins['ENSP1'].end) == (20, 80)
    assert set(graph.edges) == {('ENST1', 'ENSG1'), ('ENSE1', 'ENST1'), ('ENSP1', 'ENST1')}


def test_ensembl_annotation_discovery_is_symmetric():
    expected = {
        ensembl.ConceptPrefix.HGNC,
        ensembl.ConceptPrefix.HGNC_SYMBOL,
        ensembl.ConceptPrefix.OMIM,
        ensembl.ConceptPrefix.REACTOME,
        ensembl.ConceptPrefix.UNIPROT,
    }
    assert set(ensembl.ANNOTATIONS) == expected
