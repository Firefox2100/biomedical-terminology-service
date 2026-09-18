import gzip

import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.graphql_api import _VOCABULARY_GRAPHQL_MODULES
from bioterms.vocabulary import get_vocabulary_license
import bioterms.vocabulary.uniprot as uniprot


REVIEWED_HUMAN_RECORD = """\
ID   EF1A1_HUMAN             Reviewed;         462 AA.
AC   P68104; P04719; P04720; Q6IQ15;
DT   13-AUG-1987, integrated into UniProtKB/Swiss-Prot.
DE   RecName: Full=Elongation factor 1-alpha 1;
DE            Short=EF-1-alpha-1;
DE   AltName: Full=Elongation factor Tu;
GN   Name=EEF1A1; Synonyms=EEF1A, EF1A, LENG7;
OS   Homo sapiens (Human).
OC   Eukaryota; Metazoa; Chordata.
OX   NCBI_TaxID=9606;
DR   RefSeq; NP_001393.1; NM_001402.6.
DR   HGNC; HGNC:3189; EEF1A1.
DR   PDB; 3C5J; X-ray; 1.80 A; C=343-355.
SQ   SEQUENCE   462 AA;  50141 MW;  74B44B5F1AD5A462 CRC64;
     MGKEKTHINI VVIGHVDSGK STTTGHLIYK CGGIDKRTIE KFEKEAAEMG KGSFKYAWVL
     DKLKAERERG ITIDISLWKF ETSKYYVTII DAPGHRDFIK NMITGTSQAD CAVLIVAAGV
//
"""

UNREVIEWED_NONHUMAN_RECORD = """\
ID   R1AB_SARS2              Reviewed;        7096 AA.
AC   P0DTD1;
DE   RecName: Full=Replicase polyprotein 1ab;
DE            Short=pp1ab;
DE   Contains:
DE     RecName: Full=Host translation inhibitor nsp1;
OS   Severe acute respiratory syndrome coronavirus 2.
OX   NCBI_TaxID=2697049;
SQ   SEQUENCE   7096 AA;  794057 MW;  0 CRC64;
     MESLVPGFNE KTHVQLSLPV LQVRDVLVRG FGDSVEEVLS EARQHLKDGT CGLVEVEKGV
//
"""

UNREVIEWED_WITH_EVIDENCE_TAGS_RECORD = """\
ID   A0A0A0MS99_HUMAN        Unreviewed;      1215 AA.
AC   A0A0A0MS99;
DE   RecName: Full=Multidrug resistance-associated protein 1 {ECO:0000256|ARBA:ARBA00041009};
GN   Name=ABCC1 {ECO:0000313|Ensembl:ENSP00000382340};
OS   Homo sapiens (Human).
OX   NCBI_TaxID=9606 {ECO:0000313|Ensembl:ENSP00000382340};
SQ   SEQUENCE   1215 AA;  171622 MW;  0 CRC64;
     MLRPEPTSQD PSKLSSLLPL HTP
//
"""

NO_ACCESSION_MALFORMED_RECORD = """\
ID   BROKEN_ENTRY             Reviewed;         1 AA.
DE   RecName: Full=No accession here;
SQ   SEQUENCE   1 AA;  1 MW;  0 CRC64;
     M
//
"""


def test_uniprot_has_graphql_and_license_support():
    assert _VOCABULARY_GRAPHQL_MODULES[ConceptPrefix.UNIPROT] == (
        'UNIPROT_SCHEMA',
        'uniprot',
        ['UNIPROT_CONCEPT'],
        'UNIPROT_QUERY',
    )
    assert 'Creative Commons Attribution 4.0' in get_vocabulary_license(ConceptPrefix.UNIPROT)


def _write_gz(path, content: str):
    with gzip.open(path, 'wt', encoding='utf-8') as f:
        f.write(content)


def test_iter_dat_records_splits_and_strips_sequence(tmp_path):
    gz_path = tmp_path / 'sample.dat.gz'
    _write_gz(gz_path, REVIEWED_HUMAN_RECORD + UNREVIEWED_NONHUMAN_RECORD)

    records = list(uniprot._iter_dat_records(str(gz_path)))

    assert len(records) == 2
    joined = ''.join(records[0])
    assert 'ID   EF1A1_HUMAN' in joined
    assert 'SQ   SEQUENCE' not in joined
    assert 'MGKEKTHINI' not in joined  # sequence data lines must not survive


def test_parse_dat_record_reviewed_human_with_hgnc():
    lines = REVIEWED_HUMAN_RECORD.splitlines(keepends=True)
    # Simulate what _iter_dat_records would yield (sequence stripped, no // line).
    lines = [l for l in lines if not l.startswith('SQ') and not l.startswith('//')
             and not l.startswith('     ')]

    record = uniprot._parse_dat_record(lines)

    assert record['accession'] == 'P68104'
    assert record['reviewed'] is True
    assert record['label'] == 'Elongation factor 1-alpha 1'
    assert record['organism_name'] == 'Homo sapiens (Human)'
    assert record['organism_tax_id'] == '9606'
    assert record['hgnc_symbol'] == 'EEF1A1'


def test_parse_dat_record_nonhuman_has_no_hgnc_and_uses_top_level_name():
    lines = UNREVIEWED_NONHUMAN_RECORD.splitlines(keepends=True)
    lines = [l for l in lines if not l.startswith('SQ') and not l.startswith('//')
             and not l.startswith('     ')]

    record = uniprot._parse_dat_record(lines)

    assert record['accession'] == 'P0DTD1'
    # Must take the top-level RecName, not the nested "Contains:" sub-component name.
    assert record['label'] == 'Replicase polyprotein 1ab'
    assert record['hgnc_symbol'] is None
    assert record['organism_tax_id'] == '2697049'


def test_parse_dat_record_strips_evidence_tags():
    lines = UNREVIEWED_WITH_EVIDENCE_TAGS_RECORD.splitlines(keepends=True)
    lines = [l for l in lines if not l.startswith('SQ') and not l.startswith('//')
             and not l.startswith('     ')]

    record = uniprot._parse_dat_record(lines)

    assert record['accession'] == 'A0A0A0MS99'
    assert record['reviewed'] is False
    assert record['label'] == 'Multidrug resistance-associated protein 1'
    assert record['organism_tax_id'] == '9606'


def test_parse_dat_record_returns_none_without_accession():
    lines = NO_ACCESSION_MALFORMED_RECORD.splitlines(keepends=True)
    lines = [l for l in lines if not l.startswith('SQ') and not l.startswith('//')
             and not l.startswith('     ')]

    assert uniprot._parse_dat_record(lines) is None


def test_build_uniprot_concept_stamps_organism_fields():
    record = {
        'accession': 'P68104', 'reviewed': True, 'label': 'Elongation factor 1-alpha 1',
        'organism_name': 'Homo sapiens (Human)', 'organism_tax_id': '9606', 'hgnc_symbol': 'EEF1A1',
    }

    concept = uniprot._build_uniprot_concept(record)

    assert concept.concept_id == 'P68104'
    assert concept.reviewed is True
    assert concept.organism_tax_id == '9606'
    assert concept.organism_name == 'Homo sapiens (Human)'
    assert concept.prefix == ConceptPrefix.UNIPROT


def test_build_symbol_annotation_present_and_absent():
    with_symbol = {'accession': 'P68104', 'hgnc_symbol': 'EEF1A1'}
    without_symbol = {'accession': 'P0DTD1', 'hgnc_symbol': None}

    annotation = uniprot._build_symbol_annotation(with_symbol)
    assert annotation.annotation_type == AnnotationType.HAS_SYMBOL
    assert annotation.concept_id_from == 'P68104'
    assert annotation.concept_id_to == 'EEF1A1'
    assert annotation.prefix_to == ConceptPrefix.HGNC_SYMBOL

    assert uniprot._build_symbol_annotation(without_symbol) is None


@pytest.mark.asyncio
async def test_load_vocabulary_from_file_streams_in_batches_offline(monkeypatch, tmp_path):
    # Four records total, batch size forced to 3 -- exercises both a full-size flush and a
    # final partial flush, and confirms the second batch APPENDS rather than overwriting.
    monkeypatch.setattr(uniprot, '_BATCH_SIZE', 3)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    uniprot_dir = tmp_path / 'uniprot'
    uniprot_dir.mkdir()
    _write_gz(
        uniprot_dir / 'uniprot_sprot.dat.gz',
        REVIEWED_HUMAN_RECORD + UNREVIEWED_NONHUMAN_RECORD + UNREVIEWED_WITH_EVIDENCE_TAGS_RECORD,
    )
    _write_gz(uniprot_dir / 'uniprot_trembl.dat.gz', NO_ACCESSION_MALFORMED_RECORD)

    await uniprot.load_vocabulary_from_file(offline=True, build_search_index=False)

    doc_dump_path = tmp_path / 'offline' / 'uniprot.doc.dump'
    doc_lines = doc_dump_path.read_text().strip().splitlines()
    # 3 valid records from sprot + 0 from trembl (its only record has no accession) = 3.
    assert len(doc_lines) == 3

    node_id_path = tmp_path / 'offline' / 'uniprot.node_ids.dump'
    node_ids = [line.split(',')[0] for line in node_id_path.read_text().strip().splitlines()]
    assert set(node_ids) == {'P68104', 'P0DTD1', 'A0A0A0MS99'}

    annotation_path = tmp_path / 'offline' / 'uniprot-gene.annotation.dump'
    annotation_lines = annotation_path.read_text().strip().splitlines()
    # Only P68104 carries a DR HGNC line in these fixtures; P0DTD1 (SARS-CoV-2) and
    # A0A0A0MS99 (deliberately missing one, to isolate the evidence-tag-stripping fixture
    # from HGNC extraction, already covered by the reviewed-human case) do not.
    assert len(annotation_lines) == 1


@pytest.mark.asyncio
async def test_load_vocabulary_from_file_skips_gene_annotation_when_gene_symbol_absent(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    uniprot_dir = tmp_path / 'uniprot'
    uniprot_dir.mkdir()
    _write_gz(uniprot_dir / 'uniprot_sprot.dat.gz', REVIEWED_HUMAN_RECORD)
    _write_gz(uniprot_dir / 'uniprot_trembl.dat.gz', '')

    class FakeDocDb:
        saved = 0

        async def save_terms(self, terms, no_upsert=False):
            self.saved += len(terms)

    class FakeGraphDb:
        saved_annotations = []

        async def count_terms(self, prefix):
            return 0

        async def save_vocabulary_graph(self, concepts, graph):
            pass

        async def save_annotations(self, annotations):
            self.saved_annotations.extend(annotations)

    doc_db = FakeDocDb()
    graph_db = FakeGraphDb()
    await uniprot.load_vocabulary_from_file(doc_db=doc_db, graph_db=graph_db, offline=False)

    assert doc_db.saved == 1
    assert graph_db.saved_annotations == []


@pytest.mark.asyncio
async def test_load_vocabulary_from_file_loads_gene_annotation_when_gene_symbol_present(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    uniprot_dir = tmp_path / 'uniprot'
    uniprot_dir.mkdir()
    _write_gz(uniprot_dir / 'uniprot_sprot.dat.gz', REVIEWED_HUMAN_RECORD)
    _write_gz(uniprot_dir / 'uniprot_trembl.dat.gz', '')

    class FakeDocDb:
        async def save_terms(self, terms, no_upsert=False):
            pass

    class FakeGraphDb:
        def __init__(self):
            self.saved_annotations = []

        async def count_terms(self, prefix):
            return 1

        async def save_vocabulary_graph(self, concepts, graph):
            pass

        async def save_annotations(self, annotations):
            self.saved_annotations.extend(annotations)

    graph_db = FakeGraphDb()
    await uniprot.load_vocabulary_from_file(
        doc_db=FakeDocDb(),
        graph_db=graph_db,
        offline=False,
    )

    assert len(graph_db.saved_annotations) == 1
    assert graph_db.saved_annotations[0].concept_id_to == 'EEF1A1'


@pytest.mark.asyncio
async def test_load_vocabulary_from_file_skips_gene_symbol_check_offline(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    uniprot_dir = tmp_path / 'uniprot'
    uniprot_dir.mkdir()
    _write_gz(uniprot_dir / 'uniprot_sprot.dat.gz', REVIEWED_HUMAN_RECORD)
    _write_gz(uniprot_dir / 'uniprot_trembl.dat.gz', '')

    await uniprot.load_vocabulary_from_file(offline=True, build_search_index=False)  # must not raise


@pytest.mark.asyncio
async def test_load_vocabulary_from_file_no_annotation_skips_offline_dump(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    uniprot_dir = tmp_path / 'uniprot'
    uniprot_dir.mkdir()
    _write_gz(uniprot_dir / 'uniprot_sprot.dat.gz', REVIEWED_HUMAN_RECORD)
    _write_gz(uniprot_dir / 'uniprot_trembl.dat.gz', '')

    async def ignore_write(**_kwargs):
        pass

    async def fail_annotation_write(**_kwargs):
        raise AssertionError('annotation dump must not be written')

    monkeypatch.setattr(uniprot, 'write_concepts_to_file', ignore_write)
    monkeypatch.setattr(uniprot, 'write_graph_to_file', ignore_write)
    monkeypatch.setattr(uniprot, 'write_annotations_to_file', fail_annotation_write)

    await uniprot.load_vocabulary_from_file(
        offline=True,
        build_search_index=False,
        load_annotations=False,
    )

    assert not (tmp_path / 'offline' / 'uniprot-gene.annotation.dump').exists()


@pytest.mark.asyncio
async def test_download_vocabulary_skips_files_that_already_exist(monkeypatch, tmp_path):
    # Deliberate product choice (kept as-is on request): a file already present at its path
    # is treated as already downloaded and is not re-fetched.
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    uniprot_dir = tmp_path / 'uniprot'
    uniprot_dir.mkdir()
    (uniprot_dir / 'uniprot_sprot.dat.gz').write_bytes(b'')

    downloaded = []

    async def fake_download_file(url, file_path, download_client=None, **kwargs):
        downloaded.append((url, file_path))
        full_path = tmp_path / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(b'')

    monkeypatch.setattr(uniprot, 'download_file', fake_download_file)

    await uniprot.download_vocabulary()

    assert len(downloaded) == 1
    url, file_path = downloaded[0]
    assert file_path == 'uniprot/uniprot_trembl.dat.gz'
    assert url == (
        'https://ftp.uniprot.org/pub/databases/uniprot/current_release/'
        'knowledgebase/complete/uniprot_trembl.dat.gz'
    )
