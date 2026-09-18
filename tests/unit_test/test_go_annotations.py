import gzip

import pytest

from bioterms.annotation import go_reactome, go_uberon, go_uniprot
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.model.annotation import Annotation
from bioterms.vocabulary.uniprot import iter_go_annotations


class FakeGraphDb:
    def __init__(self):
        self.batches = []

    async def count_terms(self, _prefix=None, **_kwargs):
        return 1

    async def count_annotations(self, _prefix_1=None, _prefix_2=None, **_kwargs):
        return 0

    async def save_annotations(self, annotations):
        self.batches.append(list(annotations))


def _write_uniprot_release(tmp_path):
    directory = tmp_path / 'uniprot'
    directory.mkdir()
    record = """\
ID   TEST_HUMAN Reviewed; 10 AA.
AC   P12345;
DR   GO; GO:0005737; C:cytoplasm; IDA:UniProtKB.
DR   GO; GO:0005737; C:cytoplasm; IEA:UniProtKB-KW.
DR   GO; GO:0003674; F:molecular_function; ND:UniProtKB.
//
"""
    for filename, content in (
        ('uniprot_sprot.dat.gz', record), ('uniprot_trembl.dat.gz', ''),
    ):
        with gzip.open(directory / filename, 'wt', encoding='utf-8') as stream:
            stream.write(content)


def test_uniprot_go_parser_deduplicates_and_preserves_metadata(monkeypatch, tmp_path):
    _write_uniprot_release(tmp_path)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    annotations = list(iter_go_annotations())

    assert [(a.concept_id_from, a.concept_id_to) for a in annotations] == [
        ('P12345', '0005737'), ('P12345', '0003674'),
    ]
    assert annotations[0].annotation_type == AnnotationType.ANNOTATED_WITH
    assert annotations[0].properties == {
        'aspect': 'cellular_component', 'term': 'cytoplasm',
        'evidence': 'IDA:UniProtKB;IEA:UniProtKB-KW',
        'source': 'UniProtKB GO cross-reference',
    }


@pytest.mark.asyncio
async def test_explicit_uniprot_go_load_restreams_release(monkeypatch, tmp_path):
    _write_uniprot_release(tmp_path)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await go_uniprot.load_annotation_from_file(graph_db)

    assert sum(map(len, graph_db.batches)) == 2


@pytest.mark.asyncio
async def test_uberon_go_loader_uses_uberon_as_publisher(monkeypatch, tmp_path):
    directory = tmp_path / 'uberon'
    directory.mkdir()
    (directory / 'uberon.owl').write_text('fixture')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    calls = []

    def fake_loader(*args):
        calls.append(args)
        return []

    monkeypatch.setattr(go_uberon, 'load_obo_xref_annotations', fake_loader)
    await go_uberon.load_annotation_from_file(FakeGraphDb())

    assert calls == [(
        'uberon/uberon.owl', ConceptPrefix.UBERON, 'UBERON', 'GO', ConceptPrefix.GO,
        'Uberon hasDbXref',
    )]


@pytest.mark.asyncio
async def test_go_reactome_keeps_both_publishers_and_directions(monkeypatch, tmp_path):
    go_dir = tmp_path / 'go'
    go_dir.mkdir()
    (go_dir / 'go-basic.owl').write_text('fixture')
    reactome_dir = tmp_path / 'reactome'
    reactome_dir.mkdir()
    (reactome_dir / 'go_mapping.csv').write_text(
        'reactome_id,external_id,source_relation\n'
        'R-HSA-1,0008150,goBiologicalProcess\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    go_annotation = Annotation(
        prefixFrom=ConceptPrefix.GO, conceptIdFrom='0008150',
        prefixTo=ConceptPrefix.REACTOME, conceptIdTo='R-HSA-2',
        annotationType=AnnotationType.EXACT, properties={'source': 'GO hasDbXref'},
    )
    monkeypatch.setattr(go_reactome, 'load_obo_xref_annotations', lambda *_args: [go_annotation])
    graph_db = FakeGraphDb()

    await go_reactome.load_annotation_from_file(graph_db)

    annotations = [annotation for batch in graph_db.batches for annotation in batch]
    assert [(a.prefix_from, a.prefix_to) for a in annotations] == [
        (ConceptPrefix.GO, ConceptPrefix.REACTOME),
        (ConceptPrefix.REACTOME, ConceptPrefix.GO),
    ]
    assert annotations[1].annotation_type == AnnotationType.ANNOTATED_WITH
    assert annotations[1].properties == {
        'sourceRelation': 'goBiologicalProcess', 'source': 'Reactome GO assignment',
    }
