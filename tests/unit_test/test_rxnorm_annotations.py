import pytest

from bioterms.annotation import loinc_rxnorm, rxnorm_snomed
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix


def _rrf(*fields):
    return '|'.join(fields) + '|\n'


def _write_release(tmp_path):
    directory = tmp_path / 'rxnorm'
    directory.mkdir()
    (directory / 'RXNCONSO.RRF').write_text(
        _rrf('100', 'ENG', '', '', '', '', 'Y', 'a1', '', '', '', 'RXNORM',
             'IN', '100', 'Example ingredient', '', 'N', '')
        + _rrf('100', 'ENG', '', '', '', '', 'Y', 'a2', '', '', '', 'SNOMEDCT_US',
               'PT', '123456', 'External atom', '', 'N', '')
    )
    (directory / 'RXNREL.RRF').write_text('')
    (directory / 'RXNCUI.RRF').write_text('')


class FakeGraphDb:
    def __init__(self):
        self.annotations = []

    async def count_terms(self, _prefix=None, **_kwargs):
        return 1

    async def count_annotations(self, _prefix_1=None, _prefix_2=None, **_kwargs):
        return 0

    async def save_annotations(self, annotations):
        self.annotations.extend(annotations)


@pytest.mark.asyncio
async def test_rxnorm_snomed_mapping_uses_rxnorm_publisher_direction(monkeypatch, tmp_path):
    _write_release(tmp_path)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await rxnorm_snomed.load_annotation_from_file(graph_db)

    assert len(graph_db.annotations) == 1
    annotation = graph_db.annotations[0]
    assert (annotation.prefix_from, annotation.concept_id_from) == (ConceptPrefix.RXNORM, '100')
    assert (annotation.prefix_to, annotation.concept_id_to) == (ConceptPrefix.SNOMED, '123456')
    assert annotation.properties == {
        'sourceVocabulary': 'SNOMEDCT_US', 'source': 'NLM RxNorm full release',
    }


@pytest.mark.asyncio
async def test_loinc_rxnorm_mapping_uses_loinc_publisher_direction(monkeypatch, tmp_path):
    directory = tmp_path / 'loinc'
    directory.mkdir()
    (directory / 'PartRelatedCodeMapping.csv').write_text(
        'PartNumber,ExtCodeId,ExtCodeSystem,Equivalence\n'
        'LP1-1,100,http://www.nlm.nih.gov/research/umls/rxnorm,equivalent\n'
        'LP1-1,999,http://snomed.info/sct,equivalent\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await loinc_rxnorm.load_annotation_from_file(graph_db)

    assert len(graph_db.annotations) == 1
    annotation = graph_db.annotations[0]
    assert (annotation.prefix_from, annotation.concept_id_from) == (ConceptPrefix.LOINC, 'LP1-1')
    assert (annotation.prefix_to, annotation.concept_id_to) == (ConceptPrefix.RXNORM, '100')
    assert annotation.properties == {
        'equivalence': 'equivalent', 'source': 'LOINC PartRelatedCodeMapping',
    }
