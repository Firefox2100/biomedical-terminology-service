import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus, ConceptType
from bioterms.vocabulary import get_vocabulary_config, get_vocabulary_license
import bioterms.vocabulary.rxnorm as rxnorm


def _rrf(*fields):
    return '|'.join(fields) + '|\n'


def _write_release(tmp_path):
    directory = tmp_path / 'rxnorm'
    directory.mkdir()
    (directory / 'RXNCONSO.RRF').write_text(
        _rrf('100', 'ENG', '', '', '', '', 'Y', 'a1', '', '', '', 'RXNORM',
             'IN', '100', 'Example ingredient', '', 'N', '')
        + _rrf('100', 'ENG', '', '', '', '', 'N', 'a2', '', '', '', 'RXNORM',
               'SY', '100', 'Ingredient synonym', '', 'N', '')
        + _rrf('200', 'ENG', '', '', '', '', 'Y', 'a3', '', '', '', 'RXNORM',
               'SCD', '200', 'Example clinical drug', '', 'N', '')
        + _rrf('300', 'ENG', '', '', '', '', 'Y', 'a4', '', '', '', 'RXNORM',
               'BN', '300', 'Retired brand', '', 'O', '')
        + _rrf('100', 'ENG', '', '', '', '', 'Y', 'a5', '', '', '', 'SNOMEDCT_US',
               'PT', '123456', 'External atom', '', 'N', '')
    )
    (directory / 'RXNREL.RRF').write_text(
        _rrf('100', '', 'CUI', 'RN', '200', '', 'CUI', 'isa', '', '', 'RXNORM',
             '', '', '', 'N', '')
        + _rrf('100', '', 'CUI', 'RO', '200', '', 'CUI', 'has_ingredient', '', '',
               'RXNORM', '', '', '', 'N', '')
    )
    (directory / 'RXNCUI.RRF').write_text(
        _rrf('300', 'old', 'current', '1', '200')
    )


def test_rxnorm_is_registered_with_annotations_and_license():
    config = get_vocabulary_config(ConceptPrefix.RXNORM)

    assert config['filePaths'] == rxnorm.FILE_PATHS
    assert config['annotations'] == [
        ConceptPrefix.LOINC, ConceptPrefix.OHDSI, ConceptPrefix.SNOMED,
    ]
    assert 'UMLS' in get_vocabulary_license(ConceptPrefix.RXNORM)


@pytest.mark.asyncio
async def test_downloader_requires_umls_api_key(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    monkeypatch.setattr(CONFIG, 'nih_umls_api_key', None)

    with pytest.raises(ValueError, match='UMLS API key'):
        await rxnorm.download_vocabulary()


def test_concepts_and_relationships_follow_rrf_direction(monkeypatch, tmp_path):
    _write_release(tmp_path)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    concepts = rxnorm.load_rxnorm_concepts()
    graph = rxnorm._load_graph(concepts)

    assert concepts['100'].label == 'Example ingredient'
    assert concepts['100'].synonyms == ['Ingredient synonym']
    assert ConceptType.INGREDIENT in concepts['100'].concept_types
    assert ConceptType.CLINICAL_DRUG in concepts['200'].concept_types
    assert concepts['300'].status == ConceptStatus.DEPRECATED
    assert graph.edges['200', '100', 'isa']['label'] == ConceptRelationshipType.IS_A
    assert graph.edges['200', '100', 'has_ingredient']['label'] \
        == ConceptRelationshipType.RXNORM_RELATIONSHIP
    assert graph.edges['300', '200', 'replaced_by']['label'] \
        == ConceptRelationshipType.REPLACED_BY


def test_concept_label_prefers_preferred_then_active_then_first_atom(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    directory = tmp_path / 'rxnorm'
    directory.mkdir()

    def atom(rxcui, ispref, tty, text, suppress):
        return _rrf(rxcui, 'ENG', '', '', '', '', ispref, '', '', '', '', 'RXNORM',
                    tty, rxcui, text, '', suppress, '')

    (directory / 'RXNCONSO.RRF').write_text(
        # 1: a later preferred active atom wins over earlier active and suppressed ones.
        atom('1', 'Y', 'SY', 'Suppressed first', 'O')
        + atom('1', 'N', 'SY', 'Active synonym', 'N')
        + atom('1', 'Y', 'IN', ' Preferred name ', 'N')
        + atom('1', 'Y', 'IN', 'Active synonym', 'N')
        # 2: no preferred atom, so the first active atom is the label.
        + atom('2', 'N', 'SY', '  ', 'N')
        + atom('2', 'N', 'SCD', 'First active', 'N')
        # 3: every atom suppressed, so the concept is deprecated with its first label.
        + atom('3', 'Y', 'BN', 'Old brand', 'O')
        + atom('3', 'Y', 'BN', 'Older brand', 'E')
    )

    concepts = rxnorm.load_rxnorm_concepts()

    assert concepts['1'].label == 'Preferred name'
    assert concepts['1'].synonyms == ['Suppressed first', 'Active synonym']
    assert concepts['1'].status == ConceptStatus.ACTIVE
    assert concepts['2'].label == 'First active'
    assert concepts['2'].synonyms is None
    assert concepts['3'].label == 'Old brand'
    assert concepts['3'].synonyms == ['Older brand']
    assert concepts['3'].status == ConceptStatus.DEPRECATED
