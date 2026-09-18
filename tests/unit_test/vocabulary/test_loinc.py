import hashlib
import shutil
import zipfile

import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus
from bioterms.vocabulary import get_vocabulary_config, get_vocabulary_license
from bioterms.annotation import loinc_snomed
import bioterms.vocabulary.loinc as loinc


class FakeResponse:
    def __init__(self, metadata):
        self.metadata = metadata

    def raise_for_status(self):
        return None

    def json(self):
        return self.metadata


class FakeClient:
    def __init__(self, metadata):
        self.metadata = metadata
        self.calls = []

    async def get(self, url, headers=None):
        self.calls.append((url, headers))
        return FakeResponse(self.metadata)


def _write_release_files(tmp_path):
    release = tmp_path / 'loinc'
    release.mkdir()
    (release / 'Loinc.csv').write_text(
        'LOINC_NUM,LONG_COMMON_NAME,SHORTNAME,CONSUMER_NAME,DisplayName,'
        'RELATEDNAMES2,DefinitionDescription,STATUS\n'
        '1000-1,Example measurement,Example short,Consumer name,Display name,'
        'Alias one;Alias two,An example definition,ACTIVE\n'
        '1000-2,Old measurement,Old short,,,,,DEPRECATED\n'
    )
    (release / 'MapTo.csv').write_text(
        'LOINC,MAP_TO,COMMENT\n1000-2,1000-1,Use the active replacement\n'
    )
    (release / 'Part.csv').write_text(
        'PartNumber,PartName,PartTypeName,Status\n'
        'LP1-1,Example component,COMPONENT,ACTIVE\n'
    )
    (release / 'ComponentHierarchyBySystem.csv').write_text(
        'PATH_TO_ROOT,SEQUENCE,IMMEDIATE_PARENT,CODE,CODE_TEXT\n'
        'LP1-1,1,,LP1-1,Example component\n'
        'LP1-1.1000-1,1,LP1-1,1000-1,Example measurement\n'
    )
    (release / 'PartRelatedCodeMapping.csv').write_text(
        'PartNumber,ExtCodeId,ExtCodeSystem,Equivalence,ContentOrigin,'
        'ExtCodeSystemVersion\n'
        'LP1-1,123456,http://snomed.info/sct,equivalent,LN,2026\n'
    )
    (release / 'license.txt').write_text('Official release licence')


def test_loinc_is_registered_with_license_and_annotations():
    config = get_vocabulary_config(ConceptPrefix.LOINC)

    assert config['filePaths'] == loinc.FILE_PATHS
    assert config['annotations'] == [ConceptPrefix.RXNORM, ConceptPrefix.SNOMED]
    assert 'does not grant a LOINC licence' in get_vocabulary_license(ConceptPrefix.LOINC)


@pytest.mark.asyncio
async def test_downloader_requires_credentials(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    monkeypatch.setattr(CONFIG, 'loinc_username', None)
    monkeypatch.setattr(CONFIG, 'loinc_password', None)

    with pytest.raises(ValueError, match='BTS_LOINC_USERNAME'):
        await loinc.download_vocabulary()


@pytest.mark.asyncio
async def test_downloader_authenticates_verifies_and_extracts(monkeypatch, tmp_path):
    source_archive = tmp_path / 'source.zip'
    with zipfile.ZipFile(source_archive, 'w') as archive:
        archive.writestr('LoincTable/Loinc.csv', 'LOINC_NUM\n1000-1\n')
        archive.writestr('LoincTable/MapTo.csv', 'LOINC,MAP_TO\n1000-2,1000-1\n')
        archive.writestr('AccessoryFiles/PartFile/Part.csv', 'PartNumber\nLP1-1\n')
        archive.writestr(
            'AccessoryFiles/ComponentHierarchyBySystem/ComponentHierarchyBySystem.csv',
            'CODE,IMMEDIATE_PARENT\n1000-1,LP1-1\n',
        )
        archive.writestr(
            'AccessoryFiles/PartFile/PartRelatedCodeMapping.csv',
            'PartNumber,ExtCodeId,ExtCodeSystem\nLP1-1,123456,http://snomed.info/sct\n',
        )
        archive.writestr('LoincLicense_6.0.txt', 'Official licence')
    checksum = hashlib.md5(source_archive.read_bytes(), usedforsecurity=False).hexdigest()
    client = FakeClient({
        'version': '2.83', 'downloadUrl': 'https://example.test/loinc.zip',
        'downloadMD5Hash': checksum,
    })
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path / 'data'))
    monkeypatch.setattr(CONFIG, 'loinc_username', 'member')
    monkeypatch.setattr(CONFIG, 'loinc_password', 'secret')
    download_calls = []

    async def fake_download(url, file_path, headers=None, download_client=None):
        download_calls.append((url, file_path, headers, download_client))
        destination = tmp_path / 'data' / file_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_archive, destination)

    monkeypatch.setattr(loinc, 'download_file', fake_download)
    await loinc.download_vocabulary(download_client=client)

    assert client.calls[0][0] == loinc._API_URL
    assert client.calls[0][1]['Authorization'].startswith('Basic ')
    assert download_calls[0][2] == client.calls[0][1]
    assert all((tmp_path / 'data' / path).is_file() for path in loinc.FILE_PATHS)
    assert not (tmp_path / 'data' / loinc._ARCHIVE_PATH).exists()


@pytest.mark.asyncio
async def test_load_loinc_terms_parts_hierarchy_and_replacement(monkeypatch, tmp_path):
    _write_release_files(tmp_path)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    captured = {}

    async def capture_concepts(prefix, concepts, **_kwargs):
        captured['concepts'] = concepts

    async def capture_graph(prefix, concepts, vocabulary_graph):
        captured['graph'] = vocabulary_graph

    monkeypatch.setattr(loinc, 'write_concepts_to_file', capture_concepts)
    monkeypatch.setattr(loinc, 'write_graph_to_file', capture_graph)

    await loinc.load_vocabulary_from_file(offline=True, build_search_index=False)

    concepts = {concept.concept_id: concept for concept in captured['concepts']}
    assert concepts['1000-1'].label == 'Example measurement'
    assert concepts['1000-1'].definition == 'An example definition'
    assert concepts['1000-1'].synonyms == [
        'Example short', 'Consumer name', 'Display name', 'Alias one', 'Alias two',
    ]
    assert concepts['1000-2'].status == ConceptStatus.DEPRECATED
    assert concepts['LP1-1'].label == 'Example component'
    graph = captured['graph']
    assert graph.edges['1000-1', 'LP1-1', 'is_a']['label'] == ConceptRelationshipType.IS_A
    assert graph.edges['1000-2', '1000-1', 'replaced_by']['label'] \
        == ConceptRelationshipType.REPLACED_BY


class FakeGraphDb:
    def __init__(self):
        self.annotations = []

    async def count_terms(self, _prefix=None, **_kwargs):
        return 1

    async def count_annotations(self, *_args, **_kwargs):
        return 0

    async def save_annotations(self, annotations):
        self.annotations.extend(annotations)


@pytest.mark.asyncio
async def test_loinc_snomed_mapping_is_publisher_directed(monkeypatch, tmp_path):
    _write_release_files(tmp_path)
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    graph_db = FakeGraphDb()

    await loinc_snomed.load_annotation_from_file(graph_db)

    assert len(graph_db.annotations) == 1
    annotation = graph_db.annotations[0]
    assert annotation.prefix_from == ConceptPrefix.LOINC
    assert annotation.concept_id_from == 'LP1-1'
    assert annotation.prefix_to == ConceptPrefix.SNOMED
    assert annotation.concept_id_to == '123456'
    assert annotation.properties == {
        'mapType': 'equivalent', 'contentOrigin': 'LN', 'systemVersion': '2026',
        'source': 'LOINC PartRelatedCodeMapping',
    }
