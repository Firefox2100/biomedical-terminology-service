import types

import pytest

from bioterms.annotation import get_annotation_config, ncit_uberon, snomed_uberon
from bioterms.annotation import utils as annotation_utils
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix, ConceptRelationshipType, ConceptStatus
from bioterms.vocabulary import get_vocabulary_config, get_vocabulary_license
import bioterms.vocabulary.uberon as uberon


class FakeThing:
    def __init__(self, name, **attributes):
        self.name = name
        for key, value in attributes.items():
            setattr(self, key, value)


class FakeRestriction:
    def __init__(self, property_name, value):
        self.property = types.SimpleNamespace(name=property_name)
        self.value = value


class FakeGraphDb:
    def __init__(self):
        self.annotations = []

    async def count_terms(self, prefix):
        return 1

    async def count_annotations(self, prefix_1, prefix_2):
        return 0

    async def save_annotations(self, annotations):
        self.annotations.extend(annotations)


def test_uberon_is_registered_as_a_vocabulary():
    config = get_vocabulary_config(ConceptPrefix.UBERON)

    assert config['prefix'] == ConceptPrefix.UBERON
    assert config['filePaths'] == ['uberon/uberon.owl']
    assert config['annotations'] == [
        ConceptPrefix.GO, ConceptPrefix.NCIT, ConceptPrefix.SNOMED,
    ]
    assert 'Creative Commons Attribution 3.0' in get_vocabulary_license(ConceptPrefix.UBERON)
    assert 'GO, NCIt, and SNOMED CT identifiers' in get_vocabulary_license(ConceptPrefix.UBERON)


def test_process_uberon_class_preserves_obo_relationship_semantics(monkeypatch):
    monkeypatch.setattr(uberon, 'ThingClass', FakeThing)
    monkeypatch.setattr(uberon, 'Restriction', FakeRestriction)

    ontology_class = FakeThing(
        'UBERON_0000948',
        label=['heart'],
        IAO_0000115=['A muscular organ.'],
        comment=[],
        deprecated=[],
        hasExactSynonym=['cardiac organ'],
        hasBroadSynonym=[],
        hasNarrowSynonym=[],
        hasRelatedSynonym=[],
        is_a=[
            FakeThing('UBERON_0000062'),
            FakeThing('GO_0008150'),
            FakeRestriction('BFO_0000050', FakeThing('UBERON_0001009')),
            FakeRestriction('RO_0002202', FakeThing('UBERON_0003071')),
        ],
        hasAlternativeId=['UBERON:9999999'],
        IAO_0100001=[],
        consider=[],
    )

    concept, relationships = uberon._process_uberon_class(ontology_class)

    assert concept.concept_id == '0000948'
    assert concept.label == 'heart'
    assert concept.synonyms == ['cardiac organ']
    assert concept.status == ConceptStatus.ACTIVE
    assert relationships == [
        ('0000948', '0000062', ConceptRelationshipType.IS_A),
        ('0000948', '0001009', ConceptRelationshipType.PART_OF),
        ('9999999', '0000948', ConceptRelationshipType.REPLACED_BY),
    ]


def test_deprecated_uberon_replacements_and_consider_stay_in_namespace(monkeypatch):
    monkeypatch.setattr(uberon, 'ThingClass', FakeThing)
    monkeypatch.setattr(uberon, 'Restriction', FakeRestriction)
    ontology_class = FakeThing(
        'UBERON_0000027',
        label=['obsolete term'],
        IAO_0000115=[],
        comment=[],
        deprecated=[True],
        hasExactSynonym=[],
        hasBroadSynonym=[],
        hasNarrowSynonym=[],
        hasRelatedSynonym=[],
        is_a=[],
        hasAlternativeId=[],
        IAO_0100001=[FakeThing('UBERON_0001466'), FakeThing('CL_0000000')],
        consider=['UBERON:0000952', 'FMA:1234'],
    )

    concept, relationships = uberon._process_uberon_class(ontology_class)

    assert concept.status == ConceptStatus.DEPRECATED
    assert relationships == [
        ('0000027', '0001466', ConceptRelationshipType.REPLACED_BY),
        ('0000027', '0000952', ConceptRelationshipType.CONSIDER),
    ]


def test_generic_obo_xref_loader_filters_namespace_and_deduplicates(monkeypatch):
    classes = [
        FakeThing('UBERON_0000948', hasDbXref=['NCIT:C12727', 'NCIT:C12727', 'FMA:7088']),
        FakeThing('UBERON_0002107', hasDbXref=['NCIT:C12391']),
    ]
    monkeypatch.setattr(
        annotation_utils,
        'load_obo_owl_classes',
        lambda file_path, class_name_prefix: (object(), classes),
    )

    annotations = annotation_utils.load_obo_xref_annotations(
        'uberon/uberon.owl', ConceptPrefix.UBERON, 'UBERON', 'NCIT', ConceptPrefix.NCIT,
        'Uberon hasDbXref',
    )

    assert [(a.concept_id_from, a.concept_id_to) for a in annotations] == [
        ('0000948', 'C12727'), ('0002107', 'C12391'),
    ]
    assert all(a.prefix_from == ConceptPrefix.UBERON for a in annotations)
    assert all(a.prefix_to == ConceptPrefix.NCIT for a in annotations)
    assert all(a.annotation_type == AnnotationType.EXACT for a in annotations)
    assert all(a.properties == {'source': 'Uberon hasDbXref'} for a in annotations)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('module', 'target_prefix', 'xref_prefix'),
    [
        (ncit_uberon, ConceptPrefix.NCIT, 'NCIT'),
        (snomed_uberon, ConceptPrefix.SNOMED, 'SCTID'),
    ],
)
async def test_uberon_xref_annotation_modules_are_publisher_directed(
    monkeypatch, tmp_path, module, target_prefix, xref_prefix,
):
    uberon_dir = tmp_path / 'uberon'
    uberon_dir.mkdir()
    (uberon_dir / 'uberon.owl').write_text('fixture')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    received = []

    def fake_loader(file_path, publisher_prefix, publisher_class_prefix, raw_xref_prefix,
                    received_target_prefix, source_name):
        received.append((
            file_path, publisher_prefix, publisher_class_prefix, raw_xref_prefix,
            received_target_prefix, source_name,
        ))
        return []

    monkeypatch.setattr(module, 'load_obo_xref_annotations', fake_loader)
    graph_db = FakeGraphDb()

    await module.load_annotation_from_file(graph_db)

    assert received == [(
        'uberon/uberon.owl', ConceptPrefix.UBERON, 'UBERON', xref_prefix,
        target_prefix, 'Uberon hasDbXref',
    )]
    config = get_annotation_config(target_prefix, ConceptPrefix.UBERON)
    assert config['prefix1'] == ConceptPrefix.UBERON
    assert config['prefix2'] == target_prefix
