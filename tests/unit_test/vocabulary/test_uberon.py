import types

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus
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


def test_uberon_is_registered_as_a_vocabulary():
    config = get_vocabulary_config(ConceptPrefix.UBERON)

    assert config['prefix'] == ConceptPrefix.UBERON
    assert config['filePaths'] == ['uberon/uberon.owl']
    assert 'Creative Commons Attribution 3.0' in get_vocabulary_license(ConceptPrefix.UBERON)


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
