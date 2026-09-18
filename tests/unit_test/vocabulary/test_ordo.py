import types

from bioterms.etc.enums import ConceptRelationshipType, ConceptStatus
import bioterms.vocabulary.ordo as ordo


class FakeThing:
    def __init__(self, name, **attributes):
        self.name = name
        for key, value in attributes.items():
            setattr(self, key, value)


class FakeRestriction:
    def __init__(self, property_name, value):
        self.property = types.SimpleNamespace(name=property_name)
        self.value = value


class FakePartOf:
    def __init__(self, values):
        self.values = values

    def __getitem__(self, _ontology_class):
        return self.values


def test_process_ordo_class_preserves_is_a_part_of_and_moved_to(monkeypatch):
    monkeypatch.setattr(ordo, 'ThingClass', FakeThing)
    monkeypatch.setattr(ordo, 'Restriction', FakeRestriction)
    ontology_class = FakeThing(
        'Orphanet_123', label=['Example disease'], definition=['Definition'],
        alternative_term=['Alias'],
        is_a=[
            FakeThing('Orphanet_1'),
            FakeThing('Thing'),
            FakeRestriction('Orphanet_C056', FakeThing('Orphanet_456')),
        ],
    )

    concept, relationships = ordo._process_ordo_class(
        ontology_class, FakePartOf([FakeThing('Orphanet_789')]),
    )

    assert concept.concept_id == '123'
    assert concept.label == 'Example disease'
    assert concept.synonyms == ['Alias']
    assert concept.status == ConceptStatus.DEPRECATED
    assert relationships == [
        ('123', '1', ConceptRelationshipType.IS_A),
        ('123', '456', ConceptRelationshipType.REPLACED_BY),
        ('123', '789', ConceptRelationshipType.PART_OF),
    ]
