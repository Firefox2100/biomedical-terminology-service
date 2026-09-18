import types

import pytest

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus
from bioterms.vocabulary import get_vocabulary_config, get_vocabulary_license
import bioterms.vocabulary.go as go


class FakeThing:
    def __init__(self, name, **attributes):
        self.name = name
        for key, value in attributes.items():
            setattr(self, key, value)


class FakeRestriction:
    def __init__(self, property_name, value):
        self.property = types.SimpleNamespace(name=property_name)
        self.value = value


def _go_class(name, **overrides):
    values = {
        'label': ['example process'], 'IAO_0000115': ['A biological process.'],
        'comment': [], 'deprecated': [], 'hasExactSynonym': ['example'],
        'hasBroadSynonym': [], 'hasNarrowSynonym': [], 'hasRelatedSynonym': [],
        'is_a': [], 'hasAlternativeId': [], 'IAO_0100001': [], 'consider': [],
    }
    values.update(overrides)
    return FakeThing(name, **values)


def test_go_is_registered_with_expected_annotations_and_license():
    config = get_vocabulary_config(ConceptPrefix.GO)

    assert config['filePaths'] == ['go/go-basic.owl']
    assert config['annotations'] == [
        ConceptPrefix.REACTOME, ConceptPrefix.UBERON, ConceptPrefix.UNIPROT,
    ]
    assert 'Creative Commons' in get_vocabulary_license(ConceptPrefix.GO)


def test_process_go_class_preserves_hierarchy_safe_relations(monkeypatch):
    monkeypatch.setattr(go, 'ThingClass', FakeThing)
    monkeypatch.setattr(go, 'Restriction', FakeRestriction)
    ontology_class = _go_class(
        'GO_0000001',
        is_a=[
            _go_class('GO_0000002'),
            FakeRestriction('BFO_0000050', _go_class('GO_0000003')),
            FakeRestriction('RO_0002211', _go_class('GO_0000004')),
            FakeRestriction('RO_0002212', _go_class('GO_0000005')),
            FakeRestriction('RO_0002213', _go_class('GO_0000006')),
            FakeRestriction('RO_0001025', _go_class('GO_0000007')),
        ],
        hasAlternativeId=['GO:9999999'],
        IAO_0100001=[_go_class('GO_0000008'), _go_class('UBERON_0000001')],
        consider=['GO:0000009'],
    )

    concept, relationships = go._process_go_class(ontology_class)

    assert concept.concept_id == '0000001'
    assert concept.synonyms == ['example']
    assert concept.status == ConceptStatus.ACTIVE
    assert relationships == [
        ('0000001', '0000002', ConceptRelationshipType.IS_A),
        ('0000001', '0000003', ConceptRelationshipType.PART_OF),
        ('0000001', '0000004', ConceptRelationshipType.REGULATES),
        ('0000001', '0000005', ConceptRelationshipType.NEGATIVELY_REGULATES),
        ('0000001', '0000006', ConceptRelationshipType.POSITIVELY_REGULATES),
        ('9999999', '0000001', ConceptRelationshipType.REPLACED_BY),
        ('0000001', '0000008', ConceptRelationshipType.REPLACED_BY),
        ('0000001', '0000009', ConceptRelationshipType.CONSIDER),
    ]


@pytest.mark.asyncio
async def test_go_offline_load_writes_multigraph(monkeypatch):
    ontology_class = _go_class(
        'GO_0000001',
        is_a=[
            _go_class('GO_0000002'),
            FakeRestriction('BFO_0000050', _go_class('GO_0000002')),
        ],
    )
    monkeypatch.setattr(go, 'ThingClass', FakeThing)
    monkeypatch.setattr(go, 'Restriction', FakeRestriction)
    monkeypatch.setattr(go, 'check_files_exist', lambda _paths: True)
    monkeypatch.setattr(go, 'load_obo_owl_classes', lambda *_args: (object(), [ontology_class]))
    captured = {}

    async def capture_concepts(prefix, concepts, **_kwargs):
        captured['concepts'] = concepts

    async def capture_graph(prefix, concepts, vocabulary_graph):
        captured['graph'] = vocabulary_graph

    monkeypatch.setattr(go, 'write_concepts_to_file', capture_concepts)
    monkeypatch.setattr(go, 'write_graph_to_file', capture_graph)

    await go.load_vocabulary_from_file(offline=True, build_search_index=False)

    assert [concept.concept_id for concept in captured['concepts']] == ['0000001']
    assert captured['graph'].number_of_edges('0000001', '0000002') == 2
