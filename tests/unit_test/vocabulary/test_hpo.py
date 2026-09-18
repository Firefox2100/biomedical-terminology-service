import types
import pytest

from bioterms.etc.enums import ConceptRelationshipType, ConceptStatus
import bioterms.vocabulary.hpo as hpo


def _hpo_class(name, **overrides):
    values = {
        'name': name,
        'label': ['Seizure'],
        'IAO_0000115': ['A transient occurrence of signs or symptoms.'],
        'comment': ['Example comment'],
        'deprecated': [],
        'hasAlternativeId': [],
        'consider': [],
        'subclasses': lambda: [],
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


def test_process_hpo_class_preserves_replacement_and_consider_semantics():
    ontology_class = _hpo_class(
        'HP_0001250',
        deprecated=[True],
        hasAlternativeId=['HP:9999999'],
        consider=['HP:0001251', 'HP:0001252'],
        subclasses=lambda: [_hpo_class('HP_0012345')],
    )

    concept, relationships = hpo._process_hpo_class(ontology_class)

    assert concept.concept_id == '0001250'
    assert concept.status == ConceptStatus.DEPRECATED
    assert relationships == [
        ('0012345', '0001250', ConceptRelationshipType.IS_A),
        ('9999999', '0001250', ConceptRelationshipType.REPLACED_BY),
        ('0001250', '0001251', ConceptRelationshipType.CONSIDER),
        ('0001250', '0001252', ConceptRelationshipType.CONSIDER),
    ]


@pytest.mark.asyncio
async def test_hpo_offline_load_filters_non_hpo_classes_and_writes_graph(monkeypatch):
    hpo_class = _hpo_class('HP_0001250', subclasses=lambda: [_hpo_class('HP_0012345')])
    monkeypatch.setattr(hpo, 'check_files_exist', lambda _paths: True)
    monkeypatch.setattr(
        hpo, 'load_obo_owl_classes',
        lambda *_args: (object(), [hpo_class, _hpo_class('MONDO_0000001')]),
    )
    captured = {}

    async def capture_concepts(prefix, concepts, **_kwargs):
        captured['concepts'] = concepts

    async def capture_graph(prefix, concepts, vocabulary_graph):
        captured['graph'] = vocabulary_graph

    monkeypatch.setattr(hpo, 'write_concepts_to_file', capture_concepts)
    monkeypatch.setattr(hpo, 'write_graph_to_file', capture_graph)

    await hpo.load_vocabulary_from_file(offline=True, build_search_index=False)

    assert [concept.concept_id for concept in captured['concepts']] == ['0001250']
    assert captured['graph'].edges['0012345', '0001250']['label'] == (
        ConceptRelationshipType.IS_A
    )
