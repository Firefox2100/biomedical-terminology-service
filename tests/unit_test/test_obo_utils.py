import types

from bioterms.etc.consts import CONFIG
from bioterms.etc.utils import load_obo_owl_classes, obo_class_metadata, obo_entity_local_id


def test_obo_entity_local_id_accepts_names_curies_and_iris():
    assert obo_entity_local_id(types.SimpleNamespace(name='UBERON_0000948'), 'UBERON') == '0000948'
    assert obo_entity_local_id('UBERON:0000948', 'UBERON') == '0000948'
    assert obo_entity_local_id(
        'http://purl.obolibrary.org/obo/UBERON_0000948', 'UBERON'
    ) == '0000948'
    assert obo_entity_local_id('GO:0008150', 'UBERON') is None


def test_obo_class_metadata_normalizes_common_obo_fields():
    ontology_class = types.SimpleNamespace(
        label=['heart'],
        IAO_0000115=['A muscular organ.'],
        comment=['Test comment.'],
        deprecated=[True],
        hasExactSynonym=['cardiac organ', 'heart'],
        hasBroadSynonym=['organ'],
        hasNarrowSynonym=[],
        hasRelatedSynonym=['cardium', 'cardiac organ'],
    )

    assert obo_class_metadata(ontology_class) == {
        'label': 'heart',
        'definition': 'A muscular organ.',
        'comment': 'Test comment.',
        'deprecated': True,
        'synonyms': ['cardiac organ', 'organ', 'cardium'],
    }


def test_load_obo_owl_classes_excludes_imported_namespaces(monkeypatch, tmp_path):
    ontology_dir = tmp_path / 'uberon'
    ontology_dir.mkdir()
    (ontology_dir / 'fixture.owl').write_text('''\
<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:owl="http://www.w3.org/2002/07/owl#">
  <owl:Ontology rdf:about="http://example.org/test"/>
  <owl:Class rdf:about="http://purl.obolibrary.org/obo/UBERON_0000948"/>
  <owl:Class rdf:about="http://purl.obolibrary.org/obo/GO_0008150"/>
</rdf:RDF>
''')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    _, classes = load_obo_owl_classes('uberon/fixture.owl', 'UBERON_')

    assert [ontology_class.name for ontology_class in classes] == ['UBERON_0000948']
