import types

from bioterms.etc.enums import AnnotationType
from bioterms.vocabulary.mondo import _build_mondo_xref_annotations, _build_xref_source_lookup


NT_FIXTURE = """\
<http://purl.obolibrary.org/obo/MONDO_0000001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .
<http://purl.obolibrary.org/obo/MONDO_0000001> <http://www.geneontology.org/formats/oboInOwl#hasDbXref> "DOID:4" .
<http://example.org/ax1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Axiom> .
<http://example.org/ax1> <http://www.w3.org/2002/07/owl#annotatedSource> <http://purl.obolibrary.org/obo/MONDO_0000001> .
<http://example.org/ax1> <http://www.w3.org/2002/07/owl#annotatedProperty> <http://www.geneontology.org/formats/oboInOwl#hasDbXref> .
<http://example.org/ax1> <http://www.w3.org/2002/07/owl#annotatedTarget> "DOID:4" .
<http://example.org/ax1> <http://www.geneontology.org/formats/oboInOwl#source> "MONDO:equivalentTo" .
"""


def test_build_xref_source_lookup_reads_reified_axiom_annotations(tmp_path):
    from owlready2 import World

    fixture_path = tmp_path / 'fixture.nt'
    fixture_path.write_text(NT_FIXTURE)

    # An isolated World, not owlready2's process-wide default_world: this module's other
    # ontology-loading tests must not see each other's triples.
    world = World()
    onto = world.get_ontology(f'file://{fixture_path}').load()

    lookup = _build_xref_source_lookup(onto.world)

    assert lookup == {('0000001', 'DOID:4'): 'MONDO:equivalentTo'}


def test_build_xref_source_lookup_omits_untagged_pairs(tmp_path):
    from owlready2 import World

    # No owl:Axiom reification at all -- the ontology has classes/xrefs but nothing
    # in the lookup, which must not raise and must simply return an empty dict.
    fixture_path = tmp_path / 'untagged.nt'
    fixture_path.write_text(
        '<http://purl.obolibrary.org/obo/MONDO_0000002> '
        '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> '
        '<http://www.w3.org/2002/07/owl#Class> .\n'
    )
    world = World()
    onto = world.get_ontology(f'file://{fixture_path}').load()

    lookup = _build_xref_source_lookup(onto.world)

    assert lookup == {}


def _fake_mondo_class(exact_match=None, has_db_xref=None):
    return types.SimpleNamespace(
        exactMatch=exact_match or [],
        broadMatch=[],
        narrowMatch=[],
        relatedMatch=[],
        hasDbXref=has_db_xref or [],
    )


def test_xref_annotation_gets_mapping_source_from_lookup():
    mondo_class = _fake_mondo_class(
        exact_match=['http://purl.obolibrary.org/obo/DOID_4'],
    )
    lookup = {('0000001', 'DOID:4'): 'MONDO:equivalentTo'}

    annotations = _build_mondo_xref_annotations(mondo_class, '0000001', lookup)

    assert len(annotations) == 1
    assert annotations[0].annotation_type == AnnotationType.EXACT
    assert annotations[0].properties == {'mappingSource': 'MONDO:equivalentTo'}


def test_xref_annotation_has_no_properties_when_untagged():
    mondo_class = _fake_mondo_class(
        exact_match=['http://purl.obolibrary.org/obo/DOID_4'],
    )

    annotations = _build_mondo_xref_annotations(mondo_class, '0000001', {})

    assert len(annotations) == 1
    assert annotations[0].properties is None


def test_hasdbxref_fallback_annotation_also_gets_mapping_source():
    mondo_class = _fake_mondo_class(
        has_db_xref=['ICD9:799.9'],
    )
    lookup = {('0000001', 'ICD9:799.9'): 'MONDO:exact-label-match'}

    annotations = _build_mondo_xref_annotations(mondo_class, '0000001', lookup)

    assert len(annotations) == 1
    assert annotations[0].annotation_type == AnnotationType.ANNOTATED_WITH
    assert annotations[0].properties == {'mappingSource': 'MONDO:exact-label-match'}


def test_build_mondo_xref_annotations_defaults_lookup_to_empty():
    # No lookup argument passed at all -- must not raise, and no annotation is tagged.
    mondo_class = _fake_mondo_class(has_db_xref=['ICD9:799.9'])

    annotations = _build_mondo_xref_annotations(mondo_class, '0000001')

    assert len(annotations) == 1
    assert annotations[0].properties is None
