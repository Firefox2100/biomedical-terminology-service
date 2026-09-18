import types

import pytest

from bioterms.annotation import hgnc_mondo, hpo_mondo, mondo_ncit, mondo_omim, mondo_ordo, \
    mondo_snomed
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.vocabulary import mondo
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
    assert annotations[0].properties == {
        'mappingSource': 'MONDO:equivalentTo', 'source': 'Mondo',
    }


def test_xref_annotation_has_only_dataset_provenance_when_untagged():
    mondo_class = _fake_mondo_class(
        exact_match=['http://purl.obolibrary.org/obo/DOID_4'],
    )

    annotations = _build_mondo_xref_annotations(mondo_class, '0000001', {})

    assert len(annotations) == 1
    assert annotations[0].properties == {'source': 'Mondo'}


def test_hasdbxref_fallback_annotation_also_gets_mapping_source():
    mondo_class = _fake_mondo_class(
        has_db_xref=['ICD9:799.9'],
    )
    lookup = {('0000001', 'ICD9:799.9'): 'MONDO:exact-label-match'}

    annotations = _build_mondo_xref_annotations(mondo_class, '0000001', lookup)

    assert len(annotations) == 1
    assert annotations[0].annotation_type == AnnotationType.ANNOTATED_WITH
    assert annotations[0].properties == {
        'mappingSource': 'MONDO:exact-label-match', 'source': 'Mondo',
    }


def test_build_mondo_xref_annotations_defaults_lookup_to_empty():
    # No lookup argument passed at all -- only dataset-level provenance is attached.
    mondo_class = _fake_mondo_class(has_db_xref=['ICD9:799.9'])

    annotations = _build_mondo_xref_annotations(mondo_class, '0000001')

    assert len(annotations) == 1
    assert annotations[0].properties == {'source': 'Mondo'}


@pytest.mark.asyncio
async def test_independent_mondo_annotation_loader_filters_target_namespace(monkeypatch):
    mondo_class = _fake_mondo_class(
        exact_match=[
            'http://purl.obolibrary.org/obo/NCIT_C1',
            'https://omim.org/entry/123456',
        ],
    )
    mondo_class.name = 'MONDO_0000001'
    ontology = types.SimpleNamespace(world=object())
    graph_db = types.SimpleNamespace(annotations=[])

    async def save_annotations(annotations):
        graph_db.annotations.extend(annotations)

    graph_db.save_annotations = save_annotations
    monkeypatch.setattr(mondo, 'check_files_exist', lambda _paths: True)
    monkeypatch.setattr(
        mondo,
        'load_obo_owl_classes',
        lambda *_args: (ontology, [mondo_class]),
    )
    monkeypatch.setattr(mondo, '_build_xref_source_lookup', lambda _world: {})

    count = await mondo.load_mondo_annotations_from_file(
        ConceptPrefix.NCIT,
        graph_db=graph_db,
    )

    assert count == 1
    assert len(graph_db.annotations) == 1
    assert graph_db.annotations[0].prefix_to == ConceptPrefix.NCIT
    assert graph_db.annotations[0].concept_id_to == 'C1'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('annotation_module', 'target_prefix'),
    [
        (hgnc_mondo, ConceptPrefix.HGNC),
        (hpo_mondo, ConceptPrefix.HPO),
        (mondo_ncit, ConceptPrefix.NCIT),
        (mondo_omim, ConceptPrefix.OMIM),
        (mondo_ordo, ConceptPrefix.ORDO),
        (mondo_snomed, ConceptPrefix.SNOMED),
    ],
)
async def test_mondo_annotation_modules_delegate_to_independent_loader(
    monkeypatch,
    tmp_path,
    annotation_module,
    target_prefix,
):
    mondo_dir = tmp_path / 'mondo'
    mondo_dir.mkdir()
    (mondo_dir / 'mondo.owl').write_text('fixture')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    calls = []

    class FakeGraphDb:
        async def count_terms(self, _prefix):
            return 1

        async def count_annotations(self, prefix_1, prefix_2):
            return 0

    async def fake_load(prefix, graph_db=None):
        calls.append((prefix, graph_db))

    graph_db = FakeGraphDb()
    monkeypatch.setattr(mondo, 'load_mondo_annotations_from_file', fake_load)

    await annotation_module.load_annotation_from_file(graph_db=graph_db)

    assert calls == [(target_prefix, graph_db)]
