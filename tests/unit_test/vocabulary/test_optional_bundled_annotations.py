import inspect
import types

import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import OhdsiConcept
import bioterms.vocabulary.ensembl as ensembl
import bioterms.vocabulary.hgnc as hgnc
import bioterms.vocabulary.mondo as mondo
import bioterms.vocabulary.ohdsi as ohdsi
import bioterms.vocabulary.uniprot as uniprot


def test_only_optional_annotation_vocabularies_expose_load_annotations():
    assert 'load_annotations' not in inspect.signature(mondo.load_vocabulary_from_file).parameters
    assert 'load_annotations' in inspect.signature(ohdsi.load_vocabulary_from_file).parameters
    assert 'load_annotations' in inspect.signature(uniprot.load_vocabulary_from_file).parameters

    # Gene-vocabulary -> Gene Symbol links are required vocabulary structure.
    assert 'load_annotations' not in inspect.signature(ensembl.load_vocabulary_from_file).parameters
    assert 'load_annotations' not in inspect.signature(hgnc.load_vocabulary_from_file).parameters


@pytest.mark.asyncio
async def test_mondo_vocabulary_load_never_processes_xrefs(monkeypatch, tmp_path):
    mondo_dir = tmp_path / 'mondo'
    mondo_dir.mkdir()
    (mondo_dir / 'mondo.owl').write_text('fixture')
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    ontology = types.SimpleNamespace(classes=lambda: [], world=object())
    monkeypatch.setattr(mondo, 'load_obo_owl_classes', lambda *_args: (ontology, []))

    def fail_xref_lookup(_world):
        raise AssertionError('Mondo xrefs must not be processed')

    async def ignore_write(**_kwargs):
        pass

    monkeypatch.setattr(mondo, '_build_xref_source_lookup', fail_xref_lookup)
    monkeypatch.setattr(mondo, 'write_concepts_to_file', ignore_write)
    monkeypatch.setattr(mondo, 'write_graph_to_file', ignore_write)
    await mondo.load_vocabulary_from_file(
        offline=True,
        build_search_index=False,
    )


@pytest.mark.asyncio
async def test_ohdsi_no_annotation_skips_annotation_processing(monkeypatch):
    concept = OhdsiConcept(
        prefix=ConceptPrefix.OHDSI,
        conceptId='1',
        label='Test',
        status=ConceptStatus.ACTIVE,
    )
    monkeypatch.setattr(ohdsi, 'check_files_exist', lambda _paths: True)
    monkeypatch.setattr(ohdsi, '_process_concepts', lambda: {1: concept})
    monkeypatch.setattr(ohdsi, '_process_synonyms', lambda _concepts: None)
    monkeypatch.setattr(ohdsi, '_process_drug_strength', lambda _concepts: None)
    monkeypatch.setattr(ohdsi, '_iter_relationship_edges', lambda: iter(()))

    def fail_annotations():
        raise AssertionError('OHDSI annotations must not be processed')

    async def ignore_write(**_kwargs):
        pass

    monkeypatch.setattr(ohdsi, '_process_annotations', fail_annotations)
    monkeypatch.setattr(ohdsi, 'write_concepts_to_file', ignore_write)
    monkeypatch.setattr(ohdsi, 'write_graph_to_file', ignore_write)
    monkeypatch.setattr(ohdsi, 'write_annotations_to_file', ignore_write)

    await ohdsi.load_vocabulary_from_file(
        offline=True,
        build_search_index=False,
        load_annotations=False,
    )
