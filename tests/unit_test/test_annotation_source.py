import pytest

from bioterms.annotation.utils import AnnotationSource
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.vocabulary import ohdsi


def test_annotation_source_sets_provenance_and_publisher_direction_without_mutating_properties():
    source = AnnotationSource('HOOM', ConceptPrefix.ORDO, ConceptPrefix.HPO)
    properties = {'frequency': '0040281'}

    annotation = source.create('100', '0001', AnnotationType.EXACT, properties)

    assert annotation.prefix_from == ConceptPrefix.ORDO
    assert annotation.concept_id_from == '100'
    assert annotation.prefix_to == ConceptPrefix.HPO
    assert annotation.concept_id_to == '0001'
    assert annotation.properties == {'frequency': '0040281', 'source': 'HOOM'}
    assert properties == {'frequency': '0040281'}


def test_annotation_source_rejects_overwriting_provenance():
    source = AnnotationSource('HOOM', ConceptPrefix.ORDO, ConceptPrefix.HPO)

    with pytest.raises(ValueError, match='managed by AnnotationSource'):
        source.create('100', '0001', properties={'source': 'phenotype.hpoa'})


def test_opposite_publisher_directions_remain_distinct_annotations():
    hoom = AnnotationSource('HOOM', ConceptPrefix.ORDO, ConceptPrefix.HPO)
    hpoa = AnnotationSource('phenotype.hpoa', ConceptPrefix.HPO, ConceptPrefix.ORDO)

    annotations = [hoom.create('100', '0001'), hpoa.create('0001', '100')]

    assert len(annotations) == 2
    assert annotations[0].prefix_from == ConceptPrefix.ORDO
    assert annotations[1].prefix_from == ConceptPrefix.HPO


def test_bundled_ohdsi_annotations_use_source_factory_but_gene_links_remain_excluded(
    monkeypatch, tmp_path,
):
    ohdsi_dir = tmp_path / 'ohdsi'
    ohdsi_dir.mkdir()
    (ohdsi_dir / 'CONCEPT.csv').write_text(
        'concept_id\tconcept_name\tvocabulary_id\tconcept_code\n'
        '1\tDisease\tSNOMED\t12345\n'
        '2\tGene\tHGNC\tHGNC:2\n'
    )
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    annotations = ohdsi._process_annotations()

    assert annotations[0].properties == {'source': 'OHDSI Athena'}
    assert annotations[0].prefix_from == ConceptPrefix.OHDSI
    assert annotations[0].prefix_to == ConceptPrefix.SNOMED
    assert annotations[1].properties is None
    assert annotations[1].prefix_to == ConceptPrefix.HGNC
