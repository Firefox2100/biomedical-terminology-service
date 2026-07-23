import csv

import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.model.annotation import Annotation
from bioterms.vocabulary.utils import (
    load_annotation_from_file,
    normalise_annotation_curie,
    parse_annotation_curie,
    write_annotations_to_file,
)


def test_annotation_curie_accepts_local_and_prefixed_ids():
    assert normalise_annotation_curie(ConceptPrefix.HGNC, '5') == 'hgnc:5'
    assert normalise_annotation_curie(ConceptPrefix.HGNC, 'HGNC:5') == 'hgnc:5'
    assert parse_annotation_curie('', 'HGNC:5', ConceptPrefix.HGNC) == 'hgnc:5'
    assert normalise_annotation_curie(ConceptPrefix.HGNC_SYMBOL, 'Em:AC068896.4') \
        == 'gene:Em:AC068896.4'
    assert parse_annotation_curie('gene', 'Em:AC068896.4') == 'gene:Em:AC068896.4'


def test_external_prefix_only_strips_itself_and_does_not_check_other_colons():
    assert normalise_annotation_curie('cgi', 'ABL1:F317L') == 'cgi:ABL1:F317L'
    assert normalise_annotation_curie('cgi', 'cgi:123') == 'cgi:123'
    assert normalise_annotation_curie('cgi', 'hpo:123') == 'cgi:hpo:123'
    assert parse_annotation_curie('cgi', 'ABL1:F317L') == 'cgi:ABL1:F317L'
    assert parse_annotation_curie('cgi', 'cgi:123') == 'cgi:123'
    assert parse_annotation_curie('cgi', 'hpo:123') == 'cgi:hpo:123'


def test_annotation_curie_rejects_conflicting_prefixes():
    with pytest.raises(ValueError, match='conflicts'):
        parse_annotation_curie('hgnc', 'ensembl:5')


@pytest.mark.asyncio
async def test_writer_uses_curies_for_both_concepts(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    await write_annotations_to_file(
        prefix_from=ConceptPrefix.HGNC,
        prefix_to=ConceptPrefix.HGNC_SYMBOL,
        annotations=[Annotation(
            prefixFrom=ConceptPrefix.HGNC,
            conceptIdFrom='HGNC:5',
            prefixTo=ConceptPrefix.HGNC_SYMBOL,
            conceptIdTo='A1BG',
            annotationType=AnnotationType.HAS_SYMBOL,
        )],
    )

    with (tmp_path / 'offline' / 'hgnc-gene.annotation.dump').open(newline='') as file:
        row = next(csv.reader(file))
    assert row[:4] == ['hgnc', 'hgnc:5', 'gene', 'gene:A1BG']


@pytest.mark.asyncio
async def test_reader_accepts_zero_one_or_two_curie_ids(monkeypatch, tmp_path):
    offline = tmp_path / 'offline'
    offline.mkdir()
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    path = offline / 'hgnc-gene.annotation.dump'
    rows = [
        ('hgnc', '5', 'gene', 'A1BG', 'has_symbol', '{}'),
        ('hgnc', 'HGNC:6', 'gene', 'A2M', 'has_symbol', '{}'),
        ('hgnc', 'hgnc:7', 'gene', 'gene:A2ML1', 'has_symbol', '{}'),
    ]
    with path.open('w', newline='') as file:
        csv.writer(file).writerows(rows)

    graph = await load_annotation_from_file(ConceptPrefix.HGNC, ConceptPrefix.HGNC_SYMBOL)
    assert set(graph.edges) == {
        ('hgnc:5', 'gene:A1BG'),
        ('hgnc:6', 'gene:A2M'),
        ('hgnc:7', 'gene:A2ML1'),
    }


@pytest.mark.asyncio
async def test_reader_override_normalizes_and_filters_mixed_annotation_file(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    path = tmp_path / 'mondo.annotation.dump'
    rows = [
        ('MONDO', 'MONDO:1', 'hpo', 'hpo:2', 'exact', '{}'),
        ('mondo', '3', 'mesh', 'D0123', 'exact', '{}'),
        ('hpo', '4', 'mondo', '5', 'related', '{}'),
        ('ordo', '6', 'hpo', '7', 'exact', '{}'),
    ]
    with path.open('w', newline='') as file:
        csv.writer(file).writerows(rows)

    graph = await load_annotation_from_file(
        ConceptPrefix.HPO,
        ConceptPrefix.MONDO,
        annotation_file_path=path,
    )
    assert set(graph.edges) == {
        ('mondo:1', 'hpo:2'),
        ('hpo:4', 'mondo:5'),
    }
