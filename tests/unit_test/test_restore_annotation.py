import csv
from pathlib import Path

import pytest

from bioterms.annotation import _infer_annotation_dump_prefixes, restore_annotation
from bioterms.etc.enums import AnnotationType


class RecordingGraphDatabase:
    def __init__(self):
        self.batches = []

    async def save_annotations(self, annotations):
        self.batches.append(annotations)


class RecordingCache:
    def __init__(self):
        self.rotations = 0

    async def rotate_dataset_version(self):
        self.rotations += 1


def write_dump(path, rows):
    with path.open('w', encoding='utf-8', newline='') as file:
        csv.writer(file).writerows(rows)


@pytest.mark.asyncio
async def test_restore_annotation_parses_curie_variants_and_batches(tmp_path):
    dump_path = tmp_path / 'hgnc-gene.annotation.dump'
    write_dump(dump_path, [
        ['hgnc', 'HGNC:5', 'gene', 'gene:A1BG', 'has_symbol', '{}'],
        ['hgnc', '6', 'gene', 'Em:AC068896.4', '', ''],
    ])
    graph_db = RecordingGraphDatabase()
    cache = RecordingCache()

    count = await restore_annotation(
        dump_path,
        batch_size=1,
        graph_db=graph_db,
        cache=cache,
    )

    assert count == 2
    assert len(graph_db.batches) == 2
    first = graph_db.batches[0][0]
    assert (first.prefix_from, first.concept_id_from) == ('hgnc', '5')
    assert (first.prefix_to, first.concept_id_to) == ('gene', 'A1BG')
    assert first.annotation_type == AnnotationType.HAS_SYMBOL
    second = graph_db.batches[1][0]
    assert second.concept_id_to == 'Em:AC068896.4'
    assert second.annotation_type == AnnotationType.ANNOTATED_WITH
    assert cache.rotations == 1


@pytest.mark.asyncio
async def test_restore_annotation_uses_explicit_fallback_prefixes(tmp_path):
    dump_path = tmp_path / 'annotations.annotation.dump'
    write_dump(dump_path, [
        ['', '5', '', 'A1BG', 'has_symbol', '{}'],
    ])
    graph_db = RecordingGraphDatabase()

    await restore_annotation(
        dump_path,
        source_prefix='hgnc',
        target_prefix='gene',
        graph_db=graph_db,
        cache=RecordingCache(),
    )

    annotation = graph_db.batches[0][0]
    assert (annotation.prefix_from, annotation.concept_id_from) == ('hgnc', '5')
    assert (annotation.prefix_to, annotation.concept_id_to) == ('gene', 'A1BG')


@pytest.mark.asyncio
async def test_restore_annotation_rejects_malformed_rows(tmp_path):
    dump_path = tmp_path / 'hgnc-gene.annotation.dump'
    write_dump(dump_path, [['hgnc', '5']])

    with pytest.raises(ValueError, match='expected at least 6'):
        await restore_annotation(
            dump_path,
            graph_db=RecordingGraphDatabase(),
            cache=RecordingCache(),
        )


def test_infer_prefixes_from_filename():
    assert _infer_annotation_dump_prefixes(Path('gene-hpo.annotation.dump')) == ('gene', 'hpo')
    assert _infer_annotation_dump_prefixes(Path('mondo.annotation.dump')) == ('mondo', None)
