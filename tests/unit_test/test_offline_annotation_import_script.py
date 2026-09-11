import pytest
from pathlib import Path

from bioterms.annotation import _canonical_annotation_prefix, _infer_annotation_dump_prefixes
from bioterms.vocabulary.utils import parse_annotation_curie


def parse_annotation_row(row, source_fallback=None, target_fallback=None):
    """Compatibility helper mirroring legacy script row parsing semantics."""
    if len(row) < 6:
        raise ValueError(f'Row has {len(row)} columns; expected 6+')

    source_prefix, source_id, target_prefix, target_id, *_ = row
    source_curie = parse_annotation_curie(
        _canonical_annotation_prefix(source_prefix),
        source_id,
        _canonical_annotation_prefix(source_fallback),
    )
    target_curie = parse_annotation_curie(
        _canonical_annotation_prefix(target_prefix),
        target_id,
        _canonical_annotation_prefix(target_fallback),
    )

    source_prefix_value, source_concept_id = source_curie.split(':', 1)
    target_prefix_value, target_concept_id = target_curie.split(':', 1)
    return {
        'prefixFrom': source_prefix_value,
        'conceptIdFrom': source_concept_id,
        'prefixTo': target_prefix_value,
        'conceptIdTo': target_concept_id,
    }


@pytest.mark.parametrize(
    ('row', 'expected'),
    [
        (
            ['hgnc', '5', 'gene', 'A1BG', 'has_symbol', '{}'],
            ('hgnc', '5', 'gene', 'A1BG'),
        ),
        (
            ['hgnc', 'HGNC:5', 'gene', 'A1BG', 'has_symbol', '{}'],
            ('hgnc', '5', 'gene', 'A1BG'),
        ),
        (
            ['hgnc', 'hgnc:5', 'gene', 'gene:A1BG', 'has_symbol', '{}'],
            ('hgnc', '5', 'gene', 'A1BG'),
        ),
        (
            ['', 'hgnc:5', '', 'gene:A1BG', 'has_symbol', '{}'],
            ('hgnc', '5', 'gene', 'A1BG'),
        ),
        (
            ['hgnc', 'HGNC:5', 'gene', 'Em:AC068896.4', 'has_symbol', '{}'],
            ('hgnc', '5', 'gene', 'Em:AC068896.4'),
        ),
        (
            ['mondo', 'mondo:1', 'mesh', 'mesh:D0123', 'exact', '{}'],
            ('mondo', '1', 'mesh', 'D0123'),
        ),
        (
            ['ohdsi', '123', 'cgi', 'ABL1:F317L', 'exact', '{}'],
            ('ohdsi', '123', 'cgi', 'ABL1:F317L'),
        ),
    ],
)
def test_parse_annotation_row_compatibility(row, expected):
    parsed = parse_annotation_row(row)
    assert (
        parsed['prefixFrom'], parsed['conceptIdFrom'],
        parsed['prefixTo'], parsed['conceptIdTo'],
    ) == expected


def test_parse_annotation_row_uses_fallback_prefixes():
    parsed = parse_annotation_row(
        ['', '5', '', 'A1BG', 'has_symbol', '{}'],
        source_fallback='hgnc',
        target_fallback='gene',
    )
    assert parsed['prefixFrom'] == 'hgnc'
    assert parsed['conceptIdFrom'] == '5'
    assert parsed['prefixTo'] == 'gene'
    assert parsed['conceptIdTo'] == 'A1BG'


def test_infer_prefixes_from_filename():
    assert _infer_annotation_dump_prefixes(Path('gene-hpo.annotation.dump')) == ('gene', 'hpo')
    assert _infer_annotation_dump_prefixes(Path('mondo.annotation.dump')) == ('mondo', None)
