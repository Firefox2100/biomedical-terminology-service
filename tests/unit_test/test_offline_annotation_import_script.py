import runpy
from pathlib import Path

import pytest


SCRIPT = runpy.run_path(
    str(Path(__file__).parents[2] / 'scripts' / 'load_offline_annotations.py'),
    run_name='offline_annotation_import_script',
)


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
    parsed = SCRIPT['parse_annotation_row'](row)
    assert (
        parsed['prefixFrom'], parsed['conceptIdFrom'],
        parsed['prefixTo'], parsed['conceptIdTo'],
    ) == expected


def test_parse_annotation_row_uses_fallback_prefixes():
    parsed = SCRIPT['parse_annotation_row'](
        ['', '5', '', 'A1BG', 'has_symbol', '{}'],
        source_fallback='hgnc',
        target_fallback='gene',
    )
    assert parsed['prefixFrom'] == 'hgnc'
    assert parsed['conceptIdFrom'] == '5'
    assert parsed['prefixTo'] == 'gene'
    assert parsed['conceptIdTo'] == 'A1BG'


def test_infer_prefixes_from_filename():
    infer = SCRIPT['infer_prefixes']
    assert infer(Path('gene-hpo.annotation.dump')) == ('gene', 'hpo')
    assert infer(Path('mondo.annotation.dump')) == ('mondo', None)
