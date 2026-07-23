import runpy
from pathlib import Path

import pytest


SCRIPT = runpy.run_path(
    str(Path(__file__).parents[2] / 'scripts' / 'load_offline_vocabulary.py'),
    run_name='offline_vocabulary_import_script',
)


def test_graph_edge_query_has_pipeline_boundary_before_procedure_call():
    query = ' '.join(SCRIPT['GRAPH_EDGE_UPSERT_QUERY'].split())
    assert (
        'MERGE (target:Concept {id: edge.target, prefix: $prefix}) '
        'WITH source, target, edge '
        'CALL apoc.merge.relationship'
    ) in query
    assert 'coll.distinct' not in query


def test_only_file_resolves_bare_filename_under_offline_directory(tmp_path):
    offline_dir = tmp_path / 'offline'
    offline_dir.mkdir()
    expected = offline_dir / 'hpo-relevance-ordo.similarity.dump'
    assert SCRIPT['resolve_only_file'](Path(expected.name), offline_dir) == expected


def test_only_file_similarity_filename_is_validated():
    method, corpus = SCRIPT['parse_similarity_filename'](
        Path('hpo-relevance-ordo.similarity.dump'),
        SCRIPT['ConceptPrefix'].HPO,
    )
    assert method.value == 'relevance'
    assert corpus.value == 'ordo'


def test_only_file_accepts_matching_embedding_dump():
    classify = SCRIPT['classify_only_file']
    prefix = SCRIPT['ConceptPrefix'].HPO
    assert classify(prefix, Path('hpo.embed.dump')) == 'embedding'
    assert classify(prefix, Path('hpo-relevance-ordo.similarity.dump')) == 'similarity'

    with pytest.raises(ValueError, match='hpo.embed.dump'):
        classify(prefix, Path('mondo.embed.dump'))
