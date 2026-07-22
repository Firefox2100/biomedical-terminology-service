import runpy
from pathlib import Path


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
