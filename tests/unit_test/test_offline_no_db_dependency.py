import types

import pytest
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
import bioterms.vocabulary as vocabulary
import bioterms.similarity as similarity


@pytest.mark.asyncio
async def test_load_vocabulary_offline_does_not_require_cache(monkeypatch):
    async def fake_load_vocabulary_from_file(doc_db=None, graph_db=None, offline=False):
        assert doc_db is None
        assert graph_db is None
        assert offline is True

    fake_module = types.SimpleNamespace(
        FILE_PATHS=['/tmp/fake-vocabulary-file'],
        load_vocabulary_from_file=fake_load_vocabulary_from_file,
    )

    monkeypatch.setattr(vocabulary, 'get_vocabulary_module', lambda _: fake_module)
    monkeypatch.setattr(vocabulary, 'check_files_exist', lambda _: True)

    def fail_get_active_cache():
        raise AssertionError('get_active_cache should not be called in offline mode')

    monkeypatch.setattr(vocabulary, 'get_active_cache', fail_get_active_cache)

    await vocabulary.load_vocabulary(
        prefix=ConceptPrefix.MONDO,
        offline=True,
    )


@pytest.mark.asyncio
async def test_calculate_similarity_offline_does_not_require_cache(monkeypatch, tmp_path):
    async def fake_calculate_similarity(**_kwargs):
        yield 'a', 'b', 1.0

    monkeypatch.setattr(
        similarity,
        'get_similarity_module',
        lambda _method: types.SimpleNamespace(calculate_similarity=fake_calculate_similarity),
    )
    monkeypatch.setattr(
        similarity,
        'get_similarity_method_config',
        lambda _method: {
            'defaultThreshold': 0.5,
            'corpusRequired': False,
            'corpusGraphRequired': False,
        },
    )

    async def fake_load_graph_from_file(_prefix):
        return nx.MultiDiGraph()

    monkeypatch.setattr(similarity, 'load_graph_from_file', fake_load_graph_from_file)

    def fail_get_active_cache():
        raise AssertionError('get_active_cache should not be called in offline mode')

    monkeypatch.setattr(similarity, 'get_active_cache', fail_get_active_cache)

    data_dir = tmp_path / 'data'
    (data_dir / 'offline').mkdir(parents=True)
    monkeypatch.setattr(CONFIG, 'data_dir', str(data_dir))

    await similarity.calculate_similarity(
        method=SimilarityMethod.RELEVANCE,
        target_prefix=ConceptPrefix.MONDO,
        offline=True,
    )


@pytest.mark.asyncio
async def test_calculate_similarity_forwards_annotation_file_override(monkeypatch, tmp_path):
    async def fake_calculate_similarity(**_kwargs):
        if False:
            yield

    monkeypatch.setattr(
        similarity,
        'get_similarity_module',
        lambda _method: types.SimpleNamespace(calculate_similarity=fake_calculate_similarity),
    )
    monkeypatch.setattr(
        similarity,
        'get_similarity_method_config',
        lambda _method: {
            'defaultThreshold': 0.5,
            'corpusRequired': True,
            'corpusGraphRequired': False,
        },
    )

    async def fake_load_graph_from_file(_prefix):
        return nx.MultiDiGraph()

    annotation_path = tmp_path / 'mondo.annotation.dump'
    annotation_path.write_text('')

    async def fake_load_annotation_from_file(prefix_from, prefix_to, annotation_file_path=None):
        assert prefix_from == ConceptPrefix.HPO
        assert prefix_to == ConceptPrefix.MONDO
        assert annotation_file_path == annotation_path
        return nx.DiGraph()

    monkeypatch.setattr(similarity, 'load_graph_from_file', fake_load_graph_from_file)
    monkeypatch.setattr(similarity, 'load_annotation_from_file', fake_load_annotation_from_file)
    data_dir = tmp_path / 'data'
    (data_dir / 'offline').mkdir(parents=True)
    monkeypatch.setattr(CONFIG, 'data_dir', str(data_dir))

    await similarity.calculate_similarity(
        method=SimilarityMethod.RELEVANCE,
        target_prefix=ConceptPrefix.HPO,
        corpus_prefix=ConceptPrefix.MONDO,
        offline=True,
        annotation_file_path=annotation_path,
    )
