import types

import pytest

from bioterms import similarity
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod


def _patch_method(monkeypatch, results, default_threshold=0.5):
    async def fake_calculate_similarity(context):
        assert context.threshold == pytest.approx(expected_threshold[0])
        for result in results:
            yield result

    expected_threshold = [default_threshold]
    monkeypatch.setattr(
        similarity, 'get_similarity_module',
        lambda _method: types.SimpleNamespace(calculate_similarity=fake_calculate_similarity),
    )
    monkeypatch.setattr(
        similarity, 'get_similarity_method_config',
        lambda _method: {'defaultThreshold': default_threshold, 'corpusRequired': False,
                         'corpusGraphRequired': False},
    )

    async def fake_context(*_args):
        return types.SimpleNamespace(threshold=None)

    monkeypatch.setattr(similarity, '_load_similarity_context', fake_context)
    return expected_threshold


class _RecordingGraphDb:
    def __init__(self, fail=False):
        self.saved = []
        self.fail = fail

    async def save_similarity_scores(self, prefix_from, prefix_to, similarity_scores,
                                     similarity_method, corpus_prefix):
        if self.fail:
            raise RuntimeError('write failed')
        self.saved.append((prefix_from, prefix_to, list(similarity_scores), similarity_method, corpus_prefix))


class _RecordingCache:
    def __init__(self):
        self.rotations = 0

    async def rotate_dataset_version(self):
        self.rotations += 1


@pytest.mark.asyncio
async def test_offline_writes_results_above_threshold_to_named_dump(monkeypatch, tmp_path):
    expected_threshold = _patch_method(monkeypatch, [('a', 'b', 0.9), ('a', 'c', 0.2), ('b', 'c', 0.3)])
    expected_threshold[0] = 0.25
    (tmp_path / 'offline').mkdir()
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    await similarity.calculate_similarity(
        method=SimilarityMethod.CO_ANNOTATION, target_prefix=ConceptPrefix.HPO,
        corpus_prefix=ConceptPrefix.MONDO, similarity_threshold=0.25, offline=True,
    )

    dump = tmp_path / 'offline' / f'hpo-{SimilarityMethod.CO_ANNOTATION.value}-mondo.similarity.dump'
    assert dump.read_text() == 'a,b,0.9\nb,c,0.3\n'


@pytest.mark.asyncio
async def test_online_saves_in_batches_and_rotates_cache(monkeypatch):
    results = [(f's{i}', f't{i}', 1.0) for i in range(10001)] + [('x', 'y', 0.1)]
    _patch_method(monkeypatch, results)

    async def no_prerequisites(*_args):
        return None

    monkeypatch.setattr(similarity, '_validate_similarity_prerequisites', no_prerequisites)
    graph_db, cache = _RecordingGraphDb(), _RecordingCache()

    await similarity.calculate_similarity(
        method=SimilarityMethod.RELEVANCE, target_prefix=ConceptPrefix.HPO,
        doc_db=object(), graph_db=graph_db, cache=cache,
    )

    assert [len(batch[2]) for batch in graph_db.saved] == [10000, 1]
    assert graph_db.saved[0][:2] == (ConceptPrefix.HPO, ConceptPrefix.HPO)
    assert graph_db.saved[0][3:] == (SimilarityMethod.RELEVANCE, None)
    assert cache.rotations == 1


@pytest.mark.asyncio
async def test_online_rotates_cache_even_when_writing_fails(monkeypatch):
    _patch_method(monkeypatch, [('a', 'b', 1.0)])

    async def no_prerequisites(*_args):
        return None

    monkeypatch.setattr(similarity, '_validate_similarity_prerequisites', no_prerequisites)
    graph_db, cache = _RecordingGraphDb(fail=True), _RecordingCache()

    with pytest.raises(RuntimeError, match='write failed'):
        await similarity.calculate_similarity(
            method=SimilarityMethod.RELEVANCE, target_prefix=ConceptPrefix.HPO,
            doc_db=object(), graph_db=graph_db, cache=cache,
        )

    assert cache.rotations == 1


@pytest.mark.asyncio
async def test_annotation_file_override_requires_offline(monkeypatch, tmp_path):
    _patch_method(monkeypatch, [])

    with pytest.raises(ValueError, match='only be used in offline mode'):
        await similarity.calculate_similarity(
            method=SimilarityMethod.RELEVANCE, target_prefix=ConceptPrefix.HPO,
            annotation_file_path=tmp_path / 'x.annotation.dump',
        )
