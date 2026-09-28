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


# --- prerequisites, context loading and restore -----------------------------------------------

from types import SimpleNamespace

from bioterms.etc.enums import AnnotationType


def _loaded(value):
    async def status(*_args, **_kwargs):
        return SimpleNamespace(loaded=value(_args, _kwargs) if callable(value) else value)
    return status


@pytest.mark.asyncio
@pytest.mark.parametrize(('vocab_loaded', 'annotation_loaded', 'corpus', 'message'), [
    (lambda a, k: False, True, ConceptPrefix.OMIM, 'Target vocabulary hpo is not loaded'),
    (True, True, None, 'requires a corpus prefix'),
    (lambda a, k: a[0] == ConceptPrefix.HPO, True, ConceptPrefix.OMIM, 'Corpus vocabulary omim is not loaded'),
    (True, False, ConceptPrefix.OMIM, 'Annotation between hpo and omim is not loaded'),
])
async def test_online_prerequisites_are_validated(monkeypatch, vocab_loaded, annotation_loaded, corpus, message):
    monkeypatch.setattr(similarity, 'get_vocabulary_status', _loaded(vocab_loaded))
    monkeypatch.setattr(similarity, 'get_annotation_status', _loaded(annotation_loaded))

    with pytest.raises(ValueError, match=message):
        await similarity._validate_similarity_prerequisites(
            SimilarityMethod.RELEVANCE, {'corpusRequired': True}, ConceptPrefix.HPO, corpus, None, None,
        )


class _GraphData:
    async def get_vocabulary_data(self, prefix):
        if prefix == ConceptPrefix.HPO:
            return ['HP:1', 'HP:2', 'HP:root'], [('HP:1', 'HP:root', 'is_a', None), ('HP:2', 'HP:root', 'is_a', None)]
        return ['OMIM:1', 'OMIM:2'], []

    async def get_annotation_edges(self, target, corpus):
        # Edges may be stored in either direction and with enum or plain-string prefixes.
        yield ConceptPrefix.HPO, 'HP:1', ConceptPrefix.OMIM, 'OMIM:1', AnnotationType.ANNOTATED_WITH
        yield 'omim', 'OMIM:2', 'hpo', 'HP:2', AnnotationType.ANNOTATED_WITH
        yield ConceptPrefix.HPO, 'HP:1', ConceptPrefix.MONDO, 'MONDO:9', AnnotationType.ANNOTATED_WITH


@pytest.mark.asyncio
@pytest.mark.parametrize('corpus_graph', [True, False])
async def test_online_context_normalises_annotation_direction(corpus_graph):
    context = await similarity._load_similarity_context(
        ConceptPrefix.HPO, ConceptPrefix.OMIM,
        {'corpusRequired': True, 'corpusGraphRequired': corpus_graph}, False, None, _GraphData(),
    )

    target_ids, corpus_ids = context.target.node_ids, context.corpus.node_ids
    pairs = {
        (target_ids[t], corpus_ids[int(c)])
        for t, row in enumerate(context.annotations.target_to_corpus) for c in row
    }
    assert pairs == {('HP:1', 'OMIM:1'), ('HP:2', 'OMIM:2')}
    assert set(corpus_ids) == {'OMIM:1', 'OMIM:2'}


@pytest.mark.asyncio
async def test_context_without_corpus_is_target_only():
    context = await similarity._load_similarity_context(
        ConceptPrefix.HPO, None, {'corpusRequired': False, 'corpusGraphRequired': False}, False, None, _GraphData(),
    )

    assert context.corpus is None and context.annotations is None
    assert set(context.target.node_ids) == {'HP:1', 'HP:2', 'HP:root'}


@pytest.mark.asyncio
async def test_offline_context_reads_dumps(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    offline = tmp_path / 'offline'
    offline.mkdir()
    (offline / 'hpo.graph.dump').write_text('HP:1,HP:root,is_a,\nHP:2,HP:root,is_a,\n')
    (offline / 'hpo.node_ids.dump').write_text('HP:1,[]\nHP:2,[]\nHP:root,[]\n')
    (offline / 'omim.graph.dump').write_text('')
    (offline / 'omim.node_ids.dump').write_text('1,[]\n')
    (offline / 'hpo-omim.annotation.dump').write_text('hpo,HP:1,omim,1,annotated_with,\n')

    for corpus_graph in (True, False):
        context = await similarity._load_similarity_context(
            ConceptPrefix.HPO, ConceptPrefix.OMIM,
            {'corpusRequired': True, 'corpusGraphRequired': corpus_graph}, True, None, None,
        )
        assert list(context.corpus.node_ids) == ['1']
        hp1 = context.target.node_to_index['HP:1']
        assert list(context.annotations.target_to_corpus[hp1]) == [0]

    target_only = await similarity._load_similarity_context(
        ConceptPrefix.HPO, None, {'corpusRequired': False, 'corpusGraphRequired': False}, True, None, None,
    )
    assert target_only.corpus is None


class _ScoreRecorder:
    def __init__(self):
        self.saved = []

    async def save_similarity_scores(self, **kwargs):
        self.saved.append(kwargs)


@pytest.mark.asyncio
async def test_restore_similarity_batches_every_dump(tmp_path):
    (tmp_path / 'hpo-relevance.similarity.dump').write_text('HP:1,HP:2,0.9\n\nHP:1,HP:3,0.5\nHP:2,HP:3,0.4\n')
    (tmp_path / 'hpo-co-annotation-omim.similarity.dump').write_text('HP:1,HP:2,0.7\n')
    graph_db, cache = _ScoreRecorder(), _RecordingCache()

    total = await similarity.restore_similarity(
        ConceptPrefix.HPO, batch_size=2, offline_dir=tmp_path, graph_db=graph_db, cache=cache,
    )

    assert total == 4
    assert [(len(s['similarity_scores']), s['similarity_method'], s['corpus_prefix']) for s in graph_db.saved] == [
        (1, SimilarityMethod.CO_ANNOTATION, ConceptPrefix.OMIM),
        (2, SimilarityMethod.RELEVANCE, None),
        (1, SimilarityMethod.RELEVANCE, None),
    ]
    assert cache.rotations == 1


@pytest.mark.asyncio
async def test_restore_similarity_rejects_missing_or_malformed_dumps(tmp_path):
    with pytest.raises(ValueError, match='No similarity dump files found'):
        await similarity.restore_similarity(ConceptPrefix.HPO, offline_dir=tmp_path, graph_db=_ScoreRecorder())

    (tmp_path / 'hpo-relevance.similarity.dump').write_text('HP:1,HP:2\n')
    with pytest.raises(ValueError, match='Malformed similarity row'):
        await similarity.restore_similarity(ConceptPrefix.HPO, offline_dir=tmp_path, graph_db=_ScoreRecorder())


@pytest.mark.parametrize(('name', 'message'), [
    ('mondo-relevance.similarity.dump', 'Unexpected similarity filename'),
    ('hpo-nonsense.similarity.dump', 'Unknown similarity method'),
    ('hpo-relevance-xyz.similarity.dump', 'Unknown corpus prefix'),
])
def test_similarity_dump_filename_validation(name, message):
    from pathlib import Path

    with pytest.raises(ValueError, match=message):
        similarity._parse_similarity_dump_filename(Path(name), ConceptPrefix.HPO)
