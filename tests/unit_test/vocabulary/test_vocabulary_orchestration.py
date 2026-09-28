"""
Tests for the vocabulary orchestration layer in `bioterms.vocabulary`: each operation either
delegates to a vocabulary module's own hook or falls back to the generic database calls.
A stand-in vocabulary module and recording database fakes exercise both paths.
"""
import types

import numpy as np
import pytest

import bioterms.vocabulary as vocabulary
from bioterms.embedding import EmbeddingContainerFileV2, EmbeddingContainerV2
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, EmbeddingKind
from bioterms.model.concept import Concept
from bioterms.model.vocabulary_status import VocabularyStatus


class Recorder:
    """Records every awaited method call as (name, kwargs)."""

    def __init__(self, **returns):
        self.calls = []
        self._returns = returns

    def __getattr__(self, name):
        async def method(*args, **kwargs):
            self.calls.append((name, kwargs or args))
            return self._returns.get(name)
        return method

    def names(self):
        return [name for name, _ in self.calls]


def _module(**hooks):
    return types.SimpleNamespace(
        VOCABULARY_NAME='Test', VOCABULARY_PREFIX=ConceptPrefix.HPO, ANNOTATIONS=[],
        SIMILARITY_METHODS=[], FILE_PATHS=['test/terms.owl', 'test/extra.txt'],
        TIMESTAMP_FILE='test/.timestamp', CONCEPT_CLASS=Concept, **hooks,
    )


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    (tmp_path / 'test').mkdir()
    return tmp_path


@pytest.fixture
def cache(monkeypatch):
    recorder = Recorder()
    monkeypatch.setattr(vocabulary, 'get_active_cache', lambda: recorder)
    return recorder


def _use_module(monkeypatch, module):
    monkeypatch.setattr(vocabulary, 'get_vocabulary_module', lambda _prefix: module)


@pytest.mark.asyncio
async def test_delete_files_fallback_removes_files_under_data_dir(monkeypatch, data_dir, tmp_path):
    # Regression: fallback deletion used to remove FILE_PATHS relative to the working
    # directory, so `--redownload` resumed onto the stale file instead of replacing it.
    _use_module(monkeypatch, _module())
    for name in ('terms.owl', '.timestamp'):
        (data_dir / 'test' / name).write_text('old release')
    monkeypatch.chdir(tmp_path.parent)

    await vocabulary.delete_vocabulary_files(ConceptPrefix.HPO)

    assert not (data_dir / 'test' / 'terms.owl').exists()
    assert not (data_dir / 'test' / '.timestamp').exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('is_async', [True, False])
async def test_delete_files_prefers_module_hook(monkeypatch, is_async):
    calls = []

    async def async_hook():
        calls.append('async')

    _use_module(monkeypatch, _module(delete_vocabulary_files=async_hook if is_async else lambda: calls.append('sync')))

    await vocabulary.delete_vocabulary_files(ConceptPrefix.HPO)

    assert calls == ['async' if is_async else 'sync']


@pytest.mark.asyncio
async def test_download_redownloads_and_stamps_time(monkeypatch, data_dir):
    calls = []

    async def download():
        calls.append('download')
        (data_dir / 'test' / 'terms.owl').write_text('new release')

    _use_module(monkeypatch, _module(download_vocabulary=download))
    (data_dir / 'test' / 'terms.owl').write_text('old release')

    await vocabulary.download_vocabulary(ConceptPrefix.HPO, redownload=True)

    assert calls == ['download']
    assert (data_dir / 'test' / 'terms.owl').read_text() == 'new release'
    assert (data_dir / 'test' / '.timestamp').read_text().startswith('20')


@pytest.mark.asyncio
async def test_download_requires_module_downloader(monkeypatch, data_dir):
    _use_module(monkeypatch, _module())

    with pytest.raises(ValueError, match='does not have a download_vocabulary'):
        await vocabulary.download_vocabulary(ConceptPrefix.HPO)


@pytest.mark.asyncio
async def test_create_indexes_fallback_and_hook(monkeypatch, cache):
    doc_db, graph_db = Recorder(), Recorder()
    _use_module(monkeypatch, _module())

    await vocabulary.create_indexes(ConceptPrefix.HPO, overwrite=True, doc_db=doc_db, graph_db=graph_db)

    assert doc_db.calls == [
        ('create_index', {'prefix': ConceptPrefix.HPO, 'field': 'conceptId', 'unique': True, 'overwrite': True}),
        ('create_index', {'prefix': ConceptPrefix.HPO, 'field': 'label', 'overwrite': True}),
    ]
    assert graph_db.names() == ['create_index']

    hook_calls = []
    _use_module(monkeypatch, _module(create_indexes=lambda **kwargs: hook_calls.append(kwargs)))
    await vocabulary.create_indexes(ConceptPrefix.HPO, doc_db=doc_db, graph_db=graph_db)

    assert hook_calls == [{'overwrite': False, 'doc_db': doc_db, 'graph_db': graph_db}]
    assert cache.names() == ['rotate_dataset_version', 'rotate_dataset_version']


@pytest.mark.asyncio
async def test_delete_vocabulary_fallback_clears_every_store(monkeypatch, cache):
    doc_db, graph_db, vector_db = Recorder(), Recorder(), Recorder()
    _use_module(monkeypatch, _module())

    await vocabulary.delete_vocabulary(ConceptPrefix.HPO, doc_db=doc_db, graph_db=graph_db, vector_db=vector_db)

    assert cache.names() == ['purge', 'rotate_dataset_version']
    assert doc_db.names() == ['delete_all_for_label']
    assert graph_db.names() == ['delete_vocabulary_graph']
    assert vector_db.names() == ['delete_vectors_for_prefix']


@pytest.mark.asyncio
async def test_delete_vocabulary_prefers_module_hook(monkeypatch, cache):
    calls = []

    async def hook(doc_db, graph_db):
        calls.append((doc_db, graph_db))

    _use_module(monkeypatch, _module(delete_vocabulary_data=hook))

    await vocabulary.delete_vocabulary(ConceptPrefix.HPO, doc_db='doc', graph_db='graph')

    assert calls == [('doc', 'graph')]
    assert cache.names() == ['rotate_dataset_version']


@pytest.mark.asyncio
async def test_load_vocabulary_online_drops_indexes_loads_and_invalidates(monkeypatch, data_dir, cache):
    loaded = []

    async def load_vocabulary_from_file(doc_db, graph_db, offline, build_search_index, load_annotations):
        loaded.append((offline, build_search_index, load_annotations))

    _use_module(monkeypatch, _module(load_vocabulary_from_file=load_vocabulary_from_file))
    for path in ('terms.owl', 'extra.txt'):
        (data_dir / 'test' / path).write_text('x')
    steps = []

    async def delete(**_kwargs):
        steps.append('delete')

    async def indexes(**_kwargs):
        steps.append('indexes')

    monkeypatch.setattr(vocabulary, 'delete_vocabulary', delete)
    monkeypatch.setattr(vocabulary, 'create_indexes', indexes)

    await vocabulary.load_vocabulary(ConceptPrefix.HPO, load_annotations=False)
    await vocabulary.load_vocabulary(ConceptPrefix.HPO, offline=True)

    assert steps == ['delete', 'indexes']  # offline mode touches no database
    assert loaded == [(False, True, False), (True, True, True)]
    assert cache.names() == ['purge', 'rotate_dataset_version']


@pytest.mark.asyncio
async def test_load_vocabulary_validates_files_and_loader(monkeypatch, data_dir):
    _use_module(monkeypatch, _module())

    with pytest.raises(ValueError, match='not found. Are they downloaded'):
        await vocabulary.load_vocabulary(ConceptPrefix.HPO)

    for path in ('terms.owl', 'extra.txt'):
        (data_dir / 'test' / path).write_text('x')
    with pytest.raises(ValueError, match='does not have a load_vocabulary_from_file'):
        await vocabulary.load_vocabulary(ConceptPrefix.HPO, offline=True)


def _status(loaded, count=2):
    return VocabularyStatus(prefix=ConceptPrefix.HPO, name='HPO', fileDownloaded=True, loaded=loaded,
                            conceptCount=count, relationshipCount=0, vectorCount=0,
                            annotations=[], similarityMethods=[])


@pytest.mark.asyncio
async def test_embed_vocabulary_online(monkeypatch, cache):
    async def status(**_kwargs):
        return _status(loaded=True, count=7)

    monkeypatch.setattr(vocabulary, 'get_vocabulary_status', status)
    doc_db = types.SimpleNamespace(get_terms_iter=lambda prefix, model_class: 'concept-stream')
    vector_db = Recorder()

    await vocabulary.embed_vocabulary(ConceptPrefix.HPO, doc_db=doc_db, vector_db=vector_db)

    assert vector_db.calls == [
        ('delete_vectors_for_prefix', {'prefix': ConceptPrefix.HPO}),
        ('insert_concepts', {'concepts': 'concept-stream', 'prefix': ConceptPrefix.HPO, 'total_concepts': 7}),
    ]
    assert cache.names() == ['rotate_dataset_version']


@pytest.mark.asyncio
async def test_embed_vocabulary_online_requires_loaded_vocabulary(monkeypatch):
    async def status(**_kwargs):
        return _status(loaded=False)

    monkeypatch.setattr(vocabulary, 'get_vocabulary_status', status)

    with pytest.raises(RuntimeError, match='is not loaded. Cannot embed'):
        await vocabulary.embed_vocabulary(ConceptPrefix.HPO, doc_db=object(), vector_db=Recorder())


async def _write_embedding_dump(path):
    async def items():
        for index, concept_id in enumerate(('HP:1', 'HP:2')):
            yield EmbeddingContainerV2(item_id=f'{concept_id}:alias:0', concept_id=concept_id,
                                       kind=EmbeddingKind.ALIAS, text=f'text {index}',
                                       vector=np.array([index, 1.0, 0.5], dtype=np.float32))

    await EmbeddingContainerFileV2(str(path), dim=3).write(items())


class _VectorRecorder(Recorder):
    async def load_embedding_items(self, prefix, items):
        self.items = [item async for item in items]
        self.calls.append(('load_embedding_items', {'prefix': prefix}))


@pytest.mark.asyncio
async def test_restore_embeddings_streams_dump_into_vector_db(monkeypatch, tmp_path, cache):
    async def status(**_kwargs):
        return _status(loaded=True)

    monkeypatch.setattr(vocabulary, 'get_vocabulary_status', status)
    await _write_embedding_dump(tmp_path / 'hpo.embed.dump')
    vector_db = _VectorRecorder()

    await vocabulary.restore_vocabulary_embeddings(
        ConceptPrefix.HPO, offline_dir=tmp_path, doc_db=object(), vector_db=vector_db,
    )

    assert vector_db.names() == ['delete_vectors_for_prefix', 'load_embedding_items']
    assert [(i.concept_id, i.text, i.vector) for i in vector_db.items] == [
        ('HP:1', 'text 0', [0.0, 1.0, 0.5]), ('HP:2', 'text 1', [1.0, 1.0, 0.5]),
    ]
    assert cache.names() == ['rotate_dataset_version']

    monkeypatch.setattr(vocabulary, 'get_vocabulary_status', lambda **_k: _async(_status(loaded=False)))
    with pytest.raises(RuntimeError, match='Cannot restore embeddings'):
        await vocabulary.restore_vocabulary_embeddings(ConceptPrefix.HPO, doc_db=object(), vector_db=vector_db)


async def _async(value):
    return value


@pytest.mark.asyncio
async def test_restore_vocabulary_with_overwrite_and_embeddings(monkeypatch, tmp_path, cache):
    (tmp_path / 'hpo.doc.dump').write_text(
        Concept(prefix=ConceptPrefix.HPO, conceptId='HP:1', label='One').model_dump_json(by_alias=True) + '\n'
    )
    (tmp_path / 'hpo.node_ids.dump').write_text('HP:1,[]\nHP:2,[]\n')
    (tmp_path / 'hpo.graph.dump').write_text('HP:2,HP:1,is_a,\n')
    await _write_embedding_dump(tmp_path / 'hpo.embed.dump')
    steps = []

    async def record(name, **kwargs):
        steps.append((name, kwargs.get('drop_existing')))

    monkeypatch.setattr(vocabulary, 'delete_vocabulary', lambda **k: record('delete', **k))
    monkeypatch.setattr(vocabulary, 'create_indexes', lambda **k: record('indexes', **k))
    monkeypatch.setattr(vocabulary, 'restore_vocabulary_embeddings', lambda prefix, **k: record('embeddings', **k))

    class GraphDb:
        async def save_vocabulary_graph(self, nodes, edges, consume_concepts):
            self.nodes = [n.concept_id for n in nodes]
            self.edges = list(edges)

    doc_db, graph_db = Recorder(), GraphDb()

    summary = await vocabulary.restore_vocabulary(
        ConceptPrefix.HPO, overwrite=True, offline_dir=tmp_path,
        doc_db=doc_db, graph_db=graph_db, vector_db=object(),
    )

    assert summary == {'conceptCount': 1, 'edgeCount': 1, 'embeddingsRestored': True}
    assert steps == [('delete', None), ('indexes', None), ('embeddings', True)]
    assert doc_db.calls[0][0] == 'save_terms'
    assert graph_db.nodes == ['HP:1', 'HP:2']
    assert graph_db.edges == [('HP:2', 'HP:1', 'is_a', None)]
    assert cache.names() == ['purge', 'rotate_dataset_version']


@pytest.mark.asyncio
async def test_restore_vocabulary_reports_missing_dumps(tmp_path):
    with pytest.raises(ValueError, match='Missing required offline dump file'):
        await vocabulary.restore_vocabulary(ConceptPrefix.HPO, offline_dir=tmp_path)


def test_vocabulary_license_lookup():
    assert vocabulary.get_vocabulary_license(ConceptPrefix.HPO)
