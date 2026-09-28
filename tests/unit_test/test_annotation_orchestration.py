"""
Tests for the annotation orchestration layer in `bioterms.annotation`, using a stand-in
annotation module so both the module-hook and generic-fallback paths are exercised.
"""
import types

import pytest

import bioterms.annotation as annotation
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.model.annotation import Annotation
from bioterms.model.annotation_status import AnnotationStatus


class Recorder:
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
        ANNOTATION_NAME='HPO to MONDO', VOCABULARY_PREFIX_1=ConceptPrefix.HPO,
        VOCABULARY_PREFIX_2=ConceptPrefix.MONDO, FILE_PATHS=['ann/mapping.tsv'], **hooks,
    )


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    (tmp_path / 'ann').mkdir()
    (tmp_path / 'offline').mkdir()
    return tmp_path


@pytest.fixture
def cache(monkeypatch):
    recorder = Recorder()
    monkeypatch.setattr(annotation, 'get_active_cache', lambda: recorder)
    return recorder


def _use(monkeypatch, module):
    monkeypatch.setattr(annotation, 'get_annotation_module', lambda _p1, _p2: module)


def _annotation(target='MONDO:1'):
    return Annotation(prefixFrom=ConceptPrefix.HPO, conceptIdFrom='HP:1', prefixTo=ConceptPrefix.MONDO,
                      conceptIdTo=target, annotationType=AnnotationType.ANNOTATED_WITH)


@pytest.mark.asyncio
async def test_delete_files_fallback_removes_files_under_data_dir(monkeypatch, data_dir, tmp_path):
    # Regression: the fallback used to resolve FILE_PATHS against the working directory.
    _use(monkeypatch, _module())
    (data_dir / 'ann' / 'mapping.tsv').write_text('old')
    monkeypatch.chdir(tmp_path.parent)

    await annotation.delete_annotation_files(ConceptPrefix.HPO, ConceptPrefix.MONDO)

    assert not (data_dir / 'ann' / 'mapping.tsv').exists()


@pytest.mark.asyncio
async def test_download_redownloads_through_module_hooks(monkeypatch):
    calls = []

    async def download(download_client):
        calls.append(('download', download_client))

    _use(monkeypatch, _module(download_annotation=download, delete_annotation_files=lambda: calls.append('delete')))

    await annotation.download_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, redownload=True, download_client='c')

    assert calls == ['delete', ('download', 'c')]


@pytest.mark.asyncio
async def test_download_requires_module_downloader(monkeypatch):
    _use(monkeypatch, _module())

    with pytest.raises(ValueError, match='does not have a download_annotation'):
        await annotation.download_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO)


@pytest.mark.asyncio
async def test_delete_annotation_fallback_and_hook(monkeypatch, cache):
    graph_db = Recorder()
    _use(monkeypatch, _module())

    await annotation.delete_annotation(ConceptPrefix.MONDO, ConceptPrefix.HPO, graph_db=graph_db)

    # The module's canonical (prefix_1, prefix_2) order is used, whatever order was asked for.
    assert graph_db.calls == [('delete_annotations', {'prefix_1': ConceptPrefix.HPO, 'prefix_2': ConceptPrefix.MONDO})]

    hooked = []
    _use(monkeypatch, _module(delete_annotation_data=lambda graph_db: hooked.append(graph_db)))
    await annotation.delete_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, graph_db='g')

    assert hooked == ['g']
    assert cache.names() == ['rotate_dataset_version', 'rotate_dataset_version']


@pytest.mark.asyncio
async def test_load_online_overwrites_then_loads(monkeypatch, data_dir, cache):
    (data_dir / 'ann' / 'mapping.tsv').write_text('x')
    loaded = []
    _use(monkeypatch, _module(load_annotation_from_file=lambda graph_db: loaded.append(graph_db)))
    deleted = []

    async def delete(**kwargs):
        deleted.append(kwargs)

    monkeypatch.setattr(annotation, 'delete_annotation', delete)

    await annotation.load_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, graph_db='g')

    assert deleted == [{'prefix_1': ConceptPrefix.HPO, 'prefix_2': ConceptPrefix.MONDO, 'graph_db': 'g'}]
    assert loaded == ['g']
    assert cache.names() == ['rotate_dataset_version']


@pytest.mark.asyncio
async def test_load_validates_files_and_loader(monkeypatch, data_dir):
    _use(monkeypatch, _module())

    with pytest.raises(ValueError, match='not found. Are they downloaded'):
        await annotation.load_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO)

    (data_dir / 'ann' / 'mapping.tsv').write_text('x')
    with pytest.raises(ValueError, match='does not have a load_annotation_from_file'):
        await annotation.load_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, overwrite=False)


@pytest.mark.asyncio
async def test_offline_load_writes_dump_through_adapter(monkeypatch, data_dir):
    (data_dir / 'ann' / 'mapping.tsv').write_text('x')
    seen = {}

    async def load(graph_db):
        seen['terms'] = await graph_db.count_terms(ConceptPrefix.HPO)
        seen['before'] = await graph_db.count_annotations(ConceptPrefix.HPO, ConceptPrefix.MONDO)
        await graph_db.save_annotations([_annotation('MONDO:1')])
        await graph_db.save_annotations([_annotation('MONDO:2')])

    _use(monkeypatch, _module(load_annotation_from_file=load))

    await annotation.load_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, offline=True)
    dump = data_dir / 'offline' / 'hpo-mondo.annotation.dump'
    first_run = dump.read_text()
    await annotation.load_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, offline=True, overwrite=False)

    assert seen == {'terms': 1, 'before': 1}  # second run sees the dump written by the first
    # Batches after the first append; overwrite only truncates at the start of a run.
    assert first_run == (
        'hpo,hpo:HP:1,mondo,mondo:1,annotated_with,{}\n'
        'hpo,hpo:HP:1,mondo,mondo:2,annotated_with,{}\n'
    )
    assert dump.read_text() == first_run * 2


@pytest.mark.asyncio
@pytest.mark.parametrize('is_async', [True, False])
async def test_offline_tolerates_unimplemented_loader_only_with_existing_dump(monkeypatch, data_dir, is_async):
    (data_dir / 'ann' / 'mapping.tsv').write_text('x')

    def sync_load(graph_db):
        raise NotImplementedError

    async def async_load(graph_db):
        raise NotImplementedError

    _use(monkeypatch, _module(load_annotation_from_file=async_load if is_async else sync_load))

    with pytest.raises(NotImplementedError):
        await annotation.load_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, offline=True)

    (data_dir / 'offline' / 'hpo-mondo.annotation.dump').write_text('hpo,1,mondo,1,annotated_with,\n')
    await annotation.load_annotation(ConceptPrefix.HPO, ConceptPrefix.MONDO, offline=True)


@pytest.mark.asyncio
async def test_annotation_status_uses_cache_then_counts(monkeypatch):
    _use(monkeypatch, _module())
    cached = AnnotationStatus(prefixSource=ConceptPrefix.HPO, prefixTarget=ConceptPrefix.MONDO,
                              name='cached', loaded=True, relationshipCount=9)
    hit_cache = Recorder(get_annotation_status=cached)
    miss_cache, graph_db = Recorder(), Recorder(count_annotations=4)

    assert await annotation.get_annotation_status(ConceptPrefix.HPO, ConceptPrefix.MONDO, cache=hit_cache) is cached
    status = await annotation.get_annotation_status(
        ConceptPrefix.MONDO, ConceptPrefix.HPO, cache=miss_cache, graph_db=graph_db,
    )

    assert (status.name, status.loaded, status.relationship_count) == ('HPO to MONDO', True, 4)
    assert status.prefix_source == ConceptPrefix.HPO
    assert miss_cache.names() == ['get_annotation_status', 'save_annotation_status']


def test_unknown_annotation_pair_is_rejected():
    with pytest.raises(ValueError, match='No annotation available'):
        annotation.get_annotation_module(ConceptPrefix.CTV3, ConceptPrefix.UNIPROT)
