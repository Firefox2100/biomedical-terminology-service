import zipfile

import httpx
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.errors import FilesNotFound
from bioterms.etc import utils
from bioterms.etc.utils import aiter_progress, batch_iterable, download_file, extract_file_from_zip, iter_progress


async def _agen(n):
    for i in range(n):
        yield i


@pytest.mark.asyncio
async def test_aiter_progress_yields_all_items_when_progress_bar_disabled(monkeypatch):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', True)

    items = [item async for item in aiter_progress(_agen(5), description='test')]

    assert items == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_aiter_progress_yields_all_items_when_progress_bar_enabled(monkeypatch):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', False)

    items = [item async for item in aiter_progress(_agen(5), description='test', total=5)]

    assert items == [0, 1, 2, 3, 4]


def test_iter_progress_yields_all_items_when_progress_bar_disabled(monkeypatch):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', True)

    items = list(iter_progress(range(5), description='test'))

    assert items == [0, 1, 2, 3, 4]


def test_iter_progress_accepts_tqdm_style_desc_alias(monkeypatch):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', False)

    assert list(iter_progress(range(2), desc='test', total=2)) == [0, 1]


def test_batch_iterable_does_not_construct_progress_when_disabled(monkeypatch):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', True)
    monkeypatch.setattr(utils, 'Progress', lambda *args, **kwargs: pytest.fail('progress constructed'))

    assert list(batch_iterable(range(5), batch_size=2)) == [[0, 1], [2, 3], [4]]


class _FakeStreamResponse:
    """A minimal stand-in for httpx.Response as used via client.stream(...)."""

    def __init__(self, status_code: int, body: bytes, fail_after_bytes: int = None, fail_exc: Exception = None):
        self.status_code = status_code
        self._body = body
        self._fail_after_bytes = fail_after_bytes
        self._fail_exc = fail_exc

    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code != 416:
            raise httpx.HTTPStatusError('error', request=None, response=self)

    async def aiter_bytes(self):
        if self._fail_after_bytes is not None:
            yield self._body[:self._fail_after_bytes]
            raise self._fail_exc
        yield self._body


class _FakeStreamContextManager:
    def __init__(self, response_or_exc):
        self._response_or_exc = response_or_exc

    async def __aenter__(self):
        if isinstance(self._response_or_exc, Exception):
            raise self._response_or_exc
        return self._response_or_exc

    async def __aexit__(self, *exc_info):
        return False


class _FakeDownloadClient:
    """Queues one canned response (or exception) per call to .stream(...)."""

    def __init__(self, responses: list):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def stream(self, method, url, follow_redirects=True, headers=None):
        self.calls.append(dict(headers or {}))
        return _FakeStreamContextManager(self._responses.pop(0))


class _RecordingProgress:
    """Stands in for rich.Progress and records the task set-up and advances."""
    instances: list = []

    def __init__(self, *columns, transient=False):
        self.transient = transient
        self.task = None
        self.advanced = 0
        _RecordingProgress.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def add_task(self, description, total, completed):
        self.task = {'description': description, 'total': total, 'completed': completed}
        return 0

    def advance(self, _task, amount):
        self.advanced += amount


@pytest.mark.asyncio
@pytest.mark.parametrize(('content_length', 'expected_total', 'transient'), [
    ('6', 10, False),   # known size: total counts the resumed prefix too
    (None, None, True),  # unknown size: indeterminate, transient progress bar
])
async def test_download_file_reports_resumed_progress(monkeypatch, tmp_path, content_length,
                                                      expected_total, transient):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', False)
    monkeypatch.setattr(utils, 'Progress', _RecordingProgress)
    monkeypatch.setattr(_RecordingProgress, 'instances', [])
    (tmp_path / 'f.bin').write_bytes(b'abcd')
    response = _FakeStreamResponse(206, b'efghij')
    response.headers = {'content-length': content_length} if content_length else {}
    client = _FakeDownloadClient([response])

    await download_file('http://example.com/f', 'f.bin', download_client=client)

    assert (tmp_path / 'f.bin').read_bytes() == b'abcdefghij'
    progress = _RecordingProgress.instances[0]
    assert progress.task == {'description': 'Downloading f.bin', 'total': expected_total, 'completed': 4}
    assert progress.advanced == 6
    assert progress.transient is transient


@pytest.mark.asyncio
async def test_download_file_fresh_download_sends_no_range_header(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    client = _FakeDownloadClient([_FakeStreamResponse(200, b'hello world')])

    await download_file('http://example.com/f', 'sub/f.bin', download_client=client)

    assert (tmp_path / 'sub' / 'f.bin').read_bytes() == b'hello world'
    assert 'Range' not in client.calls[0]


@pytest.mark.asyncio
async def test_download_file_redacts_url_credentials_in_logs(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    client = _FakeDownloadClient([_FakeStreamResponse(200, b'content')])

    with caplog.at_level('INFO'):
        await download_file(
            'https://example.com/file?apiKey=secret-value&release=current',
            'f.bin', download_client=client,
        )

    assert 'secret-value' not in caplog.text
    assert '%5BREDACTED%5D' in caplog.text
    assert 'release=current' in caplog.text


@pytest.mark.asyncio
async def test_download_file_resumes_partial_file_with_range_header(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    target = tmp_path / 'f.bin'
    target.write_bytes(b'hello ')
    client = _FakeDownloadClient([_FakeStreamResponse(206, b'world')])

    await download_file('http://example.com/f', 'f.bin', download_client=client)

    assert target.read_bytes() == b'hello world'
    assert client.calls[0]['Range'] == 'bytes=6-'


@pytest.mark.asyncio
async def test_download_file_restarts_when_server_ignores_range(monkeypatch, tmp_path):
    # Server responds 200 (full body) instead of 206 to a Range request -- the existing
    # partial file can't be trusted as a valid prefix of a fresh full response.
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    target = tmp_path / 'f.bin'
    target.write_bytes(b'stale-partial-data-that-does-not-belong')
    client = _FakeDownloadClient([_FakeStreamResponse(200, b'fresh full content')])

    await download_file('http://example.com/f', 'f.bin', download_client=client)

    assert target.read_bytes() == b'fresh full content'


@pytest.mark.asyncio
async def test_download_file_treats_416_as_already_complete(monkeypatch, tmp_path):
    # Verified against the real UniProt server: a Range request starting exactly at the
    # resource's current size gets a 416 back, not an error -- confirms the file on disk
    # already is the complete download.
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    target = tmp_path / 'f.bin'
    target.write_bytes(b'already complete')
    client = _FakeDownloadClient([_FakeStreamResponse(416, b'')])

    await download_file('http://example.com/f', 'f.bin', download_client=client)

    assert target.read_bytes() == b'already complete'
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_download_file_retries_and_resumes_after_transient_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    monkeypatch.setattr(CONFIG, 'download_retry_backoff_seconds', 0.0)

    client = _FakeDownloadClient([
        _FakeStreamResponse(200, b'hello world', fail_after_bytes=5, fail_exc=httpx.ReadTimeout('boom')),
        _FakeStreamResponse(206, b' world'),
    ])

    await download_file('http://example.com/f', 'f.bin', download_client=client)

    assert (tmp_path / 'f.bin').read_bytes() == b'hello world'
    assert 'Range' not in client.calls[0]
    assert client.calls[1]['Range'] == 'bytes=5-'


@pytest.mark.asyncio
async def test_download_file_raises_after_exhausting_retries(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    monkeypatch.setattr(CONFIG, 'download_retry_backoff_seconds', 0.0)
    monkeypatch.setattr(CONFIG, 'download_max_retries', 2)

    client = _FakeDownloadClient([
        httpx.ReadTimeout('boom1'),
        httpx.ReadTimeout('boom2'),
    ])

    with pytest.raises(httpx.ReadTimeout):
        await download_file('http://example.com/f', 'f.bin', download_client=client)

    assert len(client.calls) == 2


def _make_zip(path, entries: dict[str, bytes]):
    with zipfile.ZipFile(path, 'w') as zf:
        for name, content in entries.items():
            zf.writestr(name, content)


@pytest.mark.asyncio
async def test_extract_file_from_zip_required_pattern_matches(tmp_path):
    zip_path = tmp_path / 'release.zip'
    _make_zip(zip_path, {'Full/Terminology/sct2_Concept_INT_20240101.txt': b'concept-data'})
    dest = tmp_path / 'out' / 'concept.txt'

    await extract_file_from_zip(str(zip_path), [
        ('Full/Terminology/sct2_Concept*.txt', str(dest)),
    ])

    assert dest.read_bytes() == b'concept-data'


@pytest.mark.asyncio
async def test_extract_file_from_zip_required_pattern_missing_raises_with_diagnostics(tmp_path):
    zip_path = tmp_path / 'release.zip'
    _make_zip(zip_path, {
        'Full/Refset/Content/der2_cRefset_SomeOtherRefsetFull_INT_20240101.txt': b'x',
    })
    dest = tmp_path / 'out' / 'association.txt'

    with pytest.raises(FilesNotFound) as exc_info:
        await extract_file_from_zip(str(zip_path), [
            ('Full/Refset/Content/der2_*Refset_Association*Full*.txt', str(dest)),
        ])

    # The diagnostic must name what actually is in that directory, not just say "not found".
    assert 'der2_cRefset_SomeOtherRefsetFull_INT_20240101.txt' in str(exc_info.value)
    assert not dest.exists()


@pytest.mark.asyncio
async def test_extract_file_from_zip_optional_pattern_missing_warns_and_continues(tmp_path):
    zip_path = tmp_path / 'release.zip'
    _make_zip(zip_path, {
        'Full/Terminology/sct2_Concept_INT_20240101.txt': b'concept-data',
        # No matching association refset file in this archive.
    })
    concept_dest = tmp_path / 'out' / 'concept.txt'
    association_dest = tmp_path / 'out' / 'association.txt'

    with pytest.warns(UserWarning, match='der2_.*Association.*Full'):
        await extract_file_from_zip(str(zip_path), [
            ('Full/Terminology/sct2_Concept*.txt', str(concept_dest)),
            ('Full/Refset/Content/der2_*Refset_Association*Full*.txt', str(association_dest), False),
        ])

    # Required file still extracted; optional one skipped without aborting the batch.
    assert concept_dest.read_bytes() == b'concept-data'
    assert not association_dest.exists()


@pytest.mark.asyncio
async def test_extract_file_from_zip_optional_pattern_present_still_extracts(tmp_path):
    zip_path = tmp_path / 'release.zip'
    _make_zip(zip_path, {
        'Full/Refset/Content/der2_cRefset_AssociationReferenceFull_INT_20240101.txt': b'assoc-data',
    })
    dest = tmp_path / 'out' / 'association.txt'

    await extract_file_from_zip(str(zip_path), [
        ('Full/Refset/Content/der2_*Refset_Association*Full*.txt', str(dest), False),
    ])

    assert dest.read_bytes() == b'assoc-data'


# --- batching, peeking and archive helpers ---------------------------------------------------

import gzip
import io
import tarfile

from bioterms.etc.utils import peek_first


@pytest.mark.parametrize('progress_disabled', [True, False])
@pytest.mark.parametrize('consume', [False, True])
def test_batch_iterable_on_lists(monkeypatch, progress_disabled, consume):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', progress_disabled)
    items = list(range(7))

    batches = list(batch_iterable(items, batch_size=3, consume=consume))

    assert sorted(x for batch in batches for x in batch) == list(range(7))
    assert [len(b) for b in batches] == [3, 3, 1]
    assert items == ([] if consume else list(range(7)))


def test_batch_iterable_single_batch_and_empty(monkeypatch):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', False)

    assert list(batch_iterable([1, 2], batch_size=5)) == [[1, 2]]
    assert list(batch_iterable([], batch_size=5)) == []


def test_peek_first_keeps_every_item():
    listed = [1, 2, 3]
    first, rest = peek_first(listed)
    assert (first, rest) == (1, listed)

    first, rest = peek_first(x for x in 'abc')
    assert (first, list(rest)) == ('a', ['a', 'b', 'c'])

    assert peek_first([])[0] is None
    assert peek_first(iter(()))[0] is None


@pytest.mark.asyncio
async def test_get_trud_release_url():
    def handler(request):
        if request.url.path.endswith('/ok'):
            return httpx.Response(200, json={'httpStatus': 200, 'releases': [{'archiveFileUrl': 'https://x/r.zip'}]})
        return httpx.Response(200, json={'httpStatus': 401, 'message': 'bad key'})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url='https://trud') as client:
        assert await utils.get_trud_release_url('https://trud/ok', client=client) == 'https://x/r.zip'
        with pytest.raises(ValueError, match='bad key'):
            await utils.get_trud_release_url('https://trud/denied', client=client)


@pytest.mark.asyncio
async def test_extract_file_from_gzip_streams_in_chunks(tmp_path):
    payload = b'line\n' * 10_000
    source = tmp_path / 'data.gz'
    source.write_bytes(gzip.compress(payload))

    await utils.extract_file_from_gzip(str(source), str(tmp_path / 'data'), chunk_size=1024)

    assert (tmp_path / 'data').read_bytes() == payload


def test_extract_tarball_optionally_filters_members(tmp_path):
    archive = tmp_path / 'release.tar.gz'
    with tarfile.open(archive, 'w:gz') as tar:
        for name in ('keep.txt', 'skip.txt'):
            info = tarfile.TarInfo(name)
            info.size = len(name)
            tar.addfile(info, io.BytesIO(name.encode()))

    utils._extract_tarball_sync(str(archive), str(tmp_path / 'all'))
    utils._extract_tarball_sync(str(archive), str(tmp_path / 'some'), members=['keep.txt'])

    assert sorted(p.name for p in (tmp_path / 'all').iterdir()) == ['keep.txt', 'skip.txt']
    assert [p.name for p in (tmp_path / 'some').iterdir()] == ['keep.txt']


def _tampered_archive(path, member):
    with tarfile.open(path, 'w:gz') as tar:
        info = tarfile.TarInfo(member)
        info.size = 4
        tar.addfile(info, io.BytesIO(b'evil'))
    return str(path)


def test_extract_tarball_refuses_path_traversal(tmp_path):
    archive = _tampered_archive(tmp_path / 'tampered.tar.gz', '../escaped.txt')

    with pytest.raises(tarfile.FilterError):
        utils._extract_tarball_sync(archive, str(tmp_path / 'out'))

    assert not (tmp_path / 'escaped.txt').exists()


def test_extract_tarball_confines_absolute_members_to_output_dir(tmp_path):
    target = tmp_path / 'absolute.txt'
    archive = _tampered_archive(tmp_path / 'tampered.tar.gz', str(target))

    utils._extract_tarball_sync(archive, str(tmp_path / 'out'))

    assert not target.exists()
    assert (tmp_path / 'out' / str(target).lstrip('/')).read_bytes() == b'evil'


@pytest.mark.asyncio
async def test_download_rf2_downloads_then_extracts(monkeypatch):
    calls = []

    async def fake_download(url, file_path, download_client=None):
        calls.append(('download', url, file_path.endswith('.zip')))

    async def fake_extract(zip_path, file_mapping):
        calls.append(('extract', zip_path.endswith('.zip'), file_mapping))

    monkeypatch.setattr(utils, 'download_file', fake_download)
    monkeypatch.setattr(utils, 'extract_file_from_zip', fake_extract)

    await utils.download_rf2('https://x/rf2.zip', [('*Concept*', 'snomed/concept.txt')])

    assert calls == [('download', 'https://x/rf2.zip', True), ('extract', True, [('*Concept*', 'snomed/concept.txt')])]


@pytest.mark.asyncio
async def test_download_obo_owl_release_skips_existing_files(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    client = _FakeDownloadClient([_FakeStreamResponse(200, b'<owl/>')])

    await utils.download_obo_owl_release('https://x/hp.owl', 'hpo/hp.owl', download_client=client)
    await utils.download_obo_owl_release('https://x/hp.owl', 'hpo/hp.owl', download_client=client)

    assert (tmp_path / 'hpo' / 'hp.owl').read_bytes() == b'<owl/>'
    assert len(client.calls) == 1


# --- error reporting --------------------------------------------------------------------------

@pytest.fixture
def sentry(monkeypatch):
    import sentry_sdk

    state = {'initialised': False, 'options': None}
    monkeypatch.setattr(sentry_sdk, 'is_initialized', lambda: state['initialised'])
    monkeypatch.setattr(sentry_sdk, 'init', lambda **options: state.update(options=options))
    return state


@pytest.mark.parametrize(('enabled', 'dsn', 'expected'), [(False, 'https://k@sentry/1', False), (True, None, False)])
def test_error_reporting_needs_flag_and_dsn(monkeypatch, sentry, enabled, dsn, expected):
    monkeypatch.setattr(CONFIG, 'enable_error_reporting', enabled)
    monkeypatch.setattr(CONFIG, 'sentry_dsn', dsn)

    assert utils.initialize_error_reporting('1.0') is expected
    assert sentry['options'] is None


@pytest.mark.parametrize('profiling', [False, True])
def test_error_reporting_initialises_sentry_once(monkeypatch, sentry, profiling):
    monkeypatch.setattr(CONFIG, 'enable_error_reporting', True)
    monkeypatch.setattr(CONFIG, 'sentry_dsn', 'https://k@sentry/1')
    monkeypatch.setattr(CONFIG, 'enable_profiling', profiling)

    assert utils.initialize_error_reporting('2.0.0') is True
    assert sentry['options']['release'] == '2.0.0'
    assert ('traces_sample_rate' in sentry['options']) is profiling

    sentry['initialised'] = True
    sentry['options'] = None
    assert utils.initialize_error_reporting() is True
    assert sentry['options'] is None
