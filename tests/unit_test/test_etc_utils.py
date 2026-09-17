import zipfile

import httpx
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import aiter_progress, download_file, extract_file_from_zip, iter_progress


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


@pytest.mark.asyncio
async def test_download_file_fresh_download_sends_no_range_header(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    client = _FakeDownloadClient([_FakeStreamResponse(200, b'hello world')])

    await download_file('http://example.com/f', 'sub/f.bin', download_client=client)

    assert (tmp_path / 'sub' / 'f.bin').read_bytes() == b'hello world'
    assert 'Range' not in client.calls[0]


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
