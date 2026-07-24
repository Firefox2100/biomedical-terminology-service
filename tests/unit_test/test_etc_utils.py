import os

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.utils import aiter_progress, iter_progress


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
