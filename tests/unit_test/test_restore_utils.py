import pytest

from bioterms.etc.restore import batched_write


@pytest.mark.asyncio
async def test_batched_write_accepts_sync_items():
    batches = []

    async def write(batch):
        batches.append(batch)

    count = await batched_write(range(5), write, batch_size=2)

    assert count == 5
    assert batches == [[0, 1], [2, 3], [4]]


@pytest.mark.asyncio
async def test_batched_write_accepts_async_items():
    async def items():
        for value in range(3):
            yield value

    batches = []

    async def write(batch):
        batches.append(batch)

    assert await batched_write(items(), write, batch_size=2) == 3
    assert batches == [[0, 1], [2]]


@pytest.mark.asyncio
async def test_batched_write_rejects_invalid_batch_size():
    with pytest.raises(ValueError, match='batch_size'):
        await batched_write([], lambda _: None, batch_size=0)
