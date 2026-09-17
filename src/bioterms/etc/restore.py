from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import TypeVar


T = TypeVar('T')


async def batched_write(items: Iterable[T] | AsyncIterable[T],
                        writer: Callable[[list[T]], Awaitable[None]],
                        batch_size: int,
                        ) -> int:
    """Write sync or async items in bounded batches and return the item count."""
    if batch_size < 1:
        raise ValueError('batch_size must be at least 1')

    async def as_async_iter() -> AsyncIterator[T]:
        if isinstance(items, AsyncIterable):
            async for item in items:
                yield item
        else:
            for item in items:
                yield item

    total = 0
    batch: list[T] = []
    async for item in as_async_iter():
        batch.append(item)
        if len(batch) == batch_size:
            await writer(batch)
            total += len(batch)
            batch = []

    if batch:
        await writer(batch)
        total += len(batch)

    return total
