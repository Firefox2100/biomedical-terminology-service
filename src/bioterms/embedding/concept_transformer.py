import asyncio
from concurrent.futures import ProcessPoolExecutor
from collections.abc import AsyncIterator

from bioterms.etc.consts import CONFIG
from bioterms.etc.utils import aiter_progress
from bioterms.model.concept import Concept, EmbeddingItem
from .text_transformer import TextTransformer


_PROCESS_TRANSFORMER: TextTransformer | None = None


def _init_embed_worker():
    """
    Initialiser for embedding worker processes.
    """
    global _PROCESS_TRANSFORMER
    _PROCESS_TRANSFORMER = TextTransformer()


def _embed_item_text_batch(batch: list[EmbeddingItem]) -> list[tuple[EmbeddingItem, list[float]]]:
    """
    Embed a single batch of embedding items inside a worker process.
    """
    global _PROCESS_TRANSFORMER

    if _PROCESS_TRANSFORMER is None:
        _PROCESS_TRANSFORMER = TextTransformer()

    vectors = _PROCESS_TRANSFORMER.embed_strings(
        texts=[item.text for item in batch],
    )
    return list(zip(batch, vectors))


async def _item_batches_iter(concepts: list[Concept] | AsyncIterator[Concept],
                             batch_size: int,
                             total_concepts: int | None,
                             ) -> AsyncIterator[list[EmbeddingItem]]:
    """
    Flatten a list or async iterator of concepts into their EmbeddingItems, and batch those
    items into fixed-size chunks for embedding. A concept's items are never split across two
    batches' worth of *different* concepts arbitrarily -- item order simply follows concept
    order -- but a single concept's own items can span a batch boundary, since the batch size
    bounds embedding call size, not concept count.
    :param concepts: A list or async iterator of Concept instances to batch.
    :param batch_size: The number of embedding items per batch.
    :param total_concepts: Optional total number of concepts, used for progress tracking.
    :return: An async iterator of EmbeddingItem batches.
    """
    async def concept_source():
        if isinstance(concepts, AsyncIterator):
            async for concept in aiter_progress(
                concepts,
                description='Embedding concepts',
                total=total_concepts,
            ):
                yield concept
        elif isinstance(concepts, list):
            for concept in concepts:
                yield concept
        else:
            raise TypeError('concepts must be a list or an AsyncIterator of Concept instances')

    batch: list[EmbeddingItem] = []
    async for concept in concept_source():
        batch.extend(concept.embedding_items())

        while len(batch) >= batch_size:
            yield batch[:batch_size]
            batch = batch[batch_size:]

    if batch:
        yield batch


class ConceptTransformer(TextTransformer):
    """
    A class with convenient methods for transforming Concept instances into per-item
    embeddings (see `Concept.embedding_items`).
    """

    def _process_batch(self,
                       batch: list[EmbeddingItem],
                       ) -> list[tuple[EmbeddingItem, list[float]]]:
        """
        Embed a batch of embedding items synchronously in the current process.
        :param batch: The batch of EmbeddingItem instances to embed.
        :return: A list of (EmbeddingItem, embedding_vector) tuples.
        """
        vectors = self.embed_strings(
            texts=[item.text for item in batch],
        )

        return list(zip(batch, vectors))

    @staticmethod
    async def _embed_parallel(item_batches: AsyncIterator[list[EmbeddingItem]],
                              worker_processes: int,
                              ) -> AsyncIterator[list[tuple[EmbeddingItem, list[float]]]]:
        """
        Embed item batches across a pool of worker processes, streaming results as they
        complete rather than waiting for the whole pool to finish.
        :param item_batches: An async iterator of EmbeddingItem batches to embed.
        :param worker_processes: Number of worker processes to use.
        :return: An async iterator of chunks of (EmbeddingItem, embedding_vector) tuples.
        """
        loop = asyncio.get_running_loop()
        queue_size = max(worker_processes * 2, 1)

        with ProcessPoolExecutor(
            max_workers=worker_processes,
            initializer=_init_embed_worker,
        ) as executor:
            pending: set[asyncio.Future] = set()

            async for batch in item_batches:
                pending.add(loop.run_in_executor(
                    executor,
                    _embed_item_text_batch,
                    batch,
                ))

                if len(pending) >= queue_size:
                    done, pending = await asyncio.wait(
                        pending,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for fut in done:
                        yield fut.result()

            while pending:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for fut in done:
                    yield fut.result()

    async def embed_concepts(self,
                             concepts: list[Concept] | AsyncIterator[Concept],
                             batch_size: int | None = None,
                             worker_processes: int | None = None,
                             total_concepts: int | None = None,
                             ) -> AsyncIterator[list[tuple[EmbeddingItem, list[float]]]]:
        """
        Embed every concept's embedding items using the configured SentenceTransformer model.
        :param concepts: A list or async iterator of Concept instances to embed
        :param batch_size: Number of embedding items to process in each batch
        :param worker_processes: Number of worker processes for embedding. If None, uses config
        :param total_concepts: Optional total number of concepts, used for progress tracking
        :return: An iterator of chunks of (EmbeddingItem, embedding_vector) tuples
        """
        if batch_size is None:
            batch_size = CONFIG.embedding_batch_size
        if worker_processes is None:
            worker_processes = CONFIG.embedding_process_limit

        if batch_size < 1:
            raise ValueError('batch_size must be at least 1')
        if worker_processes < 1:
            raise ValueError('worker_processes must be at least 1')

        batches = _item_batches_iter(concepts, batch_size, total_concepts)

        if worker_processes == 1:
            # Run the (blocking, GPU/CPU-bound) encode call in a worker thread rather than
            # directly on the event loop -- otherwise it monopolises the loop for its whole
            # duration, starving any concurrent async I/O (e.g. a writer task consuming this
            # generator's output while flushing previous batches to the database) even though
            # that I/O has nothing to do with the GPU and could otherwise run alongside it.
            loop = asyncio.get_running_loop()
            async for batch in batches:
                yield await loop.run_in_executor(None, self._process_batch, batch)
            return

        async for embedded_batch in self._embed_parallel(batches, worker_processes):
            yield embedded_batch
