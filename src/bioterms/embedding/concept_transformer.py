import asyncio
from concurrent.futures import ProcessPoolExecutor
from collections.abc import AsyncIterator

from bioterms.etc.consts import CONFIG
from bioterms.etc.utils import aiter_progress
from bioterms.model.concept import Concept
from .text_transformer import TextTransformer


_PROCESS_TRANSFORMER: TextTransformer | None = None


def _init_embed_worker():
    """
    Initialiser for embedding worker processes.
    """
    global _PROCESS_TRANSFORMER
    _PROCESS_TRANSFORMER = TextTransformer()


def _embed_concept_text_batch(batch: list[tuple[str, str]]) -> list[tuple[str, list[float]]]:
    """
    Embed a single (concept_id, text) batch inside a worker process.
    """
    global _PROCESS_TRANSFORMER

    if _PROCESS_TRANSFORMER is None:
        _PROCESS_TRANSFORMER = TextTransformer()

    vectors = _PROCESS_TRANSFORMER.embed_strings(
        texts=[text for _, text in batch],
    )
    return [(concept_id, vectors[idx]) for idx, (concept_id, _) in enumerate(batch)]


def _prepare_embed_batch(batch: list[Concept]) -> list[tuple[str, str]]:
    """
    Convert a batch of concepts into (concept_id, canonical_text) pairs for embedding.
    :param batch: The batch of Concept instances to prepare.
    :return: A list of (concept_id, canonical_text) tuples.
    """
    return [(c.concept_id, c.canonical_text()) for c in batch]


async def _concept_batches_iter(concepts: list[Concept] | AsyncIterator[Concept],
                                batch_size: int,
                                total_concepts: int | None,
                                ) -> AsyncIterator[list[Concept]]:
    """
    Batch a list or async iterator of concepts into fixed-size chunks for embedding.
    :param concepts: A list or async iterator of Concept instances to batch.
    :param batch_size: The number of concepts per batch.
    :param total_concepts: Optional total number of concepts, used for progress tracking.
    :return: An async iterator of concept batches.
    """
    if isinstance(concepts, AsyncIterator):
        batch = []

        async for concept in aiter_progress(
            concepts,
            description='Embedding concepts',
            total=total_concepts,
        ):
            batch.append(concept)

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch
    elif isinstance(concepts, list):
        while concepts:
            batch = concepts[:batch_size]
            del concepts[:batch_size]
            yield batch
    else:
        raise TypeError('concepts must be a list or an AsyncIterator of Concept instances')


class ConceptTransformer(TextTransformer):
    """
    A class with convenient methods for transforming Concept instances into embeddings.
    """

    def _process_batch(self,
                       batch: list[Concept],
                       ) -> list[tuple[str, list[float]]]:
        """
        Embed a batch of concepts synchronously in the current process.
        :param batch: The batch of Concept instances to embed.
        :return: A list of (concept_id, embedding_vector) tuples.
        """
        vectors = self.embed_strings(
            texts=[c.canonical_text() for c in batch],
        )

        return [(c.concept_id, vectors[idx]) for idx, c in enumerate(batch)]

    @staticmethod
    async def _embed_parallel(concept_batches: AsyncIterator[list[Concept]],
                              worker_processes: int,
                              ) -> AsyncIterator[list[tuple[str, list[float]]]]:
        """
        Embed concept batches across a pool of worker processes, streaming results as they
        complete rather than waiting for the whole pool to finish.
        :param concept_batches: An async iterator of concept batches to embed.
        :param worker_processes: Number of worker processes to use.
        :return: An async iterator of chunks of (concept_id, embedding_vector) tuples.
        """
        loop = asyncio.get_running_loop()
        queue_size = max(worker_processes * 2, 1)

        with ProcessPoolExecutor(
            max_workers=worker_processes,
            initializer=_init_embed_worker,
        ) as executor:
            pending: set[asyncio.Future] = set()

            async for batch in concept_batches:
                pending.add(loop.run_in_executor(
                    executor,
                    _embed_concept_text_batch,
                    _prepare_embed_batch(batch),
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
                             ) -> AsyncIterator[list[tuple[str, list[float]]]]:
        """
        Embed concept texts using the configured SentenceTransformer model.
        :param concepts: A list or async iterator of Concept instances to embed
        :param batch_size: Number of concepts to process in each batch
        :param worker_processes: Number of worker processes for embedding. If None, uses config
        :param total_concepts: Optional total number of concepts, used for progress tracking
        :return: An iterator of chunks of tuples containing concept IDs and their embedding vectors
        """
        if batch_size is None:
            batch_size = CONFIG.embedding_batch_size
        if worker_processes is None:
            worker_processes = CONFIG.embedding_process_limit

        if batch_size < 1:
            raise ValueError('batch_size must be at least 1')
        if worker_processes < 1:
            raise ValueError('worker_processes must be at least 1')

        batches = _concept_batches_iter(concepts, batch_size, total_concepts)

        if worker_processes == 1:
            async for batch in batches:
                yield self._process_batch(batch)
            return

        async for embedded_batch in self._embed_parallel(batches, worker_processes):
            yield embedded_batch
