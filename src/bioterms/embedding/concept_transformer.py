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


class ConceptTransformer(TextTransformer):
    """
    A class with convenient methods for transforming Concept instances into embeddings.
    """

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

        def prepare_batch(batch: list[Concept]) -> list[tuple[str, str]]:
            return [(c.concept_id, c.canonical_text()) for c in batch]

        def process_batch(batch: list[Concept]) -> list[tuple[str, list[float]]]:
            vectors = self.embed_strings(
                texts=[c.canonical_text() for c in batch],
            )

            return [(c.concept_id, vectors[idx]) for idx, c in enumerate(batch)]

        async def embed_parallel(concept_batches: AsyncIterator[list[Concept]]) -> AsyncIterator[list[tuple[str, list[float]]]]:
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
                        prepare_batch(batch),
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

        async def concept_batches_iter() -> AsyncIterator[list[Concept]]:
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

        if worker_processes == 1:
            async for batch in concept_batches_iter():
                yield process_batch(batch)
            return

        async for embedded_batch in embed_parallel(concept_batches_iter()):
            yield embedded_batch
