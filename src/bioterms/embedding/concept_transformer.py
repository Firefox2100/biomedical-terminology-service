from collections.abc import AsyncIterator

from bioterms.etc.utils import aiter_progress
from bioterms.model.concept import Concept
from .text_transformer import TextTransformer


class ConceptTransformer(TextTransformer):
    """
    A class with convenient methods for transforming Concept instances into embeddings.
    """

    async def embed_concepts(self,
                             concepts: list[Concept] | AsyncIterator[Concept],
                             batch_size: int = 32,
                             total_concepts: int = None,
                             ) -> AsyncIterator[list[tuple[str, list[float]]]]:
        """
        Embed concept texts using the configured SentenceTransformer model.
        :param concepts: A list or async iterator of Concept instances to embed
        :param batch_size: Number of concepts to process in each batch
        :param total_concepts: Optional total number of concepts, used for progress tracking
        :return: An iterator of chunks of tuples containing concept IDs and their embedding vectors
        """
        def process_batch(b: list[Concept]) -> list[tuple[str, list[float]]]:
            vectors = self.embed_strings(
                texts=[c.canonical_text() for c in b],
            )

            return [(c.concept_id, vectors[idx]) for idx, c in enumerate(b)]

        if isinstance(concepts, AsyncIterator):
            batch = []

            async for concept in aiter_progress(
                concepts,
                description='Embedding concepts',
                total=total_concepts,
            ):
                batch.append(concept)

                if len(batch) >= batch_size:
                    yield process_batch(batch)
                    batch = []

            if batch:
                yield process_batch(batch)
        elif isinstance(concepts, list):
            while concepts:
                batch = concepts[:batch_size]
                del concepts[:batch_size]

                yield process_batch(batch)
        else:
            raise TypeError('concepts must be a list or an AsyncIterator of Concept instances')
