import asyncio
import time

import pytest

from bioterms.database.vector_db.vector_db import VectorDatabase, EmbeddingItemVector
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, EmbeddingKind
from bioterms.model.concept import Concept, EmbeddingItem


def make_concept(concept_id, label):
    return Concept(
        conceptTypes=[],
        prefix=ConceptPrefix.HPO,
        conceptId=concept_id,
        label=label,
        status=ConceptStatus.ACTIVE,
    )


class FakeVectorDatabase(VectorDatabase):
    """
    Minimal in-memory VectorDatabase for exercising `insert_concepts`'s resumability logic
    without a real backend.
    """

    def __init__(self, already_embedded: set[str] = frozenset()):
        self.already_embedded = set(already_embedded)
        self.loaded_items: list[EmbeddingItemVector] = []

    async def close(self):
        pass

    async def load_embedding_items(self, prefix, items, total_items=None) -> int:
        written = 0
        async for item in items:
            self.loaded_items.append(item)
            written += 1
        return written

    async def get_embedded_concept_ids(self, prefix) -> set[str]:
        return self.already_embedded

    async def count_vectors(self, prefix) -> int:
        return len(self.loaded_items)

    def search_items_iter(self, query_vector, prefix, kind, limit=10):
        async def _empty():
            return
            yield  # pragma: no cover

        return _empty()

    async def delete_vectors_for_prefix(self, prefix) -> None:
        self.loaded_items = []


class FakeTextTransformer:
    def __init__(self, *args, **kwargs):
        pass

    @property
    def dimension(self):
        return 3

    def embed_strings(self, texts):
        return [[0.1, 0.2, 0.3] for _ in texts]


@pytest.mark.asyncio
async def test_insert_concepts_skips_already_embedded_concepts_by_default(monkeypatch):
    monkeypatch.setattr(
        'bioterms.embedding.text_transformer.TextTransformer.embed_strings',
        FakeTextTransformer.embed_strings,
    )

    vector_db = FakeVectorDatabase(already_embedded={'HP:0000001'})

    concepts = [make_concept('HP:0000001', 'Already done'), make_concept('HP:0000002', 'Still pending')]

    written = await vector_db.insert_concepts(concepts=concepts, prefix=ConceptPrefix.HPO)

    assert written == 1
    assert [item.concept_id for item in vector_db.loaded_items] == ['HP:0000002']


@pytest.mark.asyncio
async def test_insert_concepts_reembeds_everything_when_resume_is_false(monkeypatch):
    monkeypatch.setattr(
        'bioterms.embedding.text_transformer.TextTransformer.embed_strings',
        FakeTextTransformer.embed_strings,
    )

    vector_db = FakeVectorDatabase(already_embedded={'HP:0000001'})

    concepts = [make_concept('HP:0000001', 'Already done'), make_concept('HP:0000002', 'Still pending')]

    written = await vector_db.insert_concepts(concepts=concepts, prefix=ConceptPrefix.HPO, resume=False)

    assert written == 2
    assert {item.concept_id for item in vector_db.loaded_items} == {'HP:0000001', 'HP:0000002'}


class DelayedWriteVectorDatabase(FakeVectorDatabase):
    """
    Like FakeVectorDatabase, but each `load_embedding_items` write pauses for `write_delay`
    -- standing in for a real driver's network round-trip, to test that it overlaps with
    concurrent embedding work rather than serialising with it.
    """

    def __init__(self, write_delay: float):
        super().__init__()
        self.write_delay = write_delay

    async def load_embedding_items(self, prefix, items, total_items=None) -> int:
        written = 0
        async for item in items:
            await asyncio.sleep(self.write_delay)
            self.loaded_items.append(item)
            written += 1
        return written


async def _delayed_batches(batches: list[list[str]], delay: float):
    """
    A fake ConceptTransformer.embed_concepts()-shaped async generator: yields one
    (EmbeddingItem, vector) batch per entry in `batches`, pausing `delay` before each -- standing
    in for a real GPU encode call.
    """
    for batch in batches:
        await asyncio.sleep(delay)
        yield [
            (
                EmbeddingItem(item_id=f'{concept_id}:alias:0', concept_id=concept_id,
                              kind=EmbeddingKind.ALIAS, text=concept_id),
                [0.1, 0.2, 0.3],
            )
            for concept_id in batch
        ]


@pytest.mark.asyncio
async def test_insert_concepts_overlaps_embedding_with_writing(monkeypatch):
    embed_delay = 0.05
    write_delay = 0.05
    batches = [[f'C{i}'] for i in range(6)]

    class FakeConceptTransformer:
        def __init__(self, *args, **kwargs):
            pass

        def embed_concepts(self, concepts, total_concepts=None):
            return _delayed_batches(batches, embed_delay)

    monkeypatch.setattr('bioterms.embedding.ConceptTransformer', FakeConceptTransformer)

    vector_db = DelayedWriteVectorDatabase(write_delay=write_delay)
    concepts = [make_concept(f'C{i}', f'Concept {i}') for i in range(6)]

    serial_estimate = len(batches) * (embed_delay + write_delay)

    start = time.perf_counter()
    written = await vector_db.insert_concepts(concepts=concepts, prefix=ConceptPrefix.HPO)
    elapsed = time.perf_counter() - start

    assert written == 6
    # A fully-serial pipeline (embed batch, then write it, then embed the next, ...) would take
    # ~= serial_estimate; overlap should bring this well under that, close to
    # embed_delay + len(batches) * write_delay once the pipeline is full.
    assert elapsed < serial_estimate * 0.75, (
        f'expected embedding and writing to overlap (elapsed={elapsed:.3f}s, '
        f'serial_estimate={serial_estimate:.3f}s)'
    )


@pytest.mark.asyncio
async def test_insert_concepts_propagates_writer_exception_and_stops_embedding(monkeypatch):
    embed_calls = []

    async def tracking_batches(delay):
        for i in range(10):
            embed_calls.append(i)
            await asyncio.sleep(delay)
            yield [(
                EmbeddingItem(item_id=f'C{i}:alias:0', concept_id=f'C{i}',
                              kind=EmbeddingKind.ALIAS, text=f'C{i}'),
                [0.1, 0.2, 0.3],
            )]

    class FakeConceptTransformer:
        def __init__(self, *args, **kwargs):
            pass

        def embed_concepts(self, concepts, total_concepts=None):
            return tracking_batches(0.01)

    monkeypatch.setattr('bioterms.embedding.ConceptTransformer', FakeConceptTransformer)

    class FailingVectorDatabase(FakeVectorDatabase):
        async def load_embedding_items(self, prefix, items, total_items=None) -> int:
            async for _item in items:
                raise RuntimeError('write failed')
            return 0

    vector_db = FailingVectorDatabase()
    concepts = [make_concept(f'C{i}', f'Concept {i}') for i in range(10)]

    with pytest.raises(RuntimeError, match='write failed'):
        await vector_db.insert_concepts(concepts=concepts, prefix=ConceptPrefix.HPO)

    # The embedder should have been stopped rather than run to completion in the background.
    await asyncio.sleep(0.05)
    assert len(embed_calls) < 10


@pytest.mark.asyncio
async def test_insert_concepts_propagates_embedding_exception(monkeypatch):
    async def failing_batches():
        yield [(
            EmbeddingItem(item_id='C0:alias:0', concept_id='C0', kind=EmbeddingKind.ALIAS, text='C0'),
            [0.1, 0.2, 0.3],
        )]
        raise RuntimeError('embedding failed')

    class FakeConceptTransformer:
        def __init__(self, *args, **kwargs):
            pass

        def embed_concepts(self, concepts, total_concepts=None):
            return failing_batches()

    monkeypatch.setattr('bioterms.embedding.ConceptTransformer', FakeConceptTransformer)

    vector_db = FakeVectorDatabase()
    concepts = [make_concept('C0', 'Concept 0')]

    with pytest.raises(RuntimeError, match='embedding failed'):
        await vector_db.insert_concepts(concepts=concepts, prefix=ConceptPrefix.HPO)
