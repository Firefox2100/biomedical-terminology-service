import pytest

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.etc.errors import SimilarityDataNotAvailable, SimilarityNotSupported
from bioterms.model.similarity_status import SimilarityCount, SimilarityStatus
from bioterms.model.vocabulary_status import VocabularyStatus
import bioterms.similarity as similarity_module


def make_vocab_status(prefix, similarity_methods):
    return VocabularyStatus(
        prefix=prefix, name=prefix.value.upper(), loaded=True, conceptCount=5,
        similarityMethods=similarity_methods,
    )


@pytest.mark.asyncio
async def test_raises_similarity_not_supported_when_vocabulary_has_no_methods(monkeypatch):
    async def fake_vocab_status(prefix, cache=None, doc_db=None, graph_db=None, **kwargs):
        return make_vocab_status(prefix, similarity_methods=[])

    monkeypatch.setattr(similarity_module, 'get_vocabulary_status', fake_vocab_status)

    with pytest.raises(SimilarityNotSupported):
        await similarity_module.ensure_similarity_available(ConceptPrefix.HGNC)


@pytest.mark.asyncio
async def test_raises_similarity_data_not_available_when_no_relationships_computed(monkeypatch):
    async def fake_vocab_status(prefix, cache=None, doc_db=None, graph_db=None, **kwargs):
        return make_vocab_status(prefix, similarity_methods=[SimilarityMethod.RELEVANCE])

    async def fake_similarity_status(prefix, cache=None, doc_db=None, graph_db=None, **kwargs):
        return SimilarityStatus(prefix=prefix, similarityCounts=[
            SimilarityCount(method=SimilarityMethod.RELEVANCE, corpus=None, count=0),
        ])

    monkeypatch.setattr(similarity_module, 'get_vocabulary_status', fake_vocab_status)
    monkeypatch.setattr(similarity_module, 'get_similarity_status', fake_similarity_status)

    with pytest.raises(SimilarityDataNotAvailable):
        await similarity_module.ensure_similarity_available(ConceptPrefix.HPO)


@pytest.mark.asyncio
async def test_does_not_raise_when_similarity_data_is_available(monkeypatch):
    async def fake_vocab_status(prefix, cache=None, doc_db=None, graph_db=None, **kwargs):
        return make_vocab_status(prefix, similarity_methods=[SimilarityMethod.RELEVANCE])

    async def fake_similarity_status(prefix, cache=None, doc_db=None, graph_db=None, **kwargs):
        return SimilarityStatus(prefix=prefix, similarityCounts=[
            SimilarityCount(method=SimilarityMethod.RELEVANCE, corpus=None, count=42),
        ])

    monkeypatch.setattr(similarity_module, 'get_vocabulary_status', fake_vocab_status)
    monkeypatch.setattr(similarity_module, 'get_similarity_status', fake_similarity_status)

    await similarity_module.ensure_similarity_available(ConceptPrefix.HPO)


class FakeCacheMiss:
    """A cache that never has a cached status, so `get_similarity_status` must build (and
    return) one fresh -- catches the status being computed but never returned."""
    async def get_similarity_status(self, prefix):
        return None

    async def save_similarity_status(self, status):
        pass


class FakeGraphDbForSimilarityStatus:
    async def count_similarity_relationships(self, prefix_from, prefix_to, configurations):
        return [
            (method, corpus, 7)
            for method, corpus in configurations
        ]


@pytest.mark.asyncio
async def test_get_similarity_status_returns_freshly_built_status_on_cache_miss(monkeypatch):
    async def fake_vocab_status(prefix, cache=None, doc_db=None, graph_db=None, **kwargs):
        return make_vocab_status(prefix, similarity_methods=[SimilarityMethod.RELEVANCE])

    monkeypatch.setattr(similarity_module, 'get_vocabulary_status', fake_vocab_status)

    status = await similarity_module.get_similarity_status(
        ConceptPrefix.HPO,
        cache=FakeCacheMiss(),
        doc_db=object(),
        graph_db=FakeGraphDbForSimilarityStatus(),
    )

    assert status is not None
    assert isinstance(status, SimilarityStatus)
    assert status.prefix == ConceptPrefix.HPO
