import pytest

from bioterms.etc.enums import ConceptPrefix, ConceptStatus, EmbeddingKind
from bioterms.model.concept import Concept
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.search import hybrid as hybrid_module
from bioterms.search.hybrid import hybrid_search


def make_concept(concept_id, label):
    return Concept(conceptTypes=[], prefix=ConceptPrefix.HPO, conceptId=concept_id, label=label,
                   status=ConceptStatus.ACTIVE)


class FakeCacheHit:
    """A cache that already has the vocabulary status cached -- the common case between
    dataset changes, since embed/restore call `cache.rotate_dataset_version()`."""
    def __init__(self, status):
        self._status = status

    async def get_vocabulary_status(self, prefix):
        return self._status


class NoHeavyTripsDocDb:
    """Stands in for the document database: `count_terms` is the heavy call a cache hit
    should make unnecessary, so calling it fails the test. The actual search calls
    (`lexical_search`, `get_terms_by_ids`) still need to work normally."""
    def __init__(self, concepts, lexical_results):
        self.concepts = {c.concept_id: c for c in concepts}
        self.lexical_results = lexical_results

    async def count_terms(self, prefix):
        raise AssertionError('count_terms must not run when the vocabulary status is cached')

    async def lexical_search(self, prefix, query, limit):
        return self.lexical_results[:limit]

    async def get_terms_by_ids(self, prefix, concept_ids, model_class=Concept):
        return [self.concepts[c] for c in concept_ids if c in self.concepts]


class NoHeavyTripsVectorDb:
    """Stands in for the vector database: `count_vectors` is the heavy call a cache hit
    should make unnecessary."""
    def __init__(self, alias_hits):
        self.alias_hits = alias_hits

    async def count_vectors(self, prefix):
        raise AssertionError('count_vectors must not run when the vocabulary status is cached')

    async def search_items(self, query_vector, prefix, kind, limit=10):
        return self.alias_hits[:limit] if kind == EmbeddingKind.ALIAS else []


class FakeTextTransformer:
    def __init__(self, *args, **kwargs):
        pass

    def embed_strings(self, texts):
        return [[0.1, 0.2, 0.3] for _ in texts]


class FakeGraphDb:
    """Never resolved on a cache hit -- included only to prove `get_active_graph_db` is not
    reached for either, matching how `get_vocabulary_status` short-circuits before touching
    doc_db/graph_db/vector_db at all when the cache already has an answer."""


@pytest.mark.asyncio
async def test_hybrid_search_uses_cached_vocabulary_status_instead_of_live_db_counts(monkeypatch):
    monkeypatch.setattr(hybrid_module, 'TextTransformer', FakeTextTransformer)

    concepts = [make_concept('HP:1', 'Seizure disorder')]
    doc_db = NoHeavyTripsDocDb(concepts, lexical_results=[('HP:1', 1.0)])
    vector_db = NoHeavyTripsVectorDb(alias_hits=[('HP:1', 'Seizure disorder', 0.9)])
    cache = FakeCacheHit(VocabularyStatus(
        prefix=ConceptPrefix.HPO, name='HPO', loaded=True, conceptCount=1, vectorCount=5,
    ))

    results = [c async for c in hybrid_search(
        query='seizure', prefix=ConceptPrefix.HPO, doc_db=doc_db, vector_db=vector_db,
        limit=10, cache=cache,
    )]

    assert [c.concept_id for c in results] == ['HP:1']
