import pytest

from bioterms.etc.enums import ConceptPrefix
from bioterms.similarity.context import AnnotationIndex, OntologyIndex, SimilarityContext
from bioterms.similarity import co_annotation, relevance, relevance_weight


def _context():
    target = OntologyIndex.build(
        ['a', 'b', 'root'],
        [('a', 'root', 'is_a', None), ('b', 'root', 'is_a', None)],
    )
    corpus = OntologyIndex.build(['x', 'y'], [])
    annotations = AnnotationIndex.build(target, corpus, [('a', 'x'), ('b', 'y')])
    return SimilarityContext(
        ConceptPrefix.HPO, target, ConceptPrefix.OMIM, corpus, annotations,
    )


def test_context_builds_bidirectional_csr_and_annotations():
    context = _context()
    root = context.target.node_to_index['root']
    assert {
        context.target.node_ids[int(node)] for node in context.target.predecessors(root)
    } == {'a', 'b'}
    assert list(context.annotations.target_to_corpus[context.target.node_to_index['a']]) == [0]
    assert list(context.annotations.corpus_to_target[0]) == [context.target.node_to_index['a']]


@pytest.mark.asyncio
@pytest.mark.parametrize('module', [co_annotation, relevance, relevance_weight])
async def test_similarity_modules_share_compact_context_contract(module):
    results = [row async for row in module.calculate_similarity(_context())]
    assert all(len(row) == 3 for row in results)
