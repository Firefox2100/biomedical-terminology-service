"""
Numba `@njit` kernels are compiled, so coverage cannot trace them and a typing error only
surfaces when a kernel is compiled. Each similarity method is therefore run twice over the
same non-trivial ontology: once with the compiled kernels and once with every kernel swapped
for its pure-Python `.py_func` source. The results must agree, which both checks the compiled
code and puts the kernel bodies under coverage.
"""
import random

import pytest

from bioterms.etc.enums import ConceptPrefix
from bioterms.similarity import co_annotation, relevance, relevance_weight
from bioterms.similarity.context import AnnotationIndex, OntologyIndex, SimilarityContext


def _ontology(prefix, depth, width, rng):
    """A layered DAG where every node below the root has one or two parents in the layer above."""
    layers = [[f'{prefix}:root']]
    edges = []
    for level in range(1, depth):
        layer = [f'{prefix}:{level}.{i}' for i in range(width)]
        for node in layer:
            for parent in rng.sample(layers[-1], k=min(len(layers[-1]), rng.choice((1, 2)))):
                edges.append((node, parent, 'is_a', None))
        layers.append(layer)
    return [node for layer in layers for node in layer], edges


def _context(threshold):
    rng = random.Random(3)
    target_nodes, target_edges = _ontology('HP', depth=4, width=6, rng=rng)
    corpus_nodes, corpus_edges = _ontology('OMIM', depth=3, width=5, rng=rng)
    leaves = [n for n in target_nodes if n.count('.') and n.startswith('HP:3')]
    corpus_leaves = [n for n in corpus_nodes if n.startswith('OMIM:2')]
    pairs = sorted({(rng.choice(leaves), rng.choice(corpus_leaves)) for _ in range(40)})
    target = OntologyIndex.build(target_nodes, target_edges)
    corpus = OntologyIndex.build(corpus_nodes, corpus_edges)
    return SimilarityContext(
        ConceptPrefix.HPO, target, ConceptPrefix.OMIM, corpus,
        AnnotationIndex.build(target, corpus, pairs), threshold=threshold,
    )


async def _scores(module):
    return sorted([row async for row in module.calculate_similarity(_context(threshold=0.01))])


def _use_python_kernels(monkeypatch, module):
    swapped = []
    for name, value in vars(module).copy().items():
        if hasattr(value, 'py_func'):
            monkeypatch.setattr(module, name, value.py_func)
            swapped.append(name)
    return swapped


@pytest.mark.asyncio
@pytest.mark.parametrize('module', [co_annotation, relevance, relevance_weight])
async def test_compiled_and_python_kernels_agree(monkeypatch, module):
    compiled = await _scores(module)

    assert _use_python_kernels(monkeypatch, module)
    interpreted = await _scores(module)

    assert compiled, 'the fixture ontology should produce similarity scores'
    assert [row[:2] for row in interpreted] == [row[:2] for row in compiled]
    assert [row[2] for row in interpreted] == pytest.approx([row[2] for row in compiled], rel=1e-9)
    assert all(0.01 <= score for _, _, score in compiled)


@pytest.mark.asyncio
@pytest.mark.parametrize('module', [co_annotation, relevance, relevance_weight])
async def test_higher_threshold_returns_a_subset(module):
    low = {row[:2]: row[2] for row in await _scores(module)}
    high = [row async for row in module.calculate_similarity(_context(threshold=0.3))]

    assert {row[:2] for row in high} <= set(low)
    assert all(score >= 0.3 for _, _, score in high)


@pytest.mark.asyncio
@pytest.mark.parametrize('module', [co_annotation, relevance, relevance_weight])
async def test_methods_need_annotations(module):
    context = _context(threshold=0.01)
    context.annotations = None

    assert [row async for row in module.calculate_similarity(context)] == []
