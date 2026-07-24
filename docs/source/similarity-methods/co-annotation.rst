===========================
Co-Annotation Vector Method
===========================

The co-annotation vector method measures similarity between two concepts of the same target vocabulary by how much they share in terms of *which* corpus concepts annotate them, rather than by their position in the target hierarchy. Each concept is represented by the set of corpus concepts annotated to it or to anything below it (its "annotation vector"), and similarity is derived from the statistical overlap between two such sets, combining Normalized Pointwise Mutual Information (NPMI) with the Jaccard index.

Annotation vectors
====================

As with the relevance method, the target vocabulary is first restricted to its ``is_a``/``part_of`` hierarchy. For a concept :math:`c` in the target vocabulary :math:`T`, let :math:`\text{Desc}(c)` be the set of :math:`c`'s ontological descendants (more specific concepts below it), including :math:`c` itself. The annotation vector of :math:`c` is the set of corpus concepts annotated to :math:`c` or to any of its descendants:

.. math::

    A(c) = \bigcup_{d \,\in\, \text{Desc}(c)} \text{annot}(d)

where :math:`\text{annot}(d)` is the set of corpus concepts directly annotated to :math:`d`. This reflects the assumption that anything annotated to a more specific concept also applies, transitively, to its more general ancestors.

Only concepts with a non-empty annotation vector (directly or through a descendant) are scored; concepts with no annotations anywhere below them are pruned from the target graph before pairwise scoring begins.

Normalized Pointwise Mutual Information
==========================================

Let :math:`N` be the total number of distinct corpus concepts that annotate anything in the target vocabulary — the size of the universe the co-occurrence statistics are drawn from. Treating each corpus concept as one observation, the pointwise mutual information between two target concepts :math:`c_1, c_2` is:

.. math::

    PMI(c_1, c_2) = \ln\left(\frac{N \cdot |A(c_1) \cap A(c_2)|}{|A(c_1)| \cdot |A(c_2)|}\right)

Raw PMI is unbounded, so it is normalized to the :math:`[-1, 1]` range:

.. math::

    NPMI(c_1, c_2) = \frac{PMI(c_1, c_2)}{-\ln\left(\dfrac{|A(c_1) \cap A(c_2)|}{N}\right)}

where :math:`NPMI = 1` means the two concepts always co-occur together, :math:`NPMI = 0` means they are statistically independent, and :math:`NPMI = -1` means they never co-occur. Since similarity scores in this system are expected in :math:`[0, 1]`, NPMI is rescaled:

.. math::

    \widehat{NPMI}(c_1, c_2) = \frac{1 + NPMI(c_1, c_2)}{2}

Two degenerate cases are handled directly rather than through the general formula: an empty intersection (:math:`A(c_1) \cap A(c_2) = \emptyset`) yields a score of :math:`0` outright, and an intersection equal to the entire universe (:math:`|A(c_1) \cap A(c_2)| = N`) yields :math:`\widehat{NPMI} = 1`, since the normalizing denominator is zero in that case.

Jaccard index
===============

NPMI alone measures how strongly two annotation sets co-occur relative to chance, but not how much of the two sets actually overlaps in absolute terms. This is supplied by the Jaccard index:

.. math::

    J(c_1, c_2) = \frac{|A(c_1) \cap A(c_2)|}{|A(c_1) \cup A(c_2)|}

Combined score
================

The final co-annotation similarity is the product of the two:

.. math::

    \text{sim}(c_1, c_2) = \widehat{NPMI}(c_1, c_2) \times J(c_1, c_2)

This favours pairs of concepts whose annotation vectors both overlap significantly more than chance would predict (high NPMI) *and* cover a large share of their combined annotations (high Jaccard), so that a strong statistical association driven by a handful of shared annotations out of otherwise large, mostly-disjoint sets is weighted down relative to a pair that is both statistically associated and substantially overlapping.

Implementation
================

The method is implemented in ``bioterms.similarity.co_annotation``. ``calculate_similarity`` performs the following steps:

1. The target graph is deep-copied and filtered down to ``IS_A``/``PART_OF`` edges only, as in the relevance method, then annotation counts are computed per node with the same ``count_annotation_for_graph`` helper used by the relevance method (used here only to identify which nodes have zero annotations anywhere below them).
2. Nodes with a zero annotation count are pruned from a copy of the target graph, leaving only concepts that have some annotation evidence, directly or through a descendant.
3. :math:`N` (``total_annotation_count``) is computed as the number of distinct corpus-prefixed nodes appearing anywhere in the annotation graph.
4. Every remaining pair of concepts is scored, distributed across a ``ProcessPoolExecutor`` sized by ``BTS_PROCESS_LIMIT`` in fixed-size batches, mirroring the relevance method's parallelisation strategy. Each worker resolves and caches a node's annotation vector :math:`A(c)` the first time it is needed, since the same node recurs across many pairs.
5. Pairs are yielded as ``(concept_from, concept_to, score)`` triples, filtered by the requested threshold, and persisted as similarity edges in the graph database.

Like the relevance method, this method requires a corpus vocabulary (``CORPUS_REQUIRED``) but not the corpus vocabulary's own internal hierarchy (``CORPUS_GRAPH_REQUIRED`` is false).

To calculate co-annotation scores, run:

.. code-block:: bash

    bioterms-cli similarity calculate --target <the-vocabulary-id> --corpus <corpus-vocabulary-id> --method co-annotation --threshold <similarity-threshold>

The default threshold is ``0.2``; see :doc:`../build-database` for guidance on choosing one, and the growth in pair count for large vocabularies. Once calculated, scores are retrieved through the similarity and translation endpoints described in :doc:`../usage/similarity`, where this method is identified as ``co-annotation``.
