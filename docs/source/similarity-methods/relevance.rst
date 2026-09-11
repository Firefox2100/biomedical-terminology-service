================
Relevance Method
================

The relevance method is an intrinsic Information Content (IC) based similarity measure between two concepts of the same target vocabulary, using their annotation counts against one corpus vocabulary as the evidence base. It follows the "Relevance" measure described by Schlicker et al. (2006), itself an extension of Resnik's (1995) and Lin's (1998) IC-based similarity measures.

Annotation counts
==================

Let :math:`T` be the target vocabulary, restricted to its ``is_a``/``part_of`` hierarchy (every other relationship type is dropped before the calculation). For a concept :math:`c \in T`, define its annotation count :math:`n(c)` recursively over that hierarchy, from leaves to root:

.. math::

    n(c) = d(c) + \sum_{k \,\in\, \text{children}(c)} n(k)

where :math:`d(c)` is the number of corpus concepts directly annotated to :math:`c`, and :math:`\text{children}(c)` are the concepts that have an ``is_a`` or ``part_of`` edge directly into :math:`c`. Because annotations accumulate upward through every asserted path, a concept's count reflects its own direct annotations plus everything annotated anywhere below it in the hierarchy.

Let :math:`n_{\max} = \max_{c \in T} n(c)` (in practice the count at or near the root of the hierarchy).

Information Content
=====================

The information content of a concept is:

.. math::

    IC(c) = -\ln\left(\frac{n(c)}{n_{\max}}\right)

A concept with no direct or inherited annotations has :math:`n(c) = 0`, for which :math:`IC(c)` is undefined; such concepts are excluded from the similarity calculation entirely, since they carry no evidence from the corpus.

Most Informative Common Ancestor
==================================

For two concepts :math:`c_1, c_2 \in T`, let :math:`\text{Anc}(c)` be the set of :math:`c`'s ontological ancestors, including :math:`c` itself, reached by following ``is_a``/``part_of`` edges upward. The Most Informative Common Ancestor (MICA) is the shared ancestor with the highest information content:

.. math::

    \text{MICA}(c_1, c_2) = \operatorname*{arg\,max}_{a \,\in\, \text{Anc}(c_1) \cap \text{Anc}(c_2),\; IC(a) \text{ defined}} IC(a)

If the two concepts have no common ancestor with a defined information content, no relevance score is produced for that pair.

Relevance score
================

The relevance between :math:`c_1` and :math:`c_2` combines Lin's similarity ratio with a specificity weight that discounts an overly generic MICA:

.. math::

    \text{sim}(c_1, c_2) = \underbrace{\frac{2 \cdot IC(\text{MICA}(c_1, c_2))}{IC(c_1) + IC(c_2)}}_{\text{Lin's similarity}} \times \underbrace{\left(1 - \frac{n(\text{MICA}(c_1, c_2))}{n_{\max}}\right)}_{\text{specificity weight}}

The first factor is maximal (:math:`1.0`) when the MICA is one of the two concepts themselves, i.e. when one concept is an ancestor of the other. The second factor penalises common ancestors that are annotated broadly (close to :math:`n_{\max}`), so that two concepts whose only shared ancestor is a generic, high-level term score lower than two concepts that share a narrow, specific one.

Implementation
================

The method is implemented in ``bioterms.similarity.relevance``. ``calculate_similarity`` performs the following steps:

1. The target graph is deep-copied and filtered down to ``IS_A``/``PART_OF`` edges only (``filter_edges_by_relationship``), then converted to a plain ``DiGraph``.
2. ``count_annotation_for_graph`` (``bioterms.similarity.utils``) computes :math:`n(c)` for every node in a single pass over the graph's topological order, so that every child is processed before its parents.
3. :math:`n_{\max}` is taken as the maximum annotation count across all nodes, and information content is computed for every node with a non-zero annotation count.
4. Every pair of concepts with a defined information content is then scored. This is the combinatorial step: with :math:`k` concepts carrying a defined IC, there are :math:`\binom{k}{2}` pairs to evaluate, so the calculation is distributed across a ``ProcessPoolExecutor`` (sized by ``BTS_PROCESS_LIMIT``), with pairs grouped into fixed-size batches. Each worker process receives its own copy of the populated graph once at startup and caches each node's resolved ancestor set (:math:`\text{Anc}(c)`) for the lifetime of the worker, since the same node recurs across many pairs.
5. Pairs whose MICA cannot be resolved are dropped; the remainder are yielded as ``(concept_from, concept_to, score)`` triples, filtered by the requested threshold, and persisted as similarity edges in the graph database.

This method requires a corpus vocabulary (``CORPUS_REQUIRED``) to supply the annotation counts, but not the corpus vocabulary's own internal hierarchy (``CORPUS_GRAPH_REQUIRED`` is false): only which target concepts are annotated to which corpus concepts matters, not how the corpus concepts relate to each other.

To calculate relevance scores, run:

.. code-block:: bash

    bioterms-cli similarity calculate --target <the-vocabulary-id> --corpus <corpus-vocabulary-id> --method relevance --threshold <similarity-threshold>

The default threshold is ``0.2``; see :doc:`../build-database` for guidance on choosing one, and the growth in pair count for large vocabularies. Once calculated, scores are retrieved through the similarity and translation endpoints described in :doc:`../usage/similarity`, where this method is identified as ``relevance``.
