========================
Weighed Relevance Method
========================

The weighed relevance method extends the :doc:`relevance` with a correction for a specific bias that arises when the corpus vocabulary is itself another curated ontology rather than a real-world observation set (e.g. patient records, literature co-occurrence, or another frequency-based corpus). It is otherwise structurally identical to the relevance method: it uses the same Most Informative Common Ancestor (MICA) combining formula, and differs only in how each concept's annotation evidence, and therefore its information content, is computed.

Motivation
============

The relevance method's information content is derived from a concept's annotation count: how many corpus concepts are annotated to it, directly or through its descendants. This treats every direct annotation as equally informative. That assumption holds reasonably well when the corpus reflects real-world observation frequency, because a concept observed many times in the real world genuinely is common, and one observed rarely genuinely is rare.

It breaks down when the corpus is itself another ontology. An ontology's annotation structure reflects how its curators chose to model and cross-reference concepts, not how often those concepts occur in reality. Two target concepts may each be annotated to exactly one corpus concept, and be treated as equally rare by the relevance method's flat counting, even though one of those corpus concepts corresponds to something common in the real world (e.g. hypertension) and the other to something genuinely rare. A flat annotation count cannot distinguish the two, because both corpus concepts simply appear once in the annotation graph.

The weighed relevance method compensates for this by weighting each direct annotation according to an estimate of how specific, and therefore how informative, the annotating corpus concept is. That estimate is not taken from an external frequency source; it is the corpus concept's own information content, computed by running the same kind of annotation-count-and-propagate calculation on the corpus vocabulary, using the target vocabulary as the corpus role, in reverse. Because the target's information content depends on weights derived from the corpus's information content, and the corpus's information content depends symmetrically on the target's, the two are resolved together as a fixed point, iterating until both stabilize.

Weighted annotation sums
===========================

As with the relevance method, both the target vocabulary :math:`T` and, unlike the relevance method, the corpus vocabulary :math:`C` are restricted to their ``is_a``/``part_of`` hierarchies, since both are now propagated through in the same way.

Instead of counting each direct annotation edge with weight :math:`1`, as the relevance method does, each direct annotation from a target concept :math:`c` to a corpus concept :math:`u` is weighted by an estimate of :math:`u`'s own specificity, :math:`w(u)`. The weighted annotation sum of a target concept, at iteration :math:`i`, is defined by the same child-to-parent accumulation as the plain annotation count:

.. math::

    \sigma_T^{(i)}(c) = \sum_{u \,\in\, \text{annot}(c)} w^{(i)}(u) \;+\; \sum_{k \,\in\, \text{children}(c)} \sigma_T^{(i)}(k)

where :math:`\text{annot}(c)` is the set of corpus concepts directly annotated to :math:`c`, and the weight of a corpus concept :math:`u` is derived from its own information content on the corpus side:

.. math::

    w^{(i)}(u) = \exp\!\big(\alpha \cdot IC_C^{(i-1)}(u)\big)

with a fixed tuning exponent :math:`\alpha = 0.5`. On the very first pass, no corpus information content exists yet for any concept, so every direct annotation instead uses a flat bootstrap weight of :math:`0.5`, purely to obtain an initial target-side estimate to seed the process.

The target's information content at iteration :math:`i` is then computed exactly as in the plain relevance method, but from the weighted sum instead of the raw count:

.. math::

    IC_T^{(i)}(c) = -\ln\left(\frac{\sigma_T^{(i)}(c)}{\displaystyle\max_{c' \in T} \sigma_T^{(i)}(c')}\right)

The corpus side is then updated the same way, in the same iteration, using the target's information content that was just computed:

.. math::

    \sigma_C^{(i)}(u) = \sum_{c \,\in\, \text{annot}(u)} \exp\!\big(\alpha \cdot IC_T^{(i)}(c)\big) \;+\; \sum_{k \,\in\, \text{children}(u)} \sigma_C^{(i)}(k), \qquad
    IC_C^{(i)}(u) = -\ln\left(\frac{\sigma_C^{(i)}(u)}{\displaystyle\max_{u' \in C} \sigma_C^{(i)}(u')}\right)

Because the corpus update within an iteration uses the target's information content from that same iteration, while the target update uses the corpus's information content left over from the previous iteration, each iteration updates target and corpus in a staggered order rather than simultaneously from stale values on both sides. This is intentional: it lets the corpus-side pass benefit from the most current target-side estimate available, rather than waiting an extra iteration for it to propagate.

Convergence
=============

Let :math:`\Delta^{(i)}` be the largest absolute change in information content, across every concept on both sides, between iteration :math:`i` and iteration :math:`i-1`:

.. math::

    \Delta^{(i)} = \max\left(\ \max_{c \in T} \left| IC_T^{(i)}(c) - IC_T^{(i-1)}(c) \right|,\ \ \max_{u \in C} \left| IC_C^{(i)}(u) - IC_C^{(i-1)}(u) \right|\ \right)

The process repeats, always running at least two full passes, until :math:`\Delta^{(i)} < \varepsilon` with :math:`\varepsilon = 10^{-3}`. There is no upper bound on the number of iterations in the current implementation; it runs until this convergence criterion is met.

Convergence of this mutual update has not been proven analytically; it is not a standard fixed-point form with an established convergence guarantee, since each side's information content feeds into the other's weighting through the nonlinear :math:`\exp(\alpha \cdot IC)` term. In practice, on the ontology pairs this service has been tested against, the process has consistently converged within a small number of iterations.

Relevance score
==================

Once the target side has converged, concept pairs are scored with the same MICA-based formula used by the plain relevance method (see :doc:`relevance`), substituting the converged weighted sum and information content:

.. math::

    \text{sim}(c_1, c_2) = \frac{2 \cdot IC_T(\text{MICA}(c_1, c_2))}{IC_T(c_1) + IC_T(c_2)} \times \left(1 - \frac{\sigma_T(\text{MICA}(c_1, c_2))}{\displaystyle\max_{c} \sigma_T(c)}\right)

Relationship to the standard relevance method
=================================================

The per-annotation weight can be rewritten algebraically as an inverse-frequency term raised to the tuning exponent:

.. math::

    w(u) = \exp\!\big(\alpha \cdot IC_C(u)\big) = \left(\frac{\displaystyle\max_{u'} \sigma_C(u')}{\sigma_C(u)}\right)^{\alpha}

This is the same shape as inverse document frequency (IDF) weighting: a corpus concept that is itself rare, in the sense of contributing a small weighted sum on the corpus side, is treated as more informative and given a larger weight when it annotates a target concept. At :math:`\alpha = 1`, the weight is exactly the corpus concept's inverse relative frequency; at :math:`\alpha = 0`, every weight collapses to :math:`1` and the method reduces exactly to the plain relevance method's flat annotation counting. The implementation fixes :math:`\alpha = 0.5`, the square root of the full inverse-frequency ratio, which moderates how aggressively rare corpus concepts are up-weighted relative to common ones. Neither :math:`\alpha` nor the convergence threshold :math:`\varepsilon` is currently exposed as a configurable parameter.

Implementation
================

The method is implemented in ``bioterms.similarity.relevance_weight``, and largely mirrors ``bioterms.similarity.relevance`` in structure, with the annotation-counting stage replaced by an iterative loop:

1. Both the target and corpus graphs are deep-copied and filtered to ``IS_A``/``PART_OF`` edges only, since both are now propagated through the same child-to-parent accumulation used by the relevance method.
2. The loop body, per iteration: ``_sum_annotation_for_graph`` computes weighted annotation sums for the target graph in topological order (``_direct_annotation_sum`` for the weighted direct term, ``_child_annotation_sum`` for the accumulated term), then ``_calculate_ic`` derives the target's information content and reports the largest change from the previous iteration. The same two steps then run with the target and corpus graphs' roles swapped, updating the corpus side using the target's just-computed information content.
3. After ``iteration > 0`` and both sides' maximum information content change fall under the convergence threshold, the loop exits.
4. Concept pairs are then scored exactly as in the relevance method: every pair of concepts with a defined information content is distributed across a ``ProcessPoolExecutor`` (sized by ``BTS_PROCESS_LIMIT``) in fixed-size batches, with each worker caching resolved ancestor sets, and the MICA-based formula above applied per pair.

Because it needs the corpus vocabulary's own hierarchy in addition to its annotation edges, this method requires both a corpus vocabulary and that corpus's graph (``CORPUS_REQUIRED`` and ``CORPUS_GRAPH_REQUIRED`` are both true), unlike the plain relevance and co-annotation methods, which only need the annotation edges.

To calculate weighed relevance scores, run:

.. code-block:: bash

    bioterms-cli similarity calculate --target <the-vocabulary-id> --corpus <corpus-vocabulary-id> --method weighed-relevance --threshold <similarity-threshold>

The default threshold is ``0.2``; see :doc:`../build-database` for guidance on choosing one, and the growth in pair count for large vocabularies, which applies here as well since the pairwise scoring step is unchanged from the relevance method. Because of the added iterative pass over both the target and corpus graphs, calculating this method is more expensive than the plain relevance method even before the pairwise scoring stage is reached. Once calculated, scores are retrieved through the similarity and translation endpoints described in :doc:`../usage/similarity`, where this method is identified as ``weighed-relevance``.
