==================
Similarity Methods
==================

This section documents the theoretical background of the similarity methods offered by ``bioterms-cli similarity calculate`` (see :doc:`../build-database`), how each score is derived mathematically, and how the calculation is actually carried out against the concept graph. For a description of the API surface that serves these scores once calculated, see :doc:`../usage/similarity` instead.

Every method calculates a pairwise score between two concepts belonging to the same *target* vocabulary, using the annotation relationships between that target vocabulary and one *corpus* vocabulary. A score is only ever meaningful between two concepts of the target vocabulary; the corpus is used purely as the evidence base from which each method derives its statistics, and does not itself need to be similarity-scored.

Two of the currently supported methods are documented here:

.. toctree::
    :maxdepth: 1

    relevance
    co-annotation

A third method, the weighed relevance method, extends the relevance method with a corpus-side weighting scheme and is not yet documented here.
