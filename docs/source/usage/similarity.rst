==========
Similarity
==========

Similarity retrieves concepts that are semantically related to a given concept, based on the similarity scores computed with ``bioterms-cli similarity calculate`` (see :doc:`../build-database`). Unlike :doc:`mapping`, which follows explicit cross-vocabulary annotations, similarity reflects a computed score and is only available for concepts that have been through the similarity calculation step.

Retrieving similar terms
=========================

There are two versions of the endpoint, under ``/api/vocabularies/{prefix}/similarity/...``.

v1 (Cafe Variome V3)
---------------------

``POST /api/vocabularies/{prefix}/similarity/v1``

Request body:

.. code-block:: json

    {
      "termIds": ["0001250"],
      "threshold": 1.0
    }

``threshold`` (default ``1.0``, range ``0.0``-``1.0``) is the minimum similarity score to include in the response.

Query parameters:

* ``result_threshold`` (default ``0``): maximum number of similar terms to return per input term, ``0`` for no limit.

This version always restricts results to concepts in the *same* vocabulary as the input term, combining every available similarity method and corpus without distinction; use v2 for finer control. The server collects the full result set before responding:

.. code-block:: json

    [
      {
        "termId": "0001250",
        "similarIds": ["0001251"],
        "similarityThreshold": 1.0,
        "threshold": null
      }
    ]

``similarityThreshold`` echoes the request body's ``threshold``, and ``threshold`` echoes the query parameter ``result_threshold`` (``null`` when it was left at ``0``); despite the similar names, these are two different request parameters.

v2 (latest, streaming)
-----------------------

``GET /api/vocabularies/{prefix}/similarity/v2``

Query parameters:

* ``concept_ids`` (required, repeatable): one or more concept IDs to find similar terms for.
* ``threshold`` (default ``1.0``, range ``0.0``-``1.0``): minimum similarity score to include.
* ``same_prefix`` (default ``true``): restrict results to the same vocabulary as the input concepts. Set to ``false`` to also consider similarity computed against a different corpus vocabulary.
* ``corpus`` (optional): only consider scores computed against this corpus vocabulary prefix.
* ``method`` (optional): only consider scores computed with this method (``co-annotation``, ``relevance``, or ``weighed-relevance``).
* ``limit`` (optional): maximum number of similar terms to return per input term.

The response streams a JSON array, with similar concepts grouped by vocabulary prefix and annotated with the score(s) that produced the match:

.. code-block:: json

    [
      {
        "conceptId": "0001250",
        "similarGroups": [
          {
            "prefix": "hpo",
            "similarConcepts": [
              {
                "conceptId": "0001251",
                "similarityScores": {
                  "co-annotation": 0.83,
                  "relevance:mondo": 0.71
                }
              }
            ]
          }
        ]
      }
    ]

Each key in ``similarityScores`` is either a bare method name (an intrinsic score) or ``method:corpus`` (a score computed against that corpus vocabulary).

Translating terms
==================

Translation is a constrained form of similarity lookup: instead of returning every similar concept above a threshold, it finds, for each input term, the best-matching concept from a caller-supplied candidate set. This is useful when the caller needs an answer restricted to a known, smaller set of terms, e.g. mapping free-text search results onto a fixed set of filter categories.

v1 (Cafe Variome V3)
---------------------

``POST /api/vocabularies/{prefix}/translate/v1``

Request body:

.. code-block:: json

    {
      "termIds": ["0001250"],
      "constraintIds": ["271000119106", "298382003"],
      "constraintPrefix": "snomed",
      "threshold": 0.5
    }

``constraintIds`` act as an allow-list of valid translation targets. ``constraintPrefix`` is the vocabulary they belong to; if omitted, it defaults to the same vocabulary as ``termIds`` (``{prefix}``), so existing requests that predate ``constraintPrefix`` keep working unchanged. Unlike v2, all constraint IDs in a single v1 request must belong to one vocabulary; to constrain against concepts from more than one vocabulary at once, use v2. ``threshold`` may be a single float (applied to every constraint) or a list of floats matching the length of ``constraintIds``.

Query parameters:

* ``result_threshold`` (default ``0``): maximum number of translated terms to return, ``0`` for no limit.

Response:

.. code-block:: json

    [
      {
        "termId": "0001250",
        "score": 0.62
      }
    ]

v2 (latest, streaming)
-----------------------

``GET /api/vocabularies/{prefix}/translate/v2``

Query parameters:

* ``original_ids`` (required, repeatable): concept IDs to translate, in ``{prefix}``.
* ``constraint_concepts`` (required, repeatable): candidate concept IDs to translate into, each in ``prefix:id`` format, e.g. ``?constraint_concepts=snomed:271000119106``. Unlike v1, these may span multiple different vocabularies.
* ``threshold`` (default ``1.0``, range ``0.0``-``1.0``): minimum similarity score to consider a match.
* ``limit`` (optional): maximum number of translated terms to return.

The response streams a JSON array:

.. code-block:: json

    [
      {
        "conceptId": "271000119106",
        "prefix": "snomed",
        "score": 0.62
      }
    ]
