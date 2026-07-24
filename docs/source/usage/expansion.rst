=========
Expansion
=========

Expansion returns the descendants of one or more concepts within the same vocabulary, following the ``is_a``/``part_of`` hierarchy stored in the graph database. This is typically used to broaden a search, e.g. expanding a disease concept to include all of its more specific subtypes.

There are two versions of the endpoint, under ``/api/vocabularies/{prefix}/expand/...``, where ``{prefix}`` is the vocabulary prefix shared by all of the requested concepts.

v1 (Cafe Variome V3)
=====================

``POST /api/vocabularies/{prefix}/expand/v1``

Request body:

.. code-block:: json

    {
      "termIds": ["0001250", "0001251"]
    }

Query parameters:

* ``depth`` (default ``3``): how many levels of descendants to expand to, ``0`` for no limit.
* ``result_threshold`` (default ``0``): maximum number of terms to return in the response, ``0`` for no limit.

The server collects the full result set before responding:

.. code-block:: json

    [
      {
        "termId": "0001250",
        "children": ["0001251", "0001252"],
        "depth": 3
      }
    ]

Note that ``depth`` in each response item echoes the requested expansion depth, it is not the depth of that specific term's children in the hierarchy.

v2 (latest, streaming)
=======================

``GET /api/vocabularies/{prefix}/expand/v2``

Query parameters:

* ``concept_ids`` (required, repeatable): one or more concept IDs to expand, e.g. ``?concept_ids=0001250&concept_ids=0001251``.
* ``depth`` (optional): maximum depth to expand to. Omit for no limit.
* ``limit`` (optional): maximum number of descendants to return for each term. Omit for no limit.

The response streams a JSON array as results are found:

.. code-block:: json

    [
      {
        "conceptId": "0001250",
        "relatedConcepts": ["0001251", "0001252"]
      }
    ]

This is the recommended version for new integrations, particularly when expanding concepts that may have a large number of descendants.
