=======
Mapping
=======

Mapping translates one or more concepts from a source vocabulary into the equivalent concepts in a *different* target vocabulary, following the annotation (cross-vocabulary) relationships loaded between them. See :doc:`../vocabularies` for the list of supported annotation pairs; mapping between two vocabularies with no supported annotation pair between them returns no results.

There are two versions of the endpoint, under ``/api/vocabularies/{prefix}/map/.../{target_prefix}``, where ``{prefix}`` is the source vocabulary prefix and ``{target_prefix}`` is the vocabulary to map into.

v1 (Cafe Variome V3)
=====================

``POST /api/vocabularies/{prefix}/map/v1/{target_prefix}``

Request body:

.. code-block:: json

    {
      "termIds": ["0001250"]
    }

Query parameters:

* ``result_threshold`` (default ``0``): maximum number of terms to return in the response, ``0`` for no limit.

This version only follows a direct annotation between ``{prefix}`` and ``{target_prefix}``, it does not hop through an intermediate vocabulary. The server collects the full result set before responding:

.. code-block:: json

    [
      {
        "termId": "0001250",
        "mappedIds": ["271000119106"],
        "targetType": "snomed"
      }
    ]

v2 (latest, streaming)
=======================

``GET /api/vocabularies/{prefix}/map/v2/{target_prefix}``

Query parameters:

* ``concept_ids`` (required, repeatable): one or more concept IDs in the source vocabulary.
* ``max_hops`` (default ``1``): maximum number of annotation hops to traverse. Set higher than 1 to map across an intermediate vocabulary when there is no direct annotation between the source and target.
* ``limit`` (optional): maximum number of mapped concepts to return for each source term. Omit for no limit.

The response streams a JSON array as results are found:

.. code-block:: json

    [
      {
        "conceptId": "0001250",
        "relatedConcepts": ["271000119106"]
      }
    ]

This is the recommended version for new integrations. Increasing ``max_hops`` widens the search but also increases query cost, since the number of possible paths grows with each additional hop.
