=======================
Auto-Completion Service
=======================

This service provides dedicated auto-completion feature for usage with interactive UI components. The work flow is:

* In the UI, a user may input a partial string for a concept.
* The UI sends the partial string to this service. These endpoints are designed to work with frontend, focusing on fast responses, and has permissive CORS settings.
* This service returns a list of concepts that contains the provided partial string in the concept ID, label, synonym or definition.

Note that the auto-completion search uses exact match, and no fuzzy match or semantic search is implemented. If there is a typo in the string, there may be no results returned at all.

There are three versions of the endpoint, all under ``GET /api/vocabularies/{prefix}/auto-complete/...``, where ``{prefix}`` is a vocabulary prefix (e.g. ``hpo``, ``snomed``).

v1 (legacy Cafe Variome V2)
===========================

``GET /api/vocabularies/{prefix}/auto-complete/v1/query/{query_str}``

Kept for compatibility with the legacy Cafe Variome V2 frontend. ``query_str`` is a path segment rather than a query parameter, and the optional ``long`` query parameter (boolean, default ``false``) switches the result limit between 25 and 250 matches.

The response is a plain list of strings, not a list of concept objects, formatted as ``"{prefix}:{conceptId} (label) synonyms:[syn1, syn2]"`` (the label and synonym segments are omitted when the concept has none). If the query string is shorter than ``BTS_AUTO_COMPLETE_MIN_LENGTH`` (3 characters by default), the endpoint does not return an HTTP 400 error as later versions do; instead, for backward compatibility with the legacy frontend, it returns ``200 OK`` with a single-element array containing a human-readable message instead of a result list. Do not use this quirk to detect short queries in new integrations, use the minimum length client-side instead.

v2 (Cafe Variome V3)
=====================

``GET /api/vocabularies/{prefix}/auto-complete/v2``

Query parameters:

* ``query`` (required): the search string, must be at least ``BTS_AUTO_COMPLETE_MIN_LENGTH`` characters.
* ``with_definition`` (default ``false``): include the concept definition in the response.
* ``result_threshold`` (default ``0``): maximum number of results, ``0`` for no limit.

The response is a JSON array of objects shaped as:

.. code-block:: json

    [
      {
        "termId": "0001250",
        "label": "Narrow face",
        "definition": null
      }
    ]

``definition`` is always present in the response but is only populated when ``with_definition=true``.

v3 (latest, streaming)
=======================

``GET /api/vocabularies/{prefix}/auto-complete/v3``

Query parameters:

* ``query`` (required): the search string, must be at least ``BTS_AUTO_COMPLETE_MIN_LENGTH`` characters.
* ``limit`` (default ``20``): maximum number of results, ``0`` for no limit.

The response streams a JSON array of full concept objects (the same shape used across the rest of the API for the vocabulary's concept type, including synonyms and any vocabulary-specific fields) as they are found, rather than collecting them all in memory first. This is the recommended version for new integrations.
