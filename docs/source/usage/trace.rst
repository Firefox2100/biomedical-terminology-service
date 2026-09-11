=====
Trace
=====

Trace finds paths between two specific concepts in the graph database, following a single named relationship or annotation type. This is more targeted than :doc:`expansion` or :doc:`mapping`: rather than returning every descendant or every mapped concept, it answers "how (if at all) is concept A connected to concept B via relationship X".

Unlike the other endpoint groups, trace currently has only one version, which is already a streaming ``GET`` endpoint.

``GET /api/vocabularies/{prefix}/trace/v1/{target_prefix}``

``{prefix}`` is the vocabulary of the starting concept and ``{target_prefix}`` is the vocabulary of the ending concept; they may be the same vocabulary or different ones, as long as ``relationship`` is valid between them.

Query parameters:

* ``start_id`` (required): the concept ID to start tracing from, in ``{prefix}``.
* ``end_id`` (required): the concept ID to trace to, in ``{target_prefix}``.
* ``relationship`` (required): the relationship or annotation type to trace along. Accepts either a within-vocabulary relationship type (``is_a``, ``part_of``, ``replaced_by``, ``preceded_by``, ``has_input``, ``has_output``, ``ohdsi_relationship``, ``annotated_with``, ``consider``) or a cross-vocabulary annotation type (``exact``, ``broad``, ``narrow``, ``related``, ``has_symbol``, ``alias_symbol``, ``previous_symbol``, ``annotated_with``).
* ``forward`` (default ``true``): if ``true``, only paths going from ``start_id`` to ``end_id`` are returned; if ``false``, only paths from ``end_id`` to ``start_id``; if left unset (``null``), direction is ignored and only the shortest path is returned.
* ``max_hops`` (default ``12``): the maximum number of relationship hops to search along.

The response streams a JSON array of every path found, without repeating sequences (if one returned path is a subset of another with the same order preserved, only the shorter path is included):

.. code-block:: json

    [
      {
        "startConceptId": "0001250",
        "endConceptId": "0001100",
        "startPrefix": "hpo",
        "endPrefix": "hpo",
        "length": 3,
        "nodes": [
          {"conceptId": "0001250", "prefix": "hpo"},
          {"conceptId": "0001200", "prefix": "hpo"},
          {"conceptId": "0001100", "prefix": "hpo"}
        ]
      }
    ]

``nodes`` lists every concept on the path, in order from ``start_id`` to ``end_id`` regardless of the ``forward`` direction that was searched.
