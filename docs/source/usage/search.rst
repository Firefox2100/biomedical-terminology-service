======
Search
======

Search provides hybrid semantic + lexical search over a vocabulary's concepts, as opposed to :doc:`auto-complete`'s exact substring matching. Three recall arms run in parallel and are combined with Reciprocal Rank Fusion (RRF):

* **Lexical recall**: BM25 or the backend's native full-text ranking (n-gram overlap where neither is available - see :doc:`../build-database`) against concept ID/label/synonyms.
* **Alias-embedding recall**: the query is embedded and matched against each concept's individually-embedded label/synonym vectors (see "Embedding the concepts" in :doc:`../build-database`).
* **Definition-embedding recall**: the same query embedding matched against concept definition vectors.

A concept whose ID, label, or a synonym exactly matches the query (case-insensitively) bypasses RRF entirely and is placed ahead of the fused results, so an exact term can never be outranked by merely-similar embeddings or partial lexical hits. Because of this hybrid design, search can return relevant concepts even when the query does not share any exact words with the concept's label, definition, or synonyms, while still surfacing an exact match reliably.

Search only works for vocabularies that have been embedded with ``bioterms-cli vocabulary embed``; the lexical recall arm still works without embeddings, but alias/definition recall will have nothing to contribute. If a vocabulary has not been embedded, this endpoint still returns lexical matches rather than an empty result set.

``GET /api/vocabularies/{prefix}/search/v1``

Query parameters:

* ``query`` (required): the search string.
* ``limit`` (default ``10``): maximum number of concepts to return.

The response streams a JSON array of full concept objects, in the same shape used across the rest of the API for the vocabulary's concept type:

.. code-block:: json

    [
      {
        "prefix": "hpo",
        "conceptId": "0001250",
        "label": "Narrow face",
        "synonyms": ["Decreased width of face"],
        "definition": "..."
      }
    ]

Results are ordered by similarity to the query, most similar first. There is currently only one version of this endpoint.
