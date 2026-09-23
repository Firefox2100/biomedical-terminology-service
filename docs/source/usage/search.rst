======
Search
======

Search provides hybrid semantic + lexical search over a vocabulary's concepts, as opposed to :doc:`auto-complete`'s exact substring matching. Three standard recall arms run in parallel and are combined with Reciprocal Rank Fusion (RRF):

* **Lexical recall**: BM25 or the backend's native full-text ranking (n-gram overlap where neither is available - see :doc:`../build-database`) against concept ID/label/synonyms.
* **Alias-embedding recall**: the query is embedded and matched against each concept's individually-embedded label/synonym vectors (see "Embedding the concepts" in :doc:`../build-database`).
* **Definition-embedding recall**: the same query embedding matched against concept definition vectors.

An optional fourth **mapped recall** arm can be enabled with
``BTS_SEARCH_MAPPED_RECALL_LIMIT``. It lexically searches vocabularies connected to the
requested vocabulary by ``EXACT`` annotations, retains exact source ID/label/synonym matches,
and maps those source concepts into target concepts.
Mapped results are ordinary fusion candidates: they are bounded by
``BTS_SEARCH_MAPPED_CANDIDATE_LIMIT``, never treated as exact query matches, and still pass
through the configured reranker. It is disabled by default because deployments must validate
their cross-vocabulary annotations and precision before enabling it.

A concept whose ID, label, or a synonym exactly matches the query (case-insensitively) bypasses RRF entirely and is placed ahead of the fused results, so an exact term can never be outranked by merely-similar embeddings or partial lexical hits. Because of this hybrid design, search can return relevant concepts even when the query does not share any exact words with the concept's label, definition, or synonyms, while still surfacing an exact match reliably.

When a reranker is configured, the non-exact fused candidates are reranked after recall.
Exact matches bypass both fusion and reranking.

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

Results are ordered by similarity to the query, most similar first. V1 is retained for clients
that require the original bare-array response.

Search V2
---------

V2 exposes the same shared query pipeline in a cache-friendly response envelope and can search
one or more vocabularies in a single request:

``GET /api/search/v2?query=short%20stature&vocabulary=hpo&vocabulary=mondo&limit=10``

Repeat the ``vocabulary`` query parameter to search multiple vocabularies. The optional
``includeMatchDetails`` parameter defaults to ``true`` and reports whether a result was an
exact ID, label, or synonym match. Retrieval depth, vector over-retrieval, fusion, and
reranker settings remain server-side configuration.

The vocabulary-scoped convenience route uses the same implementation:

``GET /api/vocabularies/{prefix}/search/v2?query=short%20stature&limit=10``

.. code-block:: json

    {
      "query": "short stature",
      "results": [
        {
          "rank": 1,
          "concept": {
            "prefix": "hpo",
            "conceptId": "0004322",
            "label": "Short stature"
          },
          "match": {
            "type": "exact",
            "exact": true,
            "field": "label",
            "text": "Short stature"
          }
        }
      ],
      "meta": {
        "returned": 1,
        "limit": 10,
        "durationMs": 12.4,
        "vocabularies": ["hpo", "mondo"],
        "pipeline": {
          "lexical": true,
          "vector": true,
          "mapped": false,
          "reranker": true
        }
      }
    }

Both search routes are cacheable GET endpoints. Dataset rebuilds rotate the dataset version
used by the service's ``ETag`` and ``Last-Modified`` response validators.
