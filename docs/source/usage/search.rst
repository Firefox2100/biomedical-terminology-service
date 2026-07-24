======
Search
======

Search provides embedding-based semantic search over a vocabulary's concepts, as opposed to :doc:`auto-complete`'s exact substring matching. The input query string is converted into an embedding vector using the same transformer model used to embed the vocabulary (see :doc:`../build-database`), and the vector database is queried for the nearest matching concepts. This means search can return relevant concepts even when the query does not share any exact words with the concept's label, definition, or synonyms, at the cost of higher latency than auto-complete.

Search only works for vocabularies that have been embedded with ``bioterms-cli vocabulary embed``. If a vocabulary has not been embedded, this endpoint returns an empty result set rather than an error.

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
