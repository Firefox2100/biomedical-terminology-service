================
Data Management
================

These endpoints operate directly on a vocabulary's stored data, rather than querying relationships between concepts. They are not versioned like the other endpoint groups. All of them are under ``/api/vocabularies/{prefix}``, where ``{prefix}`` is a vocabulary prefix.

Authentication
==============

Most endpoints in this group are read-only and unauthenticated. The two that modify data, deleting a vocabulary and ingesting documents, require an API key. Send it as a bearer token:

.. code-block:: text

    Authorization: Bearer <api-key>

API keys are created per user account from the web UI's "API Keys" page (``/api-keys``), after logging in with an administrator account (see :doc:`../installation`). A key is only shown once, at creation time.

Vocabulary status
==================

``GET /api/vocabularies/{prefix}``

Returns the current status of a vocabulary: whether it is loaded, its concept and relationship counts, which annotations and similarity methods are available for it, and file download timestamps. This is the same information shown by ``bioterms-cli vocabulary status`` and on the vocabulary's page in the web UI.

Deleting a vocabulary
======================

``DELETE /api/vocabularies/{prefix}`` (requires an API key)

Removes all of a vocabulary's data, documents, graph nodes/relationships, and vector embeddings, from every database. This is equivalent to ``bioterms-cli vocabulary delete``. It does not delete the downloaded source files on disk.

Licence information
=====================

``GET /api/vocabularies/{prefix}/license``

Returns the vocabulary's licence text as ``text/markdown``, if one is bundled with the service (see ``src/bioterms/data/licenses``). Returns ``404`` if no licence information is available for that vocabulary.

Random concepts
================

``GET /api/vocabularies/{prefix}/random``

Query parameters:

* ``count`` (default ``10``, range 1-100): number of random concept IDs to return.

Returns a JSON array of concept ID strings, e.g. ``["0001250", "0007843"]``. This is primarily useful for testing, sampling, or populating example UIs without needing to know valid concept IDs in advance.

Downloading all documents
===========================

``GET /api/vocabularies/{prefix}/documents``

Streams every concept in the vocabulary as a JSON array, with a ``Content-Disposition: attachment`` header suggesting the filename ``{prefix}_documents.json``. This is a full export of the vocabulary's document database content and can be large for sizeable vocabularies; prefer the streaming behaviour of a client library over loading the whole response into memory.

Ingesting documents
=====================

``POST /api/vocabularies/{prefix}/documents`` (requires an API key)

Uploads concepts directly into the document database, bypassing the normal download-and-parse workflow. This is useful for loading a vocabulary from a modified or pre-processed source, or for restoring a document export produced by the endpoint above.

The request body is a stream of newline-delimited JSON objects (one concept per line), each matching the concept schema for ``{prefix}``. The body may optionally be gzip-compressed; set ``Content-Encoding: gzip`` when doing so. Malformed input aborts the ingestion and returns ``400``.

Response:

.. code-block:: json

    {
      "conceptCount": 19847
    }

``conceptCount`` is the vocabulary's total concept count after ingestion, not just the number of concepts uploaded in this request.

Getting a single concept
==========================

``GET /api/vocabularies/{prefix}/{concept_id}``

Returns a concept together with its direct (one hop) children and parents in the graph:

.. code-block:: json

    {
      "concept": {"prefix": "hpo", "conceptId": "0001250", "label": "Narrow face", "...": "..."},
      "children": [{"prefix": "hpo", "conceptId": "0001251", "...": "..."}],
      "parents": [{"prefix": "hpo", "conceptId": "0001100", "...": "..."}]
    }

Returns ``404`` if ``concept_id`` does not exist in ``{prefix}``. For descendants beyond one level, use :doc:`expansion` instead.
