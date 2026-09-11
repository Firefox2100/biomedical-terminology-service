=================
Monitoring
=================

The BioMedical Terminology Service exposes several endpoints and integrations for monitoring health, performance, and errors.

Health Check
============

``GET /health``

Returns a minimal JSON response confirming the application process is alive:

.. code-block:: json

   {"status": "ok"}

This endpoint has no authentication requirement and does not check the health of the connected databases, so it may return ``200 OK`` even when the service cannot serve queries because a backend is unreachable. Use it for liveness probes; use the vocabulary status endpoint (:doc:`usage/data-management` → ``GET /api/vocabularies/<prefix>/status``) for readiness probes that validate data availability.

Prometheus Metrics
==================

If ``BTS_ENABLE_METRICS`` is true (the default), the service runs the
`pytheus <https://github.com/Deep-EYE06/pytheus>`_ metrics library and exposes a Prometheus scraper endpoint at the root path ``/metrics``. Set ``BTS_OPENAPI_URL`` to ``null`` in the ``.env`` file or environment variables to exclude metrics from the interactive OpenAPI documentation.

The metrics endpoint requires no authentication and returns plaintext Prometheus format. It is excluded from the OpenAPI schema so that API documentation generators do not list it as an endpoint.

.. tip::

   Configure your Prometheus scrape target with:

   .. code-block:: yaml

      scrape_configs:
        - job_name: 'bioterms'
          metrics_path: '/metrics'
          static_configs:
            - targets: ['localhost:5000']

Metric categories
-----------------

The service defines 9 counters and 35 histograms. Every metric carries a ``prefix`` label that identifies the vocabulary it applies to (e.g. ``hpo``, ``snomed``). Some histograms additionally carry the ``result`` label as ``"ok"`` or ``"err"`` so that latency distributions can be split by outcome.

Counters
~~~~~~~~

| Counter | Description

| ``docdb_op_errors_total`` | Total number of document database operation errors |

| ``graphdb_op_errors_total`` | Total number of graph database operation errors |

| ``graphdb_op_retrys_total`` | Total number of graph database operation retries |

| ``embed_errors_total`` | Total number of embedding operation errors |

| ``vectordb_op_errors_total`` | Total number of vector database operation errors |

| ``autocomplete_stream_errors_total`` | Total number of auto-complete stream errors |

| ``expand_requests_total`` | Total number of expansion requests |

| ``map_requests_total`` | Total number of mapping requests |

| ``similarity_requests_total`` | Total number of similarity search requests |

Histograms
~~~~~~~~~~

Each histogram distributes its samples into buckets.

.. list-table:: Database, embedding, and vector histograms
   :header-rows: 1
   :widths: 50 20 30
   :class: noclass

   * - Name
     - Unit
     - Description
   * - ``docdb_op_duration_seconds``
     - Seconds
     - Operation duration (``backend``, ``op``, ``prefix``, ``result``)
   * - ``docdb_op_time_to_first_item_seconds``
     - Seconds
     - Time to first yielded item in streams (``backend``, ``op``, ``prefix``, ``result``)
   * - ``graphdb_op_duration_seconds``
     - Seconds
     - Operation duration (``backend``, ``op``, ``prefix``, ``mode``, ``result``)
   * - ``graphdb_op_time_to_first_result_seconds``
     - Seconds
     - Time to first result in streams (``backend``, ``op``, ``prefix``, ``mode``, ``result``)
   * - ``embed_lock_wait_seconds``
     - Seconds
     - Wait time for embed lock acquisition (``model``, ``result``)
   * - ``embed_duration_seconds``
     - Seconds
     - Embedding operation duration (``model``, ``result``)
   * - ``embed_texts_count``
     - Count
     - Texts embedded per request (``model``)
   * - ``embed_chars_total``
     - Count
     - Characters embedded per request (``model``)
   * - ``vectordb_op_duration_seconds``
     - Seconds
     - Vector database operation duration (``backend``, ``op``, ``prefix``, ``result``)

.. list-table:: Autocomplete, search, expand, and map histograms
   :header-rows: 1
   :widths: 50 20 30
   :class: noclass

   * - Name
     - Unit
     - Description
   * - ``autocomplete_items_returned``
     - Count
     - Items returned per autocomplete request (``prefix``)
   * - ``autocomplete_limit``
     - Count
     - Requested limit per autocomplete request (``prefix``)
   * - ``autocomplete_query_length``
     - Count
     - Length of the autocomplete query string (``prefix``)
   * - ``search_items_returned``
     - Count
     - Items returned per search request (``prefix``)
   * - ``search_limit``
     - Count
     - Requested limit per search request (``prefix``)
   * - ``search_query_length``
     - Count
     - Length of the search query string (``prefix``)
   * - ``expand_roots_count``
     - Count
     - Number of concept roots requested for expansion (``prefix``)
   * - ``expand_depth_requested``
     - Count
     - Requested expansion depth (``prefix``)
   * - ``expand_limit_requested``
     - Count
     - Requested expansion result limit (``prefix``, ``has_limit``)
   * - ``expand_descendants_count``
     - Count
     - Descendants returned per expansion request (``prefix``, ``mode``)
   * - ``map_roots_count``
     - Count
     - Number of concept roots requested for mapping (``prefix``, ``target_prefix``)
   * - ``map_hops_requested``
     - Count
     - Requested hop limit for multi-hop mapping (``prefix``, ``target_prefix``)
   * - ``map_limit_requested``
     - Count
     - Requested mapping result limit (``prefix``, ``target_prefix``, ``has_limit``)
   * - ``map_mapped_terms_count``
     - Count
     - Terms returned per mapping request (``prefix``, ``target_prefix``)

.. list-table:: Similarity histograms
   :header-rows: 1
   :widths: 50 20 30
   :class: noclass

   * - Name
     - Unit
     - Description
   * - ``similarity_roots_count``
     - Count
     - Number of roots requested for similarity search (``prefix``)
   * - ``similarity_threshold_requested``
     - Count
     - Requested similarity threshold (``prefix``)
   * - ``similarity_limit_requested``
     - Count
     - Requested similarity result limit (``prefix``, ``has_limit``)
   * - ``similarity_groups_count``
     - Count
     - Number of similar groups returned (``prefix``, ``variant``)
   * - ``similarity_per_group_count``
     - Count
     - Items per similarity group (``prefix``, ``variant``)
   * - ``similarity_total_items_count``
     - Count
     - Total items returned in similarity response (``prefix``, ``variant``)

Metric storage
--------------

When both ``BTS_ENABLE_METRICS`` is true (the default) and ``BTS_CACHE_DRIVER`` is set to ``redis`` (the only supported driver), metrics are stored in Redis via the ``pytheus`` ``MultiProcessRedisBackend`` on Redis database ``15``. The Redis backend ensures that metrics collected by multiple worker processes are aggregated in a shared Redis store by Pytheus.

If any other ``BTS_CACHE_DRIVER`` is used the metrics will be stored locally in each process memory and not shared across workers.

Sentry Error Tracking
=====================

If ``BTS_ENABLE_ERROR_REPORTING`` is enabled (default ``false``) and ``BTS_SENTRY_DSN`` is configured, the service initialises the Sentry SDK on startup and automatically catches application exceptions. Both ``BtsError`` (the service's exception class for operational errors) and ``HTTPException`` instances are reported to Sentry. The SDK is initialized with ``release`` set to the value of the ``VERSION`` file in the repository root (or the git ``--short`` commit hash if the ``VERSION`` file is absent), so that errors can be correlated with the deployed code.

The ``report_exception()`` helper (`etc/utils.py <https://github.com/Firefox2100/biomedical-terminology-service/blob/main/src/bioterms/etc/utils.py>`_) is called from the exception-handling middleware, so every unhandled exception is captured.

Optional profiling
------------------

When ``BTS_ENABLE_PROFILING=true`` (default ``false``) and ``BTS_SENTRY_DSN`` is configured the SDK also captures request traces and profiling sessions. Set
``BTS_SENTRY_TRACES_SAMPLE_RATE`` to control the percentage of requests captured (default 1.0 = 100%). Set ``BTS_SENTRY_PROFILE_SAMPLE_RATE`` to control the percentage of profiles collected (default 1.0 = 100%).

Profiling data is sent to Sentry's performance monitoring UI alongside the error reports and can be correlated with Prometheus metrics via the trace ID. Admins should pay extra attention that the profiling data may contain sensitive information or personal data, so it is recommended to use this feature only in trusted environments and with appropriate data handling policies.
