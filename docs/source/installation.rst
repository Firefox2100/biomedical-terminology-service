==================
Installation Guide
==================

This guide will help you install the software on your system. Note that only Linux distributions are
officially supported, due to the usage of multiprocessing libraries and torch.

Contents
--------

.. contents::
   :local:
   :depth: 2

Prerequisites
=============

The following software must be installed on your system before proceeding with the installation:

* A **document database** for storing and retrieving biomedical terminology text data.
  MongoDB is recommended and used throughout this guide. PostgreSQL, MySQL/MariaDB, or SQLite are also
  supported as alternatives via SQLAlchemy.
* A **graph database** for storing relationships between terminology concepts. Neo4j is recommended
  and used throughout this guide. PostgreSQL is also supported as an alternative (plain relational
  tables plus recursive CTEs, not a graph extension), and can share the same PostgreSQL instance as
  the document/vector databases above.
* A **cache** for hot data and inter-process communication. Only Redis is supported.
* A **vector database** for storing and searching vector embeddings. Qdrant is recommended and used
  throughout this guide. MongoDB (with Atlas Search / mongot support) and PostgreSQL (with the
  pgvector extension) are also supported as alternatives - the latter can share the same
  PostgreSQL instance as the document database, avoiding a separate vector store entirely.

Resource requirements:

* Disk space depends on the terminologies you plan to load: a few gigabytes to over 100 GB.
* At least 2 CPU cores and 4 GB of RAM for queries to work. For moderate workloads, 4 cores and
  8 GB of RAM is recommended.
* It is not recommended to build the database directly on the serving server because it consumes
  significant CPU and memory. Build it on a separate machine (preferably an HPC with CPU and GPU)
  and transfer the built database to the server. Refer to :doc:`build-database` for details.

If installing on bare metal, ensure:

* Python 3.11 or higher
* Active internet connection for downloading dependencies
* Ability to install pip packages, or use virtualenv/conda

Using Docker (Recommended)
==========================

The recommended way to install and run the software is using Docker. This method simplifies the
installation process by encapsulating all dependencies within a container, providing an isolated
environment that avoids conflicts with other software on the system.

Available Images
----------------

There are different tags available for different use cases:

* ``latest`` - tracks the latest stable release. May include breaking changes; update carefully.
* A specific version tag (e.g., ``v1.2.3``) - corresponds to a specific release. Recommended for
  production where stability is crucial.
* ``-cpu`` suffix - CPU-only installation. Suitable for systems without a compatible GPU. GPU is
  only needed for embedding and GNN training, not for serving queries.
* Default image (no ``-cpu`` suffix) - includes GPU support (CUDA torch). Recommended if you have
  an NVIDIA GPU for faster embedding and training. These images are significantly larger
  (10 GB or more) due to CUDA libraries.

Choose the appropriate tag based on your system capabilities and requirements.

Docker Compose
--------------

The repository provides two compose files:

``docker-compose.yaml``
   Deploys the bioterms service alongside its dependencies (Neo4j, MongoDB, Redis, Qdrant).
   Environment variable values are embedded directly.

``scripts/docker-compose.dependencies.yaml``
   A standalone dependencies file that publishes ports to the host, for local development or
   standalone database access. Combine it with the main ``docker-compose.yaml`` via:

   .. code-block:: bash

       docker compose -f docker-compose.yaml -f scripts/docker-compose.dependencies.yaml up

Both compose files also define two further opt-in `Compose profiles
<https://docs.docker.com/compose/how-tos/profiles/>`_, neither started by a plain
``docker compose up``:

* ``mongodb-search`` - MongoDB Community Server plus MongoDB Community Search (``mongot``), a
  self-hosted, SSPL-licensed alternative to Qdrant for ``BTS_VECTOR_DATABASE_DRIVER=mongodb``.
* ``postgres`` - a PostgreSQL+pgvector container, usable as the document database
  (``BTS_DOC_DATABASE_DRIVER=sql``), the vector database (``BTS_VECTOR_DATABASE_DRIVER=postgresql``),
  the graph database (``BTS_GRAPH_DATABASE_DRIVER=postgresql``), or all three at once - the document
  and vector stores share the same tables, and the graph store's own ``graph_*``-named tables live
  alongside them, so one PostgreSQL instance can replace MongoDB/SQL, Qdrant/MongoDB, and Neo4j
  simultaneously.

Pass ``--profile <name>`` to include one. See :doc:`build-database` for details.

### Example Compose

The ``docker-compose.yaml`` file from the repository contains the full production-ready configuration,
including both the ``bioterms`` web service and an optional ``bioterms-worker`` Celery worker process.
The compose files reference the images published on Docker Hub at
``firefox2100/biomedical-terminology-service``.

.. note::

   ``BTS_SECRET_KEY`` (used for session cookies) is auto-generated if omitted.
   ``BTS_SERVER_HMAC_KEY`` **must** be set; generate it with:

   .. code-block:: text

       docker compose exec bioterms bioterms-cli generate-hmac-key

After the containers are up, create an administrator account:

.. code-block:: bash

   docker compose exec bioterms bioterms-cli user create <username>

From Source Code
================

First install the project with the ``all`` extra, which includes every optional dependency
(SQLAlchemy drivers, GNN support, etc.):

.. code-block:: bash

   pip install .[all]

Then configure environment variables (read from ``BTS_*`` env vars first, then from a ``.env`` file
at the path specified by ``BTS_ENV_FILE``, or ``conf/.env`` by default), build the database
(see :doc:`build-database`), create an admin user, and start the service:

.. code-block:: bash

   export BTS_SERVER_HMAC_KEY=$(python -c "import secrets; print(secrets.token_urlsafe(32))")
   bioterms-cli user create admin
   uvicorn bioterms.asgi:application --host 127.0.0.1 --port 5000
   # (Optional) celery -A bioterms.task.app.celery_app worker --loglevel=info

After loading or deleting a vocabulary or annotation through the CLI or the web UI, the GraphQL
schema must be refreshed. Either send an authenticated ``POST /reload-graphql`` request or restart the
web service. A full restart is not required.

Configuration Setup
===================

The service is entirely controlled by ``BTS_*`` environment variables. The variable is resolved
in the following order (first match wins):

1. **OS environment variables** - the OS ``os.environ`` namespace.
2. **``.env`` file** - at the path specified by the ``BTS_ENV_FILE`` environment variable. Default
   path is ``conf/.env`` relative to the application directory.
3. **Docker secrets** - if the ``/run/secrets`` directory exists, individual environment variable
   files (e.g., ``/run/secrets/BTS_SERVER_HMAC_KEY``) are read.

Any variable that has a non-``...`` sentinel value in the ``Settings`` class (i.e., any field
declared with ``Field(default_value)``) has a built-in default. Variables declared as ``...``
(the Ellipsis sentinel, such as ``server_hmac_key``) are required; the service will raise a
``ValidationError`` at startup if no value is provided.

A complete ``example.env`` file is available in the repository root with every environment variable
listed, commented, and grouped into logical sections. Copy it and fill in the values needed for
your deployment:

.. code-block:: bash

   cp example.env conf/.env

Configuration Reference
=======================

The following tables document every ``BTS_*`` configuration variable.

.. note::

   The ``Sec`` column indicates if the variable is secret (``yes``) or public (``no``).
   Secret variables are often used in cookies, signatures, or API keys.

General Settings
----------------

.. list-table:: General
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_PROCESS_LIMIT``
     - ``4``
     - no
     - Maximum worker process count for CLI data handling. Not used by the running web service.
   * - ``BTS_AUTO_COMPLETE_MIN_LENGTH``
     - ``3``
     - no
     - Minimum query-string length for auto-complete searches
   * - ``BTS_LOGGING_LEVEL``
     - ``INFO``
     - no
     - Logging level (``CRITICAL``, ``ERROR``, ``WARNING``, ``INFO``, ``DEBUG``, or ``NOTSET``)
   * - ``BTS_SECRET_KEY``
     - auto-generated
     - yes
     - Secret key for session cookies. A different key is generated on each start if not set.
   * - ``BTS_SERVER_HMAC_KEY``
     - required
     - yes
     - HMAC key for hashing API keys. No default value. Generate with: ``bioterms-cli generate-hmac-key``
   * - ``BTS_USE_HTTPS``
     - ``false``
     - no
     - Whether the application is behind an HTTPS proxy. Affects cookie settings, redirect URLs, and security headers.
   * - ``BTS_ENABLE_METRICS``
     - ``true``
     - no
     - Enable Prometheus metrics endpoint at ``/metrics``
   * - ``BTS_ENABLE_ERROR_REPORTING``
     - ``false``
     - no
     - Enable error reporting to Sentry or compatible services
   * - ``BTS_ENABLE_PROFILING``
     - ``false``
     - no
     - Enable performance profiling using Sentry SDK
   * - ``BTS_SENTRY_DSN``
     - *(empty)*
     - yes
     - Sentry DSN for error reporting
   * - ``BTS_GOOGLE_SITE_VERIFICATION_ID``
     - *(empty)*
     - no
     - Google site verification ID for webmaster tools. Injects the ``<meta name="google-site-verification">`` tag into HTML pages.
   * - ``BTS_DATA_DIR``
     - ``data``
     - no
     - Directory path inside the container for storing data files
   * - ``BTS_SERVICE_ROOT_PATH``
     - *(empty)*
     - no
     - Root path prefix for reverse-proxy setups (e.g. ``/bts``)
   * - ``BTS_OPENAPI_URL``
     - ``/openapi.json``
     - no
     - URL path for the OpenAPI schema
   * - ``BTS_DOCS_URL``
     - ``/docs``
     - no
     - URL path for the Swagger UI documentation
   * - ``BTS_REDOC_URL``
     - ``/redoc``
     - no
     - URL path for the ReDoc documentation
   * - ``BTS_FHIR_CANONICAL_URL``
     - ``https://your.deployment.com/fhir``
     - no
     - Canonical base URL for FHIR resources (CodeSystem URLs). Change to your deployment domain.
   * - ``BTS_ENVIRONMENT``
     - ``dev``
     - no
     - Runtime environment (``dev``, ``staging``, ``prod``, or ``test``)

Celery (Worker Only)
--------------------

.. list-table:: Celery
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_CELERY_BROKER``
     - ``redis://localhost:6379/1``
     - yes
     - Redis URL for Celery message broker (task input)
   * - ``BTS_CELERY_BACKEND``
     - ``redis://localhost:6379/2``
     - yes
     - Redis URL for Celery result backend (task output)

Document Database
-----------------

.. list-table:: Document Database
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_DOC_DATABASE_DRIVER``
     - ``mongo``
     - no
     - ``mongo`` or ``sql``. When ``sql``, an async driver package (``asyncpg``, ``aiomysql``, or ``aiosqlite``) is required.
   * - ``BTS_MONGODB_HOST``
     - ``localhost``
     - no
     - MongoDB host
   * - ``BTS_MONGODB_PORT``
     - ``27017``
     - no
     - MongoDB port
   * - ``BTS_MONGODB_DB_NAME``
     - ``bts``
     - no
     - MongoDB database name
   * - ``BTS_MONGODB_USERNAME``
     - *(empty)*
     - yes
     - MongoDB username
   * - ``BTS_MONGODB_PASSWORD``
     - *(empty)*
     - yes
     - MongoDB password
   * - ``BTS_MONGODB_AUTH_SOURCE``
     - ``admin``
     - no
     - MongoDB authentication source database
   * - ``BTS_SQL_DB_URL``
     - ``sqlite+aiosqlite:///./bts.sqlite3``
     - no
     - SQLAlchemy async connection URL. Example: ``postgresql+asyncpg://user:pass@host:5432/bts``
   * - ``BTS_SQL_BATCH_SIZE``
     - ``5000``
     - no
     - Batch size for SQL database write operations

Graph Database
--------------

.. list-table:: Graph Database
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_GRAPH_DATABASE_DRIVER``
     - ``neo4j``
     - no
     - ``neo4j`` or ``postgresql``
   * - ``BTS_NEO4J_URI``
     - ``neo4j://localhost:7687``
     - no
     - Neo4j Bolt connection URI
   * - ``BTS_NEO4J_DB_NAME``
     - ``neo4j``
     - no
     - Neo4j database name
   * - ``BTS_NEO4J_USERNAME``
     - ``neo4j``
     - yes
     - Neo4j username
   * - ``BTS_NEO4J_PASSWORD``
     - ``password``
     - yes
     - Neo4j password
   * - ``BTS_NEO4J_DELETE_BATCH_SIZE``
     - ``2000``
     - no
     - Number of rows (relationships/nodes) committed per transaction when batch-deleting from Neo4j. Lower values reduce peak transaction memory usage at the cost of speed; important for large vocabularies on memory-constrained Neo4j instances.
   * - ``BTS_POSTGRES_GRAPH_DB_URL``
     - ``postgresql+asyncpg://localhost:5432/bts``
     - no
     - SQLAlchemy async URL for the PostgreSQL graph database. Only used when ``BTS_GRAPH_DATABASE_DRIVER=postgresql``. Graph tables live under their own ``graph_*`` names, so this can safely equal ``BTS_SQL_DB_URL``/``BTS_POSTGRES_VECTOR_DB_URL`` to share one PostgreSQL instance.
   * - ``BTS_POSTGRES_GRAPH_CLOSURE_MAX_DEPTH``
     - ``500``
     - no
     - Safety bound on recursion depth when (re)building a vocabulary's ancestor/descendant closure table. Guards against runaway recursion on a malformed/cyclic hierarchy; real ontologies are far shallower than this. Only used when ``BTS_GRAPH_DATABASE_DRIVER=postgresql``.

Cache
-----

.. list-table:: Cache
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_CACHE_DRIVER``
     - ``redis``
     - no
     - Currently only ``redis`` is supported
   * - ``BTS_REDIS_HOST``
     - ``localhost``
     - no
     - Redis host
   * - ``BTS_REDIS_PORT``
     - ``6379``
     - no
     - Redis port
   * - ``BTS_REDIS_DB``
     - ``0``
     - no
     - Redis database index
   * - ``BTS_CACHE_HARD_TTL_MULTIPLIER``
     - ``7``
     - no
     - Multiplier applied to cache item TTLs for Redis hard expiration. The item TTL is the stale-after time; Redis expiration is only a safety limit.
   * - ``BTS_CACHE_REBUILD_LOCK_TTL``
     - ``3600``
     - no
     - Seconds to hold the cache-rebuild single-flight lock, preventing concurrent rebuilds.

Vector Database
---------------

.. list-table:: Vector Database
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_VECTOR_DATABASE_DRIVER``
     - ``qdrant``
     - no
     - ``qdrant``, ``mongodb``, or ``postgresql``
   * - ``BTS_QDRANT_LOCATION``
     - ``http://localhost:6333``
     - no
     - Qdrant API endpoint URL
   * - ``BTS_MONGODB_VECTOR_INDEX_NAME``
     - ``vector_index``
     - no
     - Name of the ``$vectorSearch`` index created on each vocabulary collection. Only used when
       ``BTS_VECTOR_DATABASE_DRIVER=mongodb``; reuses the ``BTS_MONGODB_*`` connection settings above.
   * - ``BTS_MONGODB_VECTOR_NUM_CANDIDATES_MULTIPLIER``
     - ``10``
     - no
     - Multiplier applied to the requested result limit to compute ``$vectorSearch``'s ``numCandidates``.
       Only used when ``BTS_VECTOR_DATABASE_DRIVER=mongodb``.
   * - ``BTS_POSTGRES_VECTOR_DB_URL``
     - ``postgresql+asyncpg://localhost:5432/bts``
     - no
     - SQLAlchemy async URL for the PostgreSQL/pgvector vector database. Only used when
       ``BTS_VECTOR_DATABASE_DRIVER=postgresql``. When equal to ``BTS_SQL_DB_URL`` (and
       ``BTS_DOC_DATABASE_DRIVER=sql``), vectors share the SQL document database's own tables
       instead of a separate vector-only set.

External API Keys (Optional)
----------------------------

.. list-table:: External API Keys
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_BIOPORTAL_API_KEY``
     - None
     - yes
     - BioPortal API key. Needed for downloading OMIM and ORDO.
   * - ``BTS_NHS_TRUD_API_KEY``
     - None
     - yes
     - NHS TRUD API key. Needed for downloading CTV3 and SNOMED CT.
   * - ``BTS_NIH_UMLS_API_KEY``
     - None
     - yes
     - NIH UMLS API key. Needed for SNOMED-ORDO mappings.

Embedding & GNN Settings
------------------------

.. list-table:: Embedding & GNN
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_TRANSFORMER_MODEL_NAME``
     - ``BAAI/bge-base-en-v1.5``
     - no
     - HuggingFace model name for concept embedding generation
   * - ``BTS_EMBEDDING_PROCESS_LIMIT``
     - ``1``
     - no
     - Worker-process count for embedding generation. Set to 1 to disable multiprocessing.
   * - ``BTS_EMBEDDING_BATCH_SIZE``
     - ``32``
     - no
     - Number of concepts to embed per batch
   * - ``BTS_TORCH_DEVICE``
     - ``cpu``
     - no
     - PyTorch device (``cpu`` or ``cuda``)
   * - ``BTS_GNN_EPOCHS``
     - ``100``
     - no
     - Number of training epochs for the GNN model
   * - ``BTS_GNN_HIDDEN_DIM``
     - ``256``
     - no
     - Hidden dimension size for the GNN model
   * - ``BTS_GNN_OUTPUT_DIM``
     - ``256``
     - no
     - Output (embedding) dimension of the GNN model
   * - ``BTS_GNN_LEARNING_RATE``
     - ``0.001``
     - no
     - Learning rate for GNN training

CLI Output Settings
-------------------

.. list-table:: CLI Output
   :header-rows: 1
   :widths: 55 15 5 25

   * - Variable
     - Default
     - Sec
     - Description
   * - ``BTS_VERBOSE_PRINT``
     - ``false``
     - no
     - Show verbose output during CLI operations
   * - ``BTS_DISABLE_PROGRESS_BAR``
     - ``false``
     - no
     - Disable progress bars for long-running CLI commands
