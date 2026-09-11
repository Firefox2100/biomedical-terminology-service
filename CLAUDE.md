# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

The BioMedical Terminology Service (BTS) is a self-hosted platform providing a unified API (REST, GraphQL, FHIR, MCP) for searching, traversing, mapping, and semantically comparing concepts across biomedical terminologies (SNOMED CT, CTV3, HGNC, HPO, MONDO, NCIt, OMIM, ORDO, Reactome, Ensembl, OHDSI, UniProt). Backend is Python (FastAPI + Celery); `term-browser/` is a React/Vite frontend.

## Common commands

### Python (root)

```bash
pip install -e ".[test]"          # install with test/dev deps (add extras like ",postgres" as needed)
pytest                             # run the full test suite (testpaths = tests, per pyproject.toml)
pytest tests/unit_test/vocabulary  # run a subset
pytest tests/unit_test/test_offline_no_db_dependency.py::test_name  # run a single test
pylint src/bioterms                # lint (pylint is a test extra, no config file committed)
bioterms-cli --help                 # CLI entrypoint (vocabulary/annotation/similarity/user/cache management)
```

- `tests/unit_test` and `tests/function_test` run offline / against fakes and don't need external services (see `tests/conftest.py`, which sets `BTS_SERVER_HMAC_KEY` and disables metrics).
- `tests/integration_test` contains helper "container check" modules (not standalone pytest files) used by tests that need real Mongo/Postgres/Neo4j via `testcontainers`; Docker must be available to exercise these paths.
- `tests/load_test` holds Locust-based load tests (`[tool.locust] locustfile = "tests/load_test/users/"`) and standalone adaptive statistics scripts runnable via `python -m load_test.statistics.<name>` (see `tests/load_test/statistics/README.md`) — these hit a running server and are not part of the default `pytest` run.

### Running the service locally

```bash
docker compose -f docker-compose.yaml -f scripts/docker-compose.dependencies.yaml up   # app + all backing stores
uvicorn bioterms.asgi:application --reload   # run the API directly (needs config + running dependencies)
celery -A bioterms.task.app.celery_app worker   # run the Celery worker (cache purge/rebuild tasks)
```

Configuration is env-var driven (`BTS_*` prefix, see `example.env` and `bioterms.etc.consts.CONFIG`, a pydantic-settings model). Copy `example.env` to `conf/.env` for local runs. Database backends (document/graph/vector/cache) are each selected independently via `BTS_*_DATABASE_DRIVER` / `BTS_CACHE_DRIVER`.

### Frontend (`term-browser/`)

```bash
cd term-browser
npm install
npm run dev       # Vite dev server
npm run build
npm run lint       # eslint
```

## Architecture

### Pluggable storage layer

The service is deliberately decoupled from any single database vendor. Four storage roles are each abstracted behind an interface in `src/bioterms/database/`, with the concrete implementation chosen at runtime from config:

- `doc_db/` (`DocumentDatabase`) — term details/metadata. Drivers: MongoDB (`mongo_doc_db.py`) or SQL (`sql_doc_db.py`, via SQLAlchemy/asyncpg/aiosqlite).
- `graph_db/` (`GraphDatabase`) — relationships between terms (parent/child, cross-vocabulary mappings). Drivers: Neo4j or PostgreSQL.
- `vector_db/` (`VectorDatabase`) — embeddings for semantic search/similarity. Drivers: Qdrant, MongoDB (Atlas Search/mongot), or PostgreSQL (pgvector).
- `cache/` (`Cache`) — Redis-backed response/computation cache.

Each module exposes a `get_active_*()` factory (see e.g. `doc_db/doc_db.py`) that lazily imports and instantiates the driver named by the corresponding `BTS_*_DRIVER` setting, and memoizes a singleton instance. When adding a new backend for one of these roles, implement the abstract interface and register it in that factory's dispatch — don't couple call sites to a specific driver.

Because backends are independently selectable, some combinations share the same physical database (e.g. Postgres can back the doc, vector, and graph stores at once via `BTS_SQL_DB_URL` / `BTS_POSTGRES_VECTOR_DB_URL` / `BTS_POSTGRES_GRAPH_DB_URL`) — the graph tables namespace themselves (`graph_*`) to avoid collisions.

### Vocabulary plugin system

Each supported terminology is a self-contained module in `src/bioterms/vocabulary/` (e.g. `snomed.py`, `hgnc.py`, `ensembl.py`, `uniprot.py`, `reactome.py`, `ohdsi.py`, `mondo.py`, ...). A vocabulary module defines module-level constants consumed generically by `vocabulary/__init__.py` and `vocabulary/utils.py`: `VOCABULARY_NAME`, `VOCABULARY_PREFIX`, `ANNOTATIONS`, `SIMILARITY_METHODS`, `FILE_PATHS`, `CONCEPT_CLASS`, plus import/load functions the framework calls by convention (with fallback defaults, e.g. file deletion). `ConceptPrefix` (in `etc/enums.py`) is the canonical enum of supported vocabularies; `vocabulary/utils.py::get_vocabulary_module` maps a prefix to its module dynamically via `importlib`. Concept model classes for vocabularies with bespoke fields live in `src/bioterms/model/concept/` (e.g. `snomed.py`, `hgnc.py`, `ensembl.py`, `ohdsi.py`, `reactome.py`, `uniprot.py`), subclassing the base `Concept`.

Adding a new vocabulary means: a new `vocabulary/<name>.py` module with the required constants/functions, optionally a matching `model/concept/<name>.py` if it needs custom fields, and registration in `ConceptPrefix`/`ALL_VOCABULARIES`.

### Similarity methods

Analogous plugin pattern in `src/bioterms/similarity/`: `SimilarityMethod` enum values map to modules (`co_annotation.py`, `relevance.py`, `relevance_weight.py`, plus `gnn.py` for graph-neural-network-based embedding) via `ALL_SIMILARITY_METHODS` in `similarity/__init__.py`, dispatched through `get_similarity_module()`. Similarity computation reads graph/annotation data (built from the graph DB and vocabulary annotation files) and writes vectors to the vector DB.

### API surface and app composition

`src/bioterms/app.py` builds the FastAPI app via a factory (`asgi.py` is the ASGI entrypoint used by uvicorn/Docker). It mounts:

- REST routers from `src/bioterms/router/` (`auto_complete`, `data`, `expand`, `map`, `search`, `similarity`, `trace`, `fhir`, `misc`, `ui`), each corresponding to a distinct capability rather than a single vocabulary or backend.
- A GraphQL app (`graphql_api/`, built with Ariadne; schema/resolvers under `resolver/`, batching via `data_loader/`) mounted alongside REST.
- An MCP server (`mcp_api/`, via `fastmcp`) exposed for LLM/agent clients; its lifespan is combined with the FastAPI app's lifespan.
- A FHIR-compatible terminology interface (`router/fhir.py`, using `fhir.resources`) for `CodeSystem` lookup/validation.

Lifespan startup/shutdown wires up the active cache/doc/graph DB singletons and initializes the GraphQL service; shutdown closes them in turn (see `lifespan()` in `app.py`).

### Async and background work

Most I/O-bound code (DB access, HTTP calls, file I/O) is async (`aiofiles`, async DB drivers). Longer-running maintenance work (cache purge/rebuild) is offloaded to Celery (`src/bioterms/task/`), which bridges into the async database layer via `run_async` (`task/utils.py`) since Celery workers run sync.

### CLI

`src/bioterms/cli/cli.py` builds a Typer app composed of sub-apps per domain (`annotation`, `cache`, `similarity`, `user`, `vocabulary`), each in its own module under `src/bioterms/cli/`. This is the primary tool for building/maintaining the terminology database (importing vocabulary source files, computing annotations/similarity, managing cache and admin users) — see `docs/source/build-database.rst` for the data-loading workflow this CLI drives.

## Notes

- The project targets Python 3.11+; dependency versions are pinned with `~=` in `pyproject.toml` — match that convention when adding dependencies.
- Full docs (installation, database build, per-vocabulary notes, API guides, similarity math) live on Read the Docs, sourced from `docs/source/`.
- The service does not ship terminology data itself; users must obtain vocabulary source files separately (see README licensing/disclaimer section) before running the CLI import commands.
