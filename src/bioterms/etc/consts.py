"""
Constants and configuration settings for the Biomedical Terminology Service.
"""

import os
import logging
import secrets
import importlib.resources as pkg_resources
from pathlib import Path
from typing import Optional, Literal
from argon2 import PasswordHasher
from httpx import AsyncClient, Timeout
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from bioterms.etc.enums import DocDatabaseDriverType, GraphDatabaseDriverType, CacheDriverType, \
    ServiceEnvironment, VectorDatabaseDriverType, QdrantStorageType


SECRETS_DIR = '/run/secrets' if os.path.isdir('/run/secrets') else None
STATIC_FILE_PATH = pkg_resources.files('bioterms.data') / 'static'
DEFAULT_RERANKER_MODEL = (
    Path(__file__).resolve().parents[3] / 'scripts' / 'reranker' / 'runs' / 'production' / 'final'
)


class Settings(BaseSettings):
    """
    Configurations for the Biomedical Terminology Service.
    """

    model_config = SettingsConfigDict(
        env_prefix='BTS_',
        env_file_encoding='utf-8',
        **({'secrets_dir': SECRETS_DIR} if SECRETS_DIR else {})
    )

    process_limit: int = Field(
        4,
        description='Maximum number of worker process to spawn for handling data. '
                    'This is not used when running as a service, only for CLI commands.',
    )
    auto_complete_min_length: int = Field(
        3,
        description='Minimum length of query string for auto-complete searches',
    )

    logging_level: Literal['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'] = Field(
        'INFO',
        description='Logging level for the application'
    )
    secret_key: str = Field(
        default_factory=secrets.token_urlsafe,
        description='Secret key for the application',
    )
    server_hmac_key: str = Field(
        ...,
        description='HMAC key for hashing API keys',
    )
    use_https: bool = Field(
        False,
        description='Whether this application is behind an HTTPS proxy. This affects cookie '
                    'settings, redirect URLs, and security headers.',
    )
    enable_metrics: bool = Field(
        True,
        description='Enable Prometheus metrics collection and export',
    )
    enable_error_reporting: bool = Field(
        False,
        description='Enable error reporting to Sentry or other compatible tracking services',
    )
    enable_profiling: bool = Field(
        False,
        description='Enable performance profiling using Sentry SDK'
    )
    sentry_dsn: Optional[str] = Field(
        None,
        description='DSN for Sentry error reporting service',
    )
    google_site_verification_id: Optional[str] = Field(
        None,
        description='Google site verification ID for webmaster tools. Set this to inject '
                    'the verification meta tag into HTML pages.',
    )
    data_dir: str = Field(
        'data',
        description='Directory for storing data files',
    )
    service_root_path: str = Field(
        '',
        description='Root path for the service, used for reverse proxy setups',
    )
    openapi_url: Optional[str] = Field(
        '/openapi.json',
        description='URL path for the OpenAPI schema',
    )
    docs_url: Optional[str] = Field(
        '/docs',
        description='URL path for the Swagger UI documentation',
    )
    redoc_url: Optional[str] = Field(
        '/redoc',
        description='URL path for the ReDoc documentation',
    )
    fhir_canonical_url: str = Field(
        'https://your.deployment.com/fhir',
        description='Canonical URL for FHIR resources',
    )
    environment: ServiceEnvironment = Field(
        ServiceEnvironment.DEVELOPMENT,
        description='The environment in which the service is running',
    )
    celery_broker: str = Field(
        'redis://localhost:6379/1',
        description='Celery backend URL for task result storage',
    )
    celery_backend: str = Field(
        'redis://localhost:6379/2',
        description='Celery broker URL for task messaging',
    )

    doc_database_driver: DocDatabaseDriverType = Field(
        DocDatabaseDriverType.MONGO,
        description='Document database driver to use for the service',
    )
    mongodb_host: str = Field(
        'localhost',
        description='Host for the MongoDB database',
    )
    mongodb_port: int = Field(
        27017,
        description='Port for the MongoDB database',
    )
    mongodb_db_name: str = Field(
        'bts',
        description='Name of the MongoDB database to use',
    )
    mongodb_username: Optional[str] = Field(
        None,
        description='Username for the MongoDB database',
    )
    mongodb_password: Optional[str] = Field(
        None,
        description='Password for the MongoDB database',
    )
    mongodb_auth_source: str = Field(
        'admin',
        description='Authentication source database for MongoDB',
    )
    mongodb_direct_connection: bool = Field(
        False,
        description='Directly connect to the specified domain, ignoring the replica set discovery. '
                    'This may be necessary if using a docker internal domain for the set.'
    )
    mongodb_text_index_name: str = Field(
        'text_autocomplete_index',
        description='Name of the MongoDB Atlas/mongot `$search` autocomplete index created on '
                    'the "conceptId"/"label"/"synonyms" fields of each vocabulary collection, '
                    'used by auto-complete search when BTS_DOC_DATABASE_DRIVER=mongo and the '
                    'connected deployment has Atlas Search/mongot support (e.g. the '
                    '`mongodb-search` compose profile). Falls back to the legacy "nGrams" '
                    'field/index automatically when unsupported.',
    )
    elasticsearch_url: str = Field(
        'http://localhost:9200',
        description='Elasticsearch URL used by both document and vector drivers.',
    )
    elasticsearch_username: Optional[str] = Field(
        None, description='Optional Elasticsearch basic-auth username.',
    )
    elasticsearch_password: Optional[str] = Field(
        None, description='Optional Elasticsearch basic-auth password.',
    )
    elasticsearch_api_key: Optional[str] = Field(
        None, description='Optional Elasticsearch API key; takes precedence over basic auth.',
    )
    elasticsearch_index_prefix: str = Field(
        'bts', description='Prefix for all Elasticsearch indices created by this service.',
    )
    elasticsearch_batch_size: int = Field(
        1000, description='Bulk indexing chunk size for Elasticsearch drivers.',
    )
    sql_db_url: str = Field(
        'sqlite+aiosqlite:///./bts.sqlite3',
        description='Database URL for the SQL database',
    )
    sql_batch_size: int = Field(
        5000,
        description='Batch size for SQL database operations',
    )
    sql_pool_size: int = Field(
        20,
        description='Base number of pooled SQL connections kept open (SQLAlchemy default is '
                    '5, which a concurrent batch workload -- e.g. scripts/reranker/'
                    'build_training_data.py -- can exhaust well before the app itself is '
                    'under any real load, raising a pool-checkout TimeoutError).',
    )
    sql_max_overflow: int = Field(
        20,
        description='Extra connections allowed beyond `sql_pool_size` under burst load '
                    '(SQLAlchemy default is 10). Total concurrent connections this driver '
                    'will open is sql_pool_size + sql_max_overflow.',
    )

    graph_database_driver: GraphDatabaseDriverType = Field(
        GraphDatabaseDriverType.NEO4J,
        description='Graph database driver to use for the service',
    )
    neo4j_uri: str = Field(
        'neo4j://localhost:7687',
        description='Connection URI for the Neo4j database',
    )
    neo4j_db_name: str = Field(
        'neo4j',
        description='Name of the Neo4j database to use',
    )
    neo4j_username: str = Field(
        'neo4j',
        description='Username for the Neo4j database',
    )
    neo4j_password: str = Field(
        'password',
        description='Password for the Neo4j database',
    )
    neo4j_delete_batch_size: int = Field(
        2000,
        description='Number of rows (relationships/nodes) committed per transaction when '
                    'batch-deleting from Neo4j. Lower values reduce peak transaction memory '
                    'usage at the cost of speed, which matters for large vocabularies on '
                    'memory-constrained Neo4j instances.',
    )
    postgres_graph_db_url: str = Field(
        'postgresql+asyncpg://localhost:5432/bts',
        description='SQLAlchemy async URL for the PostgreSQL graph database, used when '
                    'BTS_GRAPH_DATABASE_DRIVER=postgresql. Graph tables live under their own '
                    '"graph_*" names, so this can safely be the same database as BTS_SQL_DB_URL '
                    'and/or BTS_POSTGRES_VECTOR_DB_URL to run the document, vector, and graph '
                    'stores on one PostgreSQL instance.',
    )
    postgres_graph_closure_depth: int = Field(
        5,
        description='How many BFS layers of each vocabulary\'s ancestor/descendant closure to '
                    'materialise into `graph_closure_<prefix>` (see build-database.rst). Queries '
                    'requesting at most this many hops are served from the indexed closure table; '
                    'deeper or unbounded queries fall back to a live, per-query bounded traversal '
                    'over `graph_edge_<prefix>` instead. Lower values trade slower deep/unbounded '
                    'lookups for a much smaller closure table -- this matters most on densely '
                    'polyhierarchical vocabularies (e.g. OHDSI), where materialising the full '
                    'closure can run into the hundreds of GB.',
    )
    postgres_graph_closure_max_depth: int = Field(
        500,
        description='Hard safety ceiling on recursion depth, applied both when materialising a '
                    'closure table (in case `postgres_graph_closure_depth` is set unreasonably '
                    'high) and to the live fallback traversal used for unbounded queries beyond '
                    'that depth. Guards against runaway recursion on a malformed/cyclic hierarchy; '
                    'real ontologies are far shallower than this.',
    )

    cache_driver: CacheDriverType = Field(
        CacheDriverType.REDIS,
        description='Cache driver to use for the service',
    )
    redis_host: str = Field(
        'localhost',
        description='Redis host for the cache',
    )
    redis_port: int = Field(
        6379,
        description='Redis port for the cache',
    )
    redis_db: int = Field(
        0,
        description='Redis database index for the cache',
    )
    redis_password: Optional[str] = Field(
        None,
        description='Password for the Redis cache, if authentication is required',
    )
    cache_hard_ttl_multiplier: int = Field(
        7,
        description='Multiplier applied to cache item TTLs for Redis hard expiration. '
                    'The item TTL is treated as the stale-after time; Redis expiration is '
                    'only used as a safety limit.',
    )
    cache_rebuild_lock_ttl: int = Field(
        3600,
        description='Time in seconds to hold the cache rebuild single-flight lock.',
    )

    bioportal_api_key: Optional[str] = Field(
        None,
        description='API key for accessing the BioPortal services',
    )
    nhs_trud_api_key: Optional[str] = Field(
        None,
        description='API key for accessing the NHS TRUD services',
    )
    nih_umls_api_key: Optional[str] = Field(
        None,
        description='API key for accessing the NIH UMLS services',
    )
    loinc_username: Optional[str] = Field(
        None,
        description='Username for the authenticated LOINC release API',
    )
    loinc_password: Optional[str] = Field(
        None,
        description='Password for the authenticated LOINC release API',
    )

    transformer_model_name: str = Field(
        'FremyCompany/BioLORD-2023',
        description='Name of the transformer model to use for embeddings',
    )
    embedding_process_limit: int = Field(
        1,
        description='Number of worker processes used for embedding generation. '
                    'Set to 1 to disable multiprocessing.',
    )
    embedding_batch_size: int = Field(
        128,
        description='Batch size used when generating concept embeddings. 128 saturates a '
                    'typical GPU\'s per-call overhead for short label/synonym strings; '
                    'benchmarked at ~5x the throughput of a batch size of 32 with no further '
                    'gain up to 512. Lower this on CPU-only or memory-constrained deployments.',
    )
    torch_device: str = Field(
        'cpu',
        description='Torch device to use for model inference (e.g., "cpu", "cuda")',
    )
    search_rrf_k: int = Field(
        60,
        description='The "k" constant used when fusing the lexical, alias-embedding, and '
                    'definition-embedding recall lists with Reciprocal Rank Fusion in '
                    'GET search V1/V2 (and the equivalent GraphQL/MCP search paths). Higher '
                    'values flatten the influence of rank position across the three lists.',
    )
    reranker_model: Optional[str] = Field(
        str(DEFAULT_RERANKER_MODEL) if (DEFAULT_RERANKER_MODEL / 'modules.json').is_file() else None,
        description='Optional ColBERT reranker bundle. May be a local checkpoint directory '
                    'or a Hugging Face repository ID. Defaults to the local production/final '
                    'bundle when it exists; when unset, search returns RRF order.',
    )
    search_retrieval_candidate_limit: int = Field(
        10,
        ge=1,
        description='Minimum candidates requested from each lexical/semantic recall path '
                    'before fusion. This is independent of the API result limit and the '
                    'number of fused candidates passed to the reranker.',
    )
    search_vector_overretrieve_factor: float = Field(
        1.0,
        ge=1.0,
        description='Multiplier applied to alias/definition vector recall depth before '
                    'fusion. Increase this when quantized vector storage trades precision '
                    'for space and may otherwise omit useful candidates.',
    )
    search_mapped_recall_limit: int = Field(
        0,
        ge=0,
        description='Optional cross-vocabulary recall depth. When positive, inspect the top '
                    'N lexical hits in vocabularies connected to the requested vocabulary by '
                    'EXACT annotations, retain exact source ID/label/synonym matches, map them '
                    'into the requested vocabulary, and add the mapped concepts as an '
                    'unpinned RRF arm. Zero disables mapped recall.',
    )
    search_mapped_candidate_limit: int = Field(
        20,
        ge=1,
        description='Maximum distinct EXACT-mapped concepts contributed per requested '
                    'vocabulary when cross-vocabulary mapped recall is enabled.',
    )
    reranker_candidate_limit: int = Field(
        50,
        ge=1,
        description='Maximum non-exact RRF candidates scored by the configured reranker.',
    )
    reranker_batch_size: int = Field(
        32,
        ge=1,
        description='Encoding batch size used by the ColBERT search reranker.',
    )
    reranker_query_length: int = Field(
        32,
        ge=1,
        description='Maximum query token length expected by the reranker bundle.',
    )
    reranker_document_length: int = Field(
        64,
        ge=1,
        description='Maximum rendered-candidate token length expected by the reranker bundle.',
    )
    reranker_max_aliases: int = Field(
        6,
        ge=0,
        description='Maximum synonyms rendered per reranker candidate; zero disables the cap.',
    )
    vector_database_driver: VectorDatabaseDriverType = Field(
        VectorDatabaseDriverType.QDRANT,
        description='Vector database driver to use for the service',
    )
    qdrant_location: str = Field(
        'http://localhost:6333',
        description='Location of the Qdrant vector database',
    )
    qdrant_storage_type: QdrantStorageType = Field(
        QdrantStorageType.FLOAT32,
        description='The vector storage datatype for newly created Qdrant collections, when '
                    'BTS_VECTOR_DATABASE_DRIVER=qdrant. This is the actual on-disk/in-memory '
                    'format vectors are written in -- not a quantization index built alongside '
                    'a full-precision copy. "float32" is full precision (the default); '
                    '"float16" and "uint8" are smaller, lower-fidelity storage formats; '
                    '"turbo4" stores vectors directly in TurboQuant 4-bit form. Each step down '
                    'in precision reduces storage cost at some further loss of recall accuracy.',
    )
    mongodb_vector_index_name: str = Field(
        'vector_index',
        description='Name of the MongoDB Atlas/mongot vector search index created on the '
                    '"vector" field of each vocabulary\'s "<prefix>.vectors" embedding-item '
                    'collection, when BTS_VECTOR_DATABASE_DRIVER=mongodb. The index is '
                    'filterable on "kind" so alias and definition items can be searched '
                    'separately.',
    )
    mongodb_vector_num_candidates_multiplier: int = Field(
        10,
        description='Multiplier applied to the requested result limit to determine the '
                    'numCandidates parameter of $vectorSearch queries, when '
                    'BTS_VECTOR_DATABASE_DRIVER=mongodb.',
    )
    postgres_vector_db_url: str = Field(
        'postgresql+asyncpg://localhost:5432/bts',
        description='SQLAlchemy async URL for the PostgreSQL/pgvector vector database, used '
                    'when BTS_VECTOR_DATABASE_DRIVER=postgresql. This may safely equal '
                    'BTS_SQL_DB_URL to run the document and vector stores on one PostgreSQL '
                    'instance -- embedding items still live in their own '
                    '"concept_<prefix>_vector_item" tables, so this works whether or not the '
                    'document database is also PostgreSQL.',
    )
    elasticsearch_vector_num_candidates_multiplier: int = Field(
        10,
        description='Multiplier used to derive num_candidates for Elasticsearch kNN search.',
    )

    verbose_print: bool = Field(
        False,
        description='Enable verbose printing for CLI operations',
    )
    disable_progress_bar: bool = Field(
        False,
        description='Disable progress bars for operations',
    )

    download_connect_timeout_seconds: float = Field(
        30.0,
        description='Connect/write/pool timeout for DOWNLOAD_CLIENT (large vocabulary file '
                    'downloads, e.g. UniProt\'s 100GB+ TrEMBL release). Kept short relative to '
                    'the read timeout below so an unreachable host fails fast.',
    )
    download_read_timeout_seconds: float = Field(
        120.0,
        description='Per-chunk read timeout for DOWNLOAD_CLIENT. httpx\'s own default is 5 '
                    'seconds, which is too aggressive for a multi-GB/multi-hour streamed '
                    'download -- ordinary network jitter or brief server-side pacing on a '
                    'single chunk read was enough to abort the whole transfer '
                    '(httpx.ReadTimeout). This is deliberately more generous, not unbounded.',
    )
    download_max_retries: int = Field(
        5,
        description='Maximum attempts for download_file() before giving up. Each retry after '
                    'the first resumes via an HTTP Range request from the partially-downloaded '
                    'file already on disk (see download_file docstring), rather than '
                    'restarting from byte zero -- important at UniProt scale, where restarting '
                    'a 100GB+ download from scratch on every transient network blip would be '
                    'impractical.',
    )
    download_retry_backoff_seconds: float = Field(
        5.0,
        description='Base backoff between download_file() retries; multiplied by the attempt '
                    'number (5s, 10s, 15s, ...).',
    )


CONFIG = Settings(_env_file=os.getenv('BTS_ENV_FILE', 'conf/.env'))     # type: ignore
LOGGER = logging.getLogger('bioterms')
LOGGER.setLevel(CONFIG.logging_level.upper())   # pylint: disable=no-member

if not LOGGER.hasHandlers():
    console_handler = logging.StreamHandler()
    console_handler.setLevel(CONFIG.logging_level.upper())      # pylint: disable=no-member

    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(process)d] [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z'
    )
    console_handler.setFormatter(formatter)

    LOGGER.addHandler(console_handler)


DOWNLOAD_CLIENT = AsyncClient(
    timeout=Timeout(
        connect=CONFIG.download_connect_timeout_seconds,
        read=CONFIG.download_read_timeout_seconds,
        write=CONFIG.download_connect_timeout_seconds,
        pool=CONFIG.download_connect_timeout_seconds,
    ),
)
QUERY_CLIENT = AsyncClient()

PH = PasswordHasher()
