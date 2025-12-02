import os
import logging
import secrets
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Literal
from argon2 import PasswordHasher
from httpx import AsyncClient
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from bioterms.etc.enums import DocDatabaseDriverType, GraphDatabaseDriverType, CacheDriverType, \
    ServiceEnvironment, VectorDatabaseDriverType


SECRETS_DIR = '/run/secrets' if os.path.isdir('/run/secrets') else None


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
        description='Whether this application is behind an HTTPS proxy. This affects cookie settings, '
                    'redirect URLs, and security headers.',
    )
    enable_metrics: bool = Field(
        True,
        description='Enable Prometheus metrics collection and export',
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
    environment: ServiceEnvironment = Field(
        ServiceEnvironment.DEVELOPMENT,
        description='The environment in which the service is running',
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
    sqlite_db_path: str = Field(
        'data/bioterms.db',
        description='File path for the SQLite database',
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

    bioportal_api_key: Optional[str] = Field(
        None,
        description='API key for accessing the BioPortal services',
    )
    nhs_trud_api_key: Optional[str] = Field(
        None,
        description='API key for accessing the NHS TRUD services',
    )

    transformer_model_name: str = Field(
        'BAAI/bge-base-en-v1.5',
        description='Name of the transformer model to use for embeddings',
    )
    torch_device: str = Field(
        'cpu',
        description='Torch device to use for model inference (e.g., "cpu", "cuda")',
    )
    gnn_epochs: int = Field(
        100,
        description='Number of epochs to train the GNN model',
    )
    gnn_hidden_dim: int = Field(
        256,
        description='Hidden dimension size for the GNN model',
    )
    gnn_output_dim: int = Field(
        256,
        description='Output dimension size for the GNN model',
    )
    gnn_learning_rate: float = Field(
        1e-3,
        description='Learning rate for training the GNN model',
    )
    vector_database_driver: VectorDatabaseDriverType = Field(
        VectorDatabaseDriverType.QDRANT,
        description='Vector database driver to use for the service',
    )
    qdrant_location: str = Field(
        'http://localhost:6333',
        description='Location of the Qdrant vector database',
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


EXECUTOR = ProcessPoolExecutor(max_workers=CONFIG.process_limit)
DOWNLOAD_CLIENT = AsyncClient()
QUERY_CLIENT = AsyncClient()

PH = PasswordHasher()
