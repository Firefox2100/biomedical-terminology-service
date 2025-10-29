import os
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Literal
from httpx import AsyncClient
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from bioterms.etc.enums import DocDatabaseDriverType, GraphDatabaseDriverType, CacheDriverType, \
    ServiceEnvironment


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

    logging_level: Literal['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'] = Field(
        'INFO',
        description='Logging level for the application'
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
    mongodb_username: str = Field(
        'bts_user',
        description='Username for the MongoDB database',
    )
    mongodb_password: str = Field(
        'password',
        description='Password for the MongoDB database',
    )

    graph_database_driver: GraphDatabaseDriverType = Field(
        GraphDatabaseDriverType.NEO4J,
        description='Graph database driver to use for the service',
    )
    neo4j_host: str = Field(
        'localhost',
        description='Host for the Neo4j database',
    )
    neo4j_port: int = Field(
        7687,
        description='Port for the Neo4j database',
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
