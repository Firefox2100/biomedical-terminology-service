from enum import Enum, StrEnum


class CacheDriverType(Enum):
    """
    The type of cache driver.
    """
    REDIS = 'redis'


class ConceptPrefix(StrEnum):
    """
    The prefix used for concepts in the vocabularies.
    """
    HPO = 'hpo'


class ConceptStatus(Enum):
    """
    The status of a concept in the vocabularies.
    """
    ACTIVE = 'active'
    DEPRECATED = 'deprecated'


class ConceptType(StrEnum):
    """
    The type of concept represented in the vocabularies.
    """


class ConceptRelationshipType(StrEnum):
    """
    The type of relationship between concepts in the vocabularies.
    """
    IS_A = 'is_a'
    REPLACED_BY = 'replaced_by'


class DocDatabaseDriverType(Enum):
    """
    The type of document database driver.
    """
    MONGO = 'mongo'
    SQLITE = 'sqlite'


class GraphDatabaseDriverType(Enum):
    """
    The type of graph database driver.
    """
    MEMORY = 'memory'
    NEO4J = 'neo4j'


class ServiceEnvironment(Enum):
    """
    The environment the service is running in
    """
    PRODUCTION = 'prod'
    STAGING = 'staging'
    DEVELOPMENT = 'dev'
    TESTING = 'test'
