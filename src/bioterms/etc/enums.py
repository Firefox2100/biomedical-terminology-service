from enum import Enum, StrEnum


class AnnotationType(Enum):
    """
    The type of annotation.
    """
    EXACT = 'exact'
    BROAD = 'broad'
    NARROW = 'narrow'
    RELATED = 'related'
    HAS_SYMBOL = 'has_symbol'
    ANNOTATED_WITH = 'annotated_with'


class CacheDriverType(Enum):
    """
    The type of cache driver.
    """
    REDIS = 'redis'


class ConceptPrefix(StrEnum):
    """
    The prefix used for concepts in the vocabularies.
    """
    CTV3 = 'ctv3'
    ENSEMBL = 'ensembl'
    HGNC = 'hgnc'
    HGNC_SYMBOL = 'gene'
    HPO = 'hpo'
    NCIT = 'ncit'
    OMIM = 'omim'
    ORDO = 'ordo'
    REACTOME = 'reactome'
    SNOMED = 'snomed'


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
    PATHWAY = 'pathway'
    REACTION = 'reaction'
    GENE = 'gene'
    TRANSCRIPT = 'transcript'
    EXON = 'exon'
    PROTEIN = 'protein'


class ConceptRelationshipType(StrEnum):
    """
    The type of relationship between concepts in the vocabularies.
    """
    IS_A = 'is_a'
    PART_OF = 'part_of'
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


class SimilarityMethod(Enum):
    """
    The method used for calculating similarity between concepts.
    """
    CO_ANNOTATION = 'co-annotation'
    RELEVANCE = 'relevance'


class VectorDatabaseDriverType(Enum):
    """
    The type of vector database driver.
    """
    QDRANT = 'qdrant'
