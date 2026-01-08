"""
Data loader for GraphQL API. These data loaders are used to solve the N+1 query problem
with GraphQL by batching and caching database requests.
"""

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase
from .concept import ConceptLoader
from .reactome import ReactomeLoader


class DataLoader:
    """
    Data loader for GraphQL API. This class provides access to various data loaders for
    different vocabularies and data sources.
    """

    def __init__(self,
                 doc_db: DocumentDatabase,
                 graph_db: GraphDatabase,
                 ):
        """
        Initialize the data loader.
        :param doc_db: The document database instance.
        :param graph_db: The graph database instance.
        """
        self._doc_db = doc_db
        self._graph_db = graph_db

        self._concept_loaders: dict[ConceptPrefix, ConceptLoader] = {}
        self._reactome_loader: ReactomeLoader | None = None

    def get_concept_loader(self,
                           prefix: ConceptPrefix,
                           ):
        """
        Get the concept loader for the specified vocabulary prefix.

        This method caches ConceptLoader instances, because by default the aiodataloader
        caches the loaded results in the loader instance. Maintaining a single instance in
        request session ensures that repeated requests for the same concept ID are served from
        the cache.
        :param prefix: The vocabulary prefix.
        :return: The ConceptLoader instance for the specified prefix.
        """
        if prefix not in self._concept_loaders:
            self._concept_loaders[prefix] = ConceptLoader(
                prefix=prefix,
                graph_db=self._graph_db,
                doc_db=self._doc_db,
            )

        return self._concept_loaders[prefix]

    @property
    def reactome(self) -> ReactomeLoader:
        """
        Get the Reactome data loader. Reactome data loader is more specialised with
        its internal hierarchy and relationships.
        :return: The ReactomeLoader instance.
        """
        if self._reactome_loader is None:
            self._reactome_loader = ReactomeLoader(
                graph_db=self._graph_db,
            )

        return self._reactome_loader
