from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase
from .concept import ConceptLoader
from .reactome import ReactomeLoader


class DataLoader:
    def __init__(self,
                 doc_db: DocumentDatabase,
                 graph_db: GraphDatabase,
                 ):
        self._doc_db = doc_db
        self._graph_db = graph_db

        self._concept_loaders: dict[ConceptPrefix, ConceptLoader] = {}
        self._reactome_loader: ReactomeLoader | None = None

    def get_concept_loader(self,
                           prefix: ConceptPrefix,
                           ):
        if prefix not in self._concept_loaders:
            self._concept_loaders[prefix] = ConceptLoader(
                prefix=prefix,
                graph_db=self._graph_db,
                doc_db=self._doc_db,
            )

        return self._concept_loaders[prefix]

    @property
    def reactome(self) -> ReactomeLoader:
        if self._reactome_loader is None:
            self._reactome_loader = ReactomeLoader(
                graph_db=self._graph_db,
            )

        return self._reactome_loader
