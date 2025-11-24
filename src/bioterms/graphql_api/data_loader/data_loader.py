from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase
from .concept import ConceptLoader


class DataLoader:
    def __init__(self,
                 doc_db: DocumentDatabase,
                 graph_db: GraphDatabase,
                 ):
        self._doc_db = doc_db
        self._graph_db = graph_db

        self._concept_loaders: dict[ConceptPrefix, ConceptLoader] = {}

    def get_concept_loader(self,
                           prefix: ConceptPrefix,
                           ):
        if prefix not in self._concept_loaders:
            self._concept_loaders[prefix] = ConceptLoader(
                prefix=prefix,
                doc_db=self._doc_db,
            )

        return self._concept_loaders[prefix]
