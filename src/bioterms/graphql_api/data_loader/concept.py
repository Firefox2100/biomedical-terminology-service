from typing import Optional
from aiodataloader import DataLoader

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase


class ConceptLoaderById(DataLoader[str, Optional[dict]]):
    def __init__(self,
                 prefix: ConceptPrefix,
                 doc_db: DocumentDatabase,
                 ):
        super().__init__()

        self._prefix = prefix
        self._doc_db = doc_db

    async def batch_load_fn(self,
                            concept_ids: list[str]
                            ) -> list[Optional[dict]]:
        concepts = await self._doc_db.get_terms_by_ids(
            prefix=self._prefix,
            concept_ids=concept_ids,
        )

        concept_map = {concept.concept_id: concept.model_dump() for concept in concepts}
        sorted_concepts = [concept_map.get(concept_id) for concept_id in concept_ids]

        return sorted_concepts


class ConceptLoader:
    def __init__(self,
                 prefix: ConceptPrefix,
                 doc_db: DocumentDatabase,
                 ):
        self._prefix = prefix
        self._doc_db = doc_db

        self._id_loader = None

    @property
    def id(self) -> ConceptLoaderById:
        if self._id_loader is None:
            self._id_loader = ConceptLoaderById(
                prefix=self._prefix,
                doc_db=self._doc_db,
            )

        return self._id_loader
