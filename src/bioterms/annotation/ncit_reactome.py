import httpx

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import verbose_print
from bioterms.vocabulary.reactome import build_reference_annotations, download_vocabulary
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'Reactome ReferenceEntity Mapping to NCIt'
VOCABULARY_PREFIX_1 = ConceptPrefix.REACTOME
VOCABULARY_PREFIX_2 = ConceptPrefix.NCIT
FILE_PATHS = ['reactome/ncit_mapping.csv']


async def download_annotation(download_client: httpx.AsyncClient = None):
    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    annotations = build_reference_annotations(ConceptPrefix.NCIT)
    verbose_print(f'Processed {len(annotations)} Reactome to NCIt annotations. Saving...')
    await graph_db.save_annotations(annotations)
