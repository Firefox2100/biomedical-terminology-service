"""UniProtKB-published protein links to OMIM gene and phenotype records."""

import httpx

from bioterms.annotation.utils import assert_pre_requisite, save_annotation_stream
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import verbose_print
from bioterms.vocabulary.uniprot import FILE_PATHS, download_vocabulary, iter_omim_annotations


ANNOTATION_NAME = 'UniProtKB Mapping to OMIM'
VOCABULARY_PREFIX_1 = ConceptPrefix.OMIM
VOCABULARY_PREFIX_2 = ConceptPrefix.UNIPROT


async def download_annotation(download_client: httpx.AsyncClient = None):
    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    count = await save_annotation_stream(graph_db, iter_omim_annotations())
    verbose_print(f'Loaded {count} UniProtKB to OMIM annotations.')
