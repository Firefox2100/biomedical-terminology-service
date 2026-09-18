"""OHDSI Athena projections from OMOP concept IDs to RxNorm identifiers."""

import httpx

from bioterms.annotation.utils import assert_pre_requisite
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist
from bioterms.vocabulary.ohdsi import _process_annotations


ANNOTATION_NAME = 'OHDSI Mapping to RxNorm'
VOCABULARY_PREFIX_1 = ConceptPrefix.OHDSI
VOCABULARY_PREFIX_2 = ConceptPrefix.RXNORM
FILE_PATHS = ['ohdsi/CONCEPT.csv']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """Confirm the manually obtained Athena release contains its concept table."""
    if check_files_exist(FILE_PATHS):
        return
    raise FilesNotFound(
        'OHDSI-to-RxNorm mappings are part of the manually downloaded Athena release.',
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Load OHDSI-published RxNorm identifier projections explicitly."""
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    annotations = _process_annotations(ConceptPrefix.RXNORM)
    if annotations:
        await graph_db.save_annotations(annotations)
