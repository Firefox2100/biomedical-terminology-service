import httpx

from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import batch_iterable, verbose_print
from bioterms.vocabulary.uniprot import download_vocabulary, iter_go_annotations
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'UniProtKB Protein Annotations to Gene Ontology'
VOCABULARY_PREFIX_1 = ConceptPrefix.UNIPROT
VOCABULARY_PREFIX_2 = ConceptPrefix.GO
FILE_PATHS = [
    'uniprot/uniprot_sprot.dat.gz',
    'uniprot/uniprot_trembl.dat.gz',
]


async def download_annotation(download_client: httpx.AsyncClient = None):
    """Reuse the complete UniProtKB release containing GO cross-references."""
    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Re-stream UniProtKB and explicitly load its protein-to-GO annotations."""
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )
    annotation_count = 0
    for annotation_batch in batch_iterable(iter_go_annotations(), batch_size=100000):
        if annotation_batch:
            await graph_db.save_annotations(annotation_batch)
            annotation_count += len(annotation_batch)
    verbose_print(f'Loaded {annotation_count} UniProtKB to GO annotations.')
