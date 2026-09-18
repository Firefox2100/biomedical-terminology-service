import httpx

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import batch_iterable, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.vocabulary.uniprot import download_vocabulary, iter_gene_annotations
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'UniProt Mapping to HGNC Gene Symbol'
VOCABULARY_PREFIX_1 = ConceptPrefix.UNIPROT
VOCABULARY_PREFIX_2 = ConceptPrefix.HGNC_SYMBOL
FILE_PATHS = [
    'uniprot/uniprot_sprot.dat.gz',
    'uniprot/uniprot_trembl.dat.gz',
]


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the UniProt release files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    await download_vocabulary(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the UniProt to HGNC Gene Symbol mapping from a file into the primary databases.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    await assert_pre_requisite(
        annotation_name=ANNOTATION_NAME,
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
        file_paths=FILE_PATHS,
        graph_db=graph_db,
    )

    annotation_count = 0
    # This is intentionally a fresh streaming pass over the UniProt release. An explicit
    # annotation load is a request to rebuild the mapping independently of vocabulary load.
    for annotation_batch in batch_iterable(iter_gene_annotations(), batch_size=100000):
        if annotation_batch:
            await graph_db.save_annotations(annotation_batch)
            annotation_count += len(annotation_batch)

    verbose_print(f'Loaded {annotation_count} UniProt to gene-symbol annotations.')
