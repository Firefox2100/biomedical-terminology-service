import os
import aiofiles
import aiofiles.os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, extract_file_from_zip
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_NAME = 'National Cancer Institute Thesaurus'
VOCABULARY_PREFIX = ConceptPrefix.NCIT
ANNOTATIONS = []
SIMILARITY_METHODS = [SimilarityMethod.RELEVANCE]
FILE_PATHS = ['ncit/thesaurus.txt']
TIMESTAMP_FILE = 'ncit/.timestamp'
CONCEPT_CLASS = Concept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the NCIT vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    flat_file_url = 'https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.FLAT.zip'
    zip_path = 'ncit/Thesaurus.FLAT.zip'
    zip_full_path = os.path.join(CONFIG.data_dir, zip_path)

    try:
        await download_file(
            url=flat_file_url,
            file_path=zip_path,
            download_client=download_client,
        )

        await extract_file_from_zip(
            zip_path=zip_full_path,
            file_mapping=[
                ('Thesaurus.txt', os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
            ],
        )
    finally:
        try:
            await aiofiles.os.remove(zip_full_path)
        except Exception:
            pass


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the NCIT vocabulary from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('NCIT flat file not found')

    flat_file_path = str(os.path.join(CONFIG.data_dir, FILE_PATHS[0]))

    ncit_df = pd.read_csv(
        flat_file_path,
        sep='\t',
        names=[
            'code',
            'concept_iri',
            'parents',
            'synonyms',
            'definition',
            'display_name',
            'concept_status',
            'semantic_type',
            'concept_in_subset',
        ],
        dtype=str,
    )

    ncit_graph = nx.DiGraph()
    concepts = []

    for _, row in ncit_df.iterrows():
        synonyms = row['synonyms'].split('|')

        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['code'],
            label=synonyms[0],
            synonyms=synonyms[1:] if len(synonyms) > 1 else None,
            definition=row['definition'] if not pd.isna(row['definition']) else None,
            status=ConceptStatus.DEPRECATED if row['concept_status'] == 'Obsolete_Concept' else ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        ncit_graph.add_node(row['code'])

        if not pd.isna(row['parents']):
            for parent in row['parents'].split('|'):
                ncit_graph.add_edge(
                    row['code'],
                    parent,
                    label=ConceptRelationshipType.IS_A
                )

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.save_terms(
        terms=concepts
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=ncit_graph,
    )
