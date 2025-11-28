import os
import gzip
import aiofiles
import aiofiles.os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_NAME = 'Online Mendelian Inheritance in Man'
VOCABULARY_PREFIX = ConceptPrefix.OMIM
ANNOTATIONS = []
SIMILARITY_METHODS = [SimilarityMethod.RELEVANCE]
FILE_PATHS = ['omim/omim.csv']
CONCEPT_CLASS = Concept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the OMIM vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    csv_url = 'https://data.bioontology.org/ontologies/OMIM/download?download_format=csv'
    csv_path = 'omim/omim.gz'

    if not CONFIG.bioportal_api_key:
        raise ValueError('BioPortal API key is required to download OMIM ontology.')

    await download_file(
        url=csv_url,
        file_path=csv_path,
        headers={'Authorization': f'apikey token={CONFIG.bioportal_api_key}'},
        download_client=download_client,
    )

    gz_full_path = os.path.join(CONFIG.data_dir, csv_path)
    out_path = os.path.join(CONFIG.data_dir, FILE_PATHS[0])

    # Read the gz file asynchronously, decompress in memory, then write output asynchronously
    async with aiofiles.open(gz_full_path, 'rb') as f_in:
        gz_data = await f_in.read()
    decompressed = gzip.decompress(gz_data)
    async with aiofiles.open(out_path, 'wb') as f_out:
        await f_out.write(decompressed)

    try:
        await aiofiles.os.remove(gz_full_path)
    except Exception:
        pass


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the OMIM vocabulary from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('OMIM CSV file not found')

    csv_path = str(os.path.join(CONFIG.data_dir, FILE_PATHS[0]))

    omim_df = pd.read_csv(
        csv_path,
    )

    omim_graph = nx.DiGraph()
    concepts = []

    for _, row in omim_df.iterrows():
        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptTypes=[],
            conceptId=row['Class ID'].split('/')[-1],
            label=row['Preferred Label'] if not pd.isna(row['Preferred Label']) else None,
            synonyms=row['Synonyms'].split('|') if not pd.isna(row['Synonyms']) else None,
            status=ConceptStatus.DEPRECATED if not pd.isna(row['Obsolete']) and bool(row['Obsolete']) else ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        omim_graph.add_node(concept.concept_id)

        if not pd.isna(row['Parents']):
            for parent in row['Parents'].split('|'):
                omim_graph.add_edge(
                    concept.concept_id,
                    parent,
                    label=ConceptRelationshipType.IS_A
                )

        if not pd.isna(row['Moved from']):
            omim_graph.add_edge(
                row['Moved from'],
                concept.concept_id,
                label=ConceptRelationshipType.REPLACED_BY
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
        graph=omim_graph,
    )
