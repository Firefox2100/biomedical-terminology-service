import os
import io
import zipfile
import aiofiles
import aiofiles.os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, \
    get_trud_release_url, extract_file_from_zip
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_NAME = 'Reactome Pathways'
VOCABULARY_PREFIX = ConceptPrefix.REACTOME
ANNOTATIONS = []
SIMILARITY_METHODS = []
FILE_PATHS = [
    'reactome/pathway.csv',
    'reactome/reaction.csv',
    'reactome/pathway_to_reaction.csv',
]
CONCEPT_CLASS = Concept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the Reactome vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    pathway_url = 'https://artifactory.cafevariome.org/repository/cv3-bioterms/data/reactome/reactome_pathways.csv'
    reaction_url = 'https://artifactory.cafevariome.org/repository/cv3-bioterms/data/reactome/reactome_reactions.csv'
    mapping_url = 'https://artifactory.cafevariome.org/repository/cv3-bioterms/data/reactome/reactome_pathway_to_reaction.csv'

    await download_file(
        url=pathway_url,
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )
    await download_file(
        url=reaction_url,
        file_path=FILE_PATHS[1],
        download_client=download_client,
    )
    await download_file(
        url=mapping_url,
        file_path=FILE_PATHS[2],
        download_client=download_client,
    )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the Reactome vocabulary from files into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Reactome release files not found')

    pathway_df = pd.read_csv(str(os.path.join(CONFIG.data_dir, FILE_PATHS[0])))
    reaction_df = pd.read_csv(str(os.path.join(CONFIG.data_dir, FILE_PATHS[1])))
    mapping_df = pd.read_csv(str(os.path.join(CONFIG.data_dir, FILE_PATHS[2])))
