import os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, ConceptType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file
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
TIMESTAMP_FILE = 'reactome/.timestamp'
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

    concepts = []
    reactome_graph = nx.DiGraph()

    for _, row in pathway_df.iterrows():
        if pd.notna(row['synonyms']):
            synonyms = row['synonyms'].split('|')
        else:
            synonyms = None

        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['st_id'],
            conceptTypes=[ConceptType.PATHWAY],
            label=row['display_name'] if not pd.isna(row['display_name']) else None,
            synonyms=synonyms,
            status=ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        reactome_graph.add_node(row['st_id'])

    for _, row in reaction_df.iterrows():
        if pd.notna(row['synonyms']):
            synonyms = row['synonyms'].split('|')
        else:
            synonyms = None

        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['st_id'],
            conceptTypes=[ConceptType.REACTION],
            label=row['display_name'] if not pd.isna(row['display_name']) else None,
            synonyms=synonyms,
            status=ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        reactome_graph.add_node(row['st_id'])

    for _, row in mapping_df.iterrows():
        reactome_graph.add_edge(
            row['reaction'],
            row['pathway'],
            label=ConceptRelationshipType.PART_OF,
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
        graph=reactome_graph,
    )
