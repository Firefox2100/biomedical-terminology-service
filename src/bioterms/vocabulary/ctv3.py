import os
import aiofiles
import aiofiles.os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, \
    get_trud_release_url, extract_file_from_zip
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_NAME = 'Clinical Terms Version 3 (Read Codes)'
VOCABULARY_PREFIX = ConceptPrefix.CTV3
ANNOTATIONS = []
SIMILARITY_METHODS = [SimilarityMethod.RELEVANCE]
FILE_PATHS = [
    'ctv3/concept.v3',
    'ctv3/description.v3',
    'ctv3/term.v3',
    'ctv3/hierarchy.v3',
    'ctv3/redundancy.map',
]
TIMESTAMP_FILE = 'ctv3/.timestamp'
CONCEPT_CLASS = Concept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the CTV3 vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    if not CONFIG.nhs_trud_api_key:
        raise ValueError('NHS TRUD API key is required to download CTV3 ontology.')
    api_key = CONFIG.nhs_trud_api_key

    release_url = await get_trud_release_url(
        f'https://isd.digital.nhs.uk/trud/api/v1/keys/{api_key}/items/19/releases?latest'
    )
    zip_path = os.path.join(CONFIG.data_dir, 'ctv3/readctv3.zip')

    try:
        await download_file(
            url=release_url,
            file_path='ctv3/readctv3.zip',
            download_client=download_client,
        )

        await extract_file_from_zip(
            zip_path=zip_path,
            file_mapping=[
                ('V3/Concept.v3', os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
                ('V3/Descrip.v3', os.path.join(CONFIG.data_dir, FILE_PATHS[1])),
                ('V3/Terms.v3', os.path.join(CONFIG.data_dir, FILE_PATHS[2])),
                ('V3/V3hier.v3', os.path.join(CONFIG.data_dir, FILE_PATHS[3])),
                ('V3/Redun.map', os.path.join(CONFIG.data_dir, FILE_PATHS[4])),
            ],
        )
    finally:
        try:
            await aiofiles.os.remove(zip_path)
        except Exception:
            pass


def _parse_term_label(term_30: str,
                      term_60: str,
                      term_198: str,
                      ) -> str:
    """
    Parse the term label from available term fields.
    :param term_30: The 30-character term.
    :param term_60: The 60-character term.
    :param term_198: The 198-character term.
    :return: The selected term label.
    """
    if not pd.isna(term_198):
        return term_198

    if not pd.isna(term_60):
        return term_60

    return term_30


def _load_concepts() -> list[CONCEPT_CLASS]:
    """
    Load concepts from the CTV3 vocabulary files.
    :return: The list of Concept instances.
    """
    concept_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        sep='|',
        header=None,
        names=['concept_id', 'status'],
        usecols=[0, 1],
    ).sort_values('concept_id').reset_index(drop=True)

    description_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[1]),
        sep='|',
        header=None,
        names=['concept_id', 'term_id', 'type'],
        usecols=[0, 1, 2],
    )

    term_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[2]),
        sep='|',
        header=None,
        names=['term_id', 'term_30', 'term_60', 'term_198'],
        usecols=[0, 2, 3, 4],
    )

    merged_term_df = pd.merge(
        description_df,
        term_df,
        on='term_id',
        how='inner',
        validate='many_to_one',
    ).sort_values('concept_id').reset_index(drop=True).groupby('concept_id')

    del description_df
    del term_df

    concept_index = 0
    concept_count = len(concept_df)
    concepts = []

    for concept_id, group in merged_term_df:
        while concept_index < concept_count and concept_df.iloc[concept_index]['concept_id'] != concept_id:
            concept = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX,
                conceptId=concept_df.iloc[concept_index]['concept_id'],
                status=ConceptStatus.DEPRECATED
                    if concept_df.iloc[concept_index]['status'] not in ['C', 'O']
                    else ConceptStatus.ACTIVE,
            )

            concepts.append(concept)
            concept_index += 1

        if concept_index < concept_count and concept_df.iloc[concept_index]['concept_id'] == concept_id:
            label_row = group[group['type'] == 'P']
            synonym_rows = group[group['type'] == 'S']

            if not label_row.empty:
                label = _parse_term_label(
                    term_30=label_row.iloc[0]['term_30'],
                    term_60=label_row.iloc[0]['term_60'],
                    term_198=label_row.iloc[0]['term_198'],
                )
            elif not synonym_rows.empty:
                label = _parse_term_label(
                    term_30=synonym_rows.iloc[0]['term_30'],
                    term_60=synonym_rows.iloc[0]['term_60'],
                    term_198=synonym_rows.iloc[0]['term_198'],
                )
            else:
                # No label or synonym at all, however, the concept ID appeared in the file
                continue

            synonyms = [
                _parse_term_label(
                    term_30=row['term_30'],
                    term_60=row['term_60'],
                    term_198=row['term_198']
                )
                for _, row in synonym_rows.iterrows()
            ]

            concept = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX,
                conceptId=concept_id,
                label=label if not pd.isna(label) and label else None,
                synonyms=synonyms if synonyms else None,
                status=ConceptStatus.DEPRECATED
                    if concept_df.iloc[concept_index]['status'] not in ['C', 'O']
                    else ConceptStatus.ACTIVE,
            )

            concepts.append(concept)
            concept_index += 1

    while concept_index < concept_count:
        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=concept_df.iloc[concept_index]['concept_id'],
            status=ConceptStatus.DEPRECATED
                if concept_df.iloc[concept_index]['status'] not in ['C', 'O']
                else ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        concept_index += 1

    return concepts


def _load_relationships(concepts: list[CONCEPT_CLASS],
                        ) -> nx.DiGraph:
    """
    Load relationships from the CTV3 vocabulary files.
    :param concepts: The list of Concept instances.
    :return: The directed graph of relationships.
    """
    ctv3_graph = nx.DiGraph()

    for concept in concepts:
        ctv3_graph.add_node(concept.concept_id)

    hierarchy_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[3]),
        sep='|',
        header=None,
        names=['child_id', 'parent_id'],
        usecols=[0, 1],
    )

    redundant_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[4]),
        sep='|',
        header=None,
        names=['current_id', 'old_id'],
    )

    for _, row in hierarchy_df.iterrows():
        ctv3_graph.add_edge(
            row['child_id'],
            row['parent_id'],
            label=ConceptRelationshipType.IS_A
        )

    for _, row in redundant_df.iterrows():
        ctv3_graph.add_edge(
            row['old_id'],
            row['current_id'],
            label=ConceptRelationshipType.REPLACED_BY
        )

    return ctv3_graph


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the CTV3 vocabulary from files into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('CTV3 release files not found')

    concepts = _load_concepts()
    ctv3_graph = _load_relationships(concepts=concepts)

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.save_terms(
        terms=concepts
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=ctv3_graph,
    )
