import os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix, ConceptStatus, ConceptRelationshipType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.annotation import Annotation
from bioterms.model.concept import GeneConcept


VOCABULARY_NAME = 'HUGO Gene Nomenclature Committee'
VOCABULARY_PREFIX = ConceptPrefix.HGNC
ANNOTATIONS = []
SIMILARITY_METHODS = []
FILE_PATHS = [
    'hgnc/symbol.txt',
    'hgnc/withdrawn.txt',
]
CONCEPT_CLASS = GeneConcept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the HGNC vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    symbol_url = 'https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt'
    withdrawn_url = 'https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/withdrawn.txt'

    await download_file(
        url=symbol_url,
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )
    await download_file(
        url=withdrawn_url,
        file_path=FILE_PATHS[1],
        download_client=download_client,
    )


def delete_vocabulary_files():
    """
    Delete the HGNC vocabulary files.
    """
    for file_path in FILE_PATHS:
        try:
            os.remove(os.path.join(CONFIG.data_dir, file_path))
        except Exception:
            pass


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the HGNC vocabulary from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('HGNC release files not found')

    symbol_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
        sep='\t',
        dtype=str,
    )
    withdrawn_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[1])),
        sep='\t',
        dtype={
            'WITHDRAWN_SYMBOL': str,
        },
        keep_default_na=False,
    )
    withdrawn_df = withdrawn_df.drop(withdrawn_df[withdrawn_df['STATUS'] == 'Entry Withdrawn'].index)

    concepts = []
    annotations = []
    hgnc_graph = nx.DiGraph()

    for _, row in symbol_df.iterrows():
        synonyms = []
        if row['alias_symbol'] and pd.notna(row['alias_symbol']):
            alias_symbols = row['alias_symbol'].split('|')

            synonyms.extend(alias_symbols)

            for alias_symbol in alias_symbols:
                annotations.append(Annotation(
                    prefixFrom=VOCABULARY_PREFIX,
                    prefixTo=ConceptPrefix.HGNC_SYMBOL,
                    conceptIdFrom=row['hgnc_id'],
                    conceptIdTo=alias_symbol,
                    annotationType=AnnotationType.HAS_SYMBOL,
                ))

        if row['alias_name'] and pd.notna(row['alias_name']):
            synonyms.extend(row['alias_name'].split('|'))

        if not pd.isna(row['location_sortable']):
            location = row['location_sortable']
        elif not pd.isna(row['location']):
            location = row['location']
        else:
            location = None

        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['hgnc_id'].split(':')[1],
            label=row['symbol'] if pd.notna(row['symbol']) else None,
            synonyms=synonyms if synonyms else None,
            definition=row['name'] if pd.notna(row['name']) else None,
            location=location,
            status=ConceptStatus.ACTIVE if row['status'] == 'Approved' else ConceptStatus.DEPRECATED,
        )

        concepts.append(concept)
        hgnc_graph.add_node(concept.concept_id)
        annotations.append(Annotation(
            prefixFrom=VOCABULARY_PREFIX,
            prefixTo=ConceptPrefix.HGNC_SYMBOL,
            conceptIdFrom=row['hgnc_id'],
            conceptIdTo=row['symbol'],
            annotationType=AnnotationType.HAS_SYMBOL,
        ))

    for _, row in withdrawn_df.iterrows():
        replacing_symbols = row['MERGED_INTO_REPORT(S) (i.e HGNC_ID|SYMBOL|STATUS)'].split(', ')
        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['HGNC_ID'].split(':')[1],
            label=row['WITHDRAWN_SYMBOL'],
            status=ConceptStatus.DEPRECATED,
        )

        for symbol in replacing_symbols:
            hgnc_graph.add_edge(
                concept.concept_id,
                symbol.split('|')[0].split(':')[1],
                label=ConceptRelationshipType.REPLACED_BY,
            )
            annotations.append(Annotation(
                prefixFrom=VOCABULARY_PREFIX,
                prefixTo=VOCABULARY_PREFIX,
                conceptIdFrom=row['HGNC_ID'],
                conceptIdTo=row['WITHDRAWN_SYMBOL'],
                annotationType=AnnotationType.HAS_SYMBOL,
            ))

        concepts.append(concept)
        hgnc_graph.add_node(concept.concept_id)

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.save_terms(
        terms=concepts
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=hgnc_graph,
    )
    await graph_db.save_annotations(annotations)


async def create_indexes(overwrite: bool = False,
                         doc_db: DocumentDatabase = None,
                         graph_db: GraphDatabase = None,
                         ):
    """
    Create indexes for the HGNC vocabulary in the primary databases.
    :param overwrite: Whether to overwrite existing indexes.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.create_index(
        prefix=VOCABULARY_PREFIX,
        field='conceptId',
        unique=True,
        overwrite=overwrite,
    )
    await doc_db.create_index(
        prefix=VOCABULARY_PREFIX,
        field='label',
        overwrite=overwrite,
    )

    await graph_db.create_index()


async def delete_vocabulary_data(doc_db: DocumentDatabase = None,
                                 graph_db: GraphDatabase = None,
                                 ):
    """
    Delete all HGNC vocabulary data from the primary databases.
    """
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.delete_all_for_label(VOCABULARY_PREFIX)
    await graph_db.delete_vocabulary_graph(prefix=VOCABULARY_PREFIX)
