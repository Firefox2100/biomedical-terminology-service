import os
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept
from .hgnc import download_vocabulary


VOCABULARY_NAME = 'HUGO Gene Nomenclature Committee Symbol'
VOCABULARY_PREFIX = ConceptPrefix.HGNC_SYMBOL
ANNOTATIONS = []
SIMILARITY_METHODS = []
FILE_PATHS = [
    'hgnc/symbol.txt',
    'hgnc/withdrawn.txt',
]
TIMESTAMP_FILE = 'hgnc/.timestamp'
CONCEPT_CLASS = Concept


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the HGNC symbols from a file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('HGNC symbol files not found')

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

    active_symbols = set()
    symbol_df['alias_symbol_substrings'] = symbol_df['alias_symbol'].str.split('|')
    alias_symbols = symbol_df['alias_symbol_substrings'].explode().dropna().tolist()

    active_symbols.update(alias_symbols)
    active_symbols.update(symbol_df['symbol'].tolist())

    withdrawn_symbols = set(withdrawn_df['WITHDRAWN_SYMBOL'].tolist())
    active_symbols = active_symbols - withdrawn_symbols

    concepts = []
    gene_graph = nx.DiGraph()

    for symbol in active_symbols:
        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=symbol,
            label=symbol,
            status=ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        gene_graph.add_node(symbol)

    for symbol in withdrawn_symbols:
        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=symbol,
            label=symbol,
            status=ConceptStatus.DEPRECATED,
        )

        concepts.append(concept)
        gene_graph.add_node(symbol)

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.save_terms(
        terms=concepts
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=gene_graph,
    )
