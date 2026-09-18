"""RxNorm full monthly release download and ingestion."""

import os

import aiofiles.os
import httpx
import pandas as pd

from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus, ConceptType, \
    SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_file, ensure_data_directory, \
    extract_file_from_zip, iter_progress
from bioterms.model.concept import Concept
from bioterms.model.edge_buffer import EdgeBuffer
from .utils import write_concepts_to_file, write_graph_to_file


VOCABULARY_NAME = 'RxNorm'
VOCABULARY_PREFIX = ConceptPrefix.RXNORM
ANNOTATIONS = [ConceptPrefix.LOINC, ConceptPrefix.OHDSI, ConceptPrefix.SNOMED]
SIMILARITY_METHODS = [
    SimilarityMethod.RELEVANCE, SimilarityMethod.CO_ANNOTATION,
    SimilarityMethod.WEIGHED_RELEVANCE,
]
FILE_PATHS = [
    'rxnorm/RXNCONSO.RRF', 'rxnorm/RXNREL.RRF', 'rxnorm/RXNCUI.RRF',
]
TIMESTAMP_FILE = 'rxnorm/.timestamp'
CONCEPT_CLASS = Concept

_ARCHIVE_PATH = 'rxnorm/rxnorm-full-current.zip'
_TTY_TYPES = {
    'IN': ConceptType.INGREDIENT, 'PIN': ConceptType.INGREDIENT,
    'MIN': ConceptType.INGREDIENT, 'BN': ConceptType.BRAND,
    'DF': ConceptType.DOSE_FORM, 'DFG': ConceptType.DOSE_FORM,
    'SCD': ConceptType.CLINICAL_DRUG, 'SCDC': ConceptType.CLINICAL_DRUG,
    'SCDF': ConceptType.CLINICAL_DRUG, 'SCDG': ConceptType.CLINICAL_DRUG,
    'SBD': ConceptType.BRANDED_DRUG, 'SBDC': ConceptType.BRANDED_DRUG,
    'SBDF': ConceptType.BRANDED_DRUG, 'SBDG': ConceptType.BRANDED_DRUG,
    'GPCK': ConceptType.PACK, 'BPCK': ConceptType.PACK,
}


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """Download and extract the current full monthly RxNorm release through UTS."""
    if check_files_exist(FILE_PATHS):
        return
    if not CONFIG.nih_umls_api_key:
        raise ValueError('NIH UMLS API key is required to download the full RxNorm release.')
    ensure_data_directory()
    archive_path = os.path.join(CONFIG.data_dir, _ARCHIVE_PATH)
    release_url = (
        'https://uts-ws.nlm.nih.gov/download?url='
        'https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip'
        f'&apiKey={CONFIG.nih_umls_api_key}'
    )
    try:
        await download_file(release_url, _ARCHIVE_PATH, download_client=download_client)
        await extract_file_from_zip(
            archive_path,
            [(f'*rrf/{name}', os.path.join(CONFIG.data_dir, path))
             for name, path in zip(('RXNCONSO.RRF', 'RXNREL.RRF', 'RXNCUI.RRF'), FILE_PATHS)],
        )
    finally:
        try:
            await aiofiles.os.remove(archive_path)
        except FileNotFoundError:
            pass


def _read_rrf(path: str, columns: list[str]) -> pd.DataFrame:
    return pd.read_csv(
        os.path.join(CONFIG.data_dir, path), sep='|', header=None, names=columns,
        usecols=range(len(columns)), dtype=str, keep_default_na=False,
    )


def _read_rrf_chunks(path: str, columns: list[str], chunk_size: int = 250_000):
    """Stream a large RRF table in bounded-memory chunks."""
    return pd.read_csv(
        os.path.join(CONFIG.data_dir, path), sep='|', header=None, names=columns,
        usecols=range(len(columns)), dtype=str, keep_default_na=False,
        chunksize=chunk_size,
    )


def iter_rxnorm_atoms():
    """Yield bounded-memory chunks from the release atom table for annotation loaders."""
    yield from _read_rrf_chunks(FILE_PATHS[0], [
        'RXCUI', 'LAT', 'TS', 'LUI', 'STT', 'SUI', 'ISPREF', 'RXAUI', 'SAUI', 'SCUI',
        'SDUI', 'SAB', 'TTY', 'CODE', 'STR', 'SRL', 'SUPPRESS', 'CVF',
    ])


def load_rxnorm_concepts() -> dict[str, CONCEPT_CLASS]:
    """Build current RxNorm concepts without retaining the full source atom table."""
    records: dict[str, dict] = {}
    for atoms in iter_progress(
        iter_rxnorm_atoms(),
        description='Processing RxNorm atom batches',
    ):
        atoms = atoms[(atoms['SAB'] == 'RXNORM') & (atoms['LAT'] == 'ENG')]
        for row in atoms.itertuples(index=False):
            record = records.setdefault(row.RXCUI, {
                'terms': [], 'term_set': set(), 'types': [], 'active': False,
                'first_label': None, 'active_label': None, 'preferred_label': None,
            })
            term = row.STR.strip()
            if term and term not in record['term_set']:
                record['term_set'].add(term)
                record['terms'].append(term)
            if record['first_label'] is None and term:
                record['first_label'] = term
            if row.SUPPRESS == 'N':
                record['active'] = True
                if record['active_label'] is None and term:
                    record['active_label'] = term
                if row.ISPREF == 'Y' and record['preferred_label'] is None and term:
                    record['preferred_label'] = term
            concept_type = _TTY_TYPES.get(row.TTY, ConceptType.DRUG)
            if concept_type not in record['types']:
                record['types'].append(concept_type)

    concepts = {}
    for rxcui, record in iter_progress(
        records.items(), description='Building RxNorm concepts', total=len(records),
    ):
        label = record['preferred_label'] or record['active_label'] or record['first_label']
        concepts[rxcui] = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX, conceptId=rxcui,
            conceptTypes=record['types'], label=label,
            synonyms=[term for term in record['terms'] if term != label] or None,
            status=ConceptStatus.ACTIVE if record['active'] else ConceptStatus.DEPRECATED,
        )
    return concepts


def _load_graph(concepts: dict[str, CONCEPT_CLASS]) -> EdgeBuffer:
    graph = EdgeBuffer()
    graph.add_nodes_from(concepts)
    for relations in iter_progress(
        _read_rrf_chunks(FILE_PATHS[1], [
            'RXCUI1', 'RXAUI1', 'STYPE1', 'REL', 'RXCUI2', 'RXAUI2', 'STYPE2', 'RELA',
            'RUI', 'SRUI', 'SAB', 'SL', 'DIR', 'RG', 'SUPPRESS', 'CVF',
        ]),
        description='Processing RxNorm relationship batches',
    ):
        relations = relations[
            (relations['SAB'] == 'RXNORM') & (relations['STYPE1'] == 'CUI')
            & (relations['STYPE2'] == 'CUI') & (relations['SUPPRESS'].isin(['', 'N']))
        ]
        for row in relations.itertuples(index=False):
            first, second = row.RXCUI1, row.RXCUI2
            if first not in concepts or second not in concepts:
                continue
            relationship = row.RELA or row.REL
            if relationship == 'isa':
                source, target, label = second, first, ConceptRelationshipType.IS_A
            elif relationship == 'inverse_isa':
                source, target, label = first, second, ConceptRelationshipType.IS_A
            else:
                source, target, label = second, first, ConceptRelationshipType.RXNORM_RELATIONSHIP
            graph.add_edge(source, target, key=relationship, label=label)

    retired = _read_rrf(
        FILE_PATHS[2], ['CUI1', 'VER_START', 'VER_END', 'CARDINALITY', 'CUI2'],
    )
    for _, row in retired.iterrows():
        old_id, new_id = row['CUI1'].strip(), row['CUI2'].strip()
        if not old_id or not new_id or old_id == new_id:
            continue
        if old_id not in concepts:
            concepts[old_id] = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX, conceptId=old_id, conceptTypes=[ConceptType.DRUG],
                status=ConceptStatus.DEPRECATED,
            )
            graph.add_node(old_id)
        if new_id not in concepts:
            concepts[new_id] = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX, conceptId=new_id, conceptTypes=[ConceptType.DRUG],
                status=ConceptStatus.DEPRECATED,
            )
            graph.add_node(new_id)
        graph.add_edge(
            old_id, new_id, key=ConceptRelationshipType.REPLACED_BY.value,
            label=ConceptRelationshipType.REPLACED_BY,
        )
    return graph


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True):
    """Load the complete current RxNorm terminology and cumulative replacements."""
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('RxNorm full release files not found')
    concepts = load_rxnorm_concepts()
    graph = _load_graph(concepts)
    concept_list = list(concepts.values())
    if offline:
        await write_concepts_to_file(
            VOCABULARY_PREFIX, concept_list, build_search_index=build_search_index,
        )
        await write_graph_to_file(VOCABULARY_PREFIX, concept_list, graph)
        return
    doc_db = doc_db or await get_active_doc_db()
    graph_db = graph_db or get_active_graph_db()
    await doc_db.save_terms(terms=concept_list, no_upsert=True)
    await graph_db.save_vocabulary_graph(concepts=concept_list, graph=graph)
