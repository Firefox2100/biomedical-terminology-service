"""Authenticated LOINC release download and vocabulary loading support."""

import base64
import hashlib
import os

import aiofiles
import aiofiles.os
import httpx
import networkx as nx
import pandas as pd

from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, \
    get_active_graph_db
from bioterms.etc.consts import CONFIG, DOWNLOAD_CLIENT
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, ConceptStatus, \
    SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_file, ensure_data_directory, \
    extract_file_from_zip, iter_progress, verbose_print
from bioterms.model.concept import Concept
from .utils import write_concepts_to_file, write_graph_to_file


VOCABULARY_NAME = 'Logical Observation Identifiers Names and Codes'
VOCABULARY_PREFIX = ConceptPrefix.LOINC
ANNOTATIONS = [ConceptPrefix.RXNORM, ConceptPrefix.SNOMED]
SIMILARITY_METHODS = [
    SimilarityMethod.RELEVANCE,
    SimilarityMethod.CO_ANNOTATION,
    SimilarityMethod.WEIGHED_RELEVANCE,
]
FILE_PATHS = [
    'loinc/Loinc.csv',
    'loinc/MapTo.csv',
    'loinc/Part.csv',
    'loinc/ComponentHierarchyBySystem.csv',
    'loinc/PartRelatedCodeMapping.csv',
    'loinc/license.txt',
]
TIMESTAMP_FILE = 'loinc/.timestamp'
CONCEPT_CLASS = Concept

_API_URL = 'https://loinc.regenstrief.org/api/v1/Loinc'
_ARCHIVE_PATH = 'loinc/loinc-release.zip'


def _basic_auth_header(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f'{username}:{password}'.encode()).decode()
    return {'Authorization': f'Basic {token}'}


async def _md5(path: str) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    async with aiofiles.open(path, 'rb') as stream:
        while chunk := await stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """Download the current licensed LOINC release through its official API."""
    if check_files_exist(FILE_PATHS):
        return
    if not CONFIG.loinc_username or not CONFIG.loinc_password:
        raise ValueError(
            'LOINC username and password are required. Set BTS_LOINC_USERNAME and '
            'BTS_LOINC_PASSWORD after accepting the LOINC licence.',
        )

    ensure_data_directory()
    headers = _basic_auth_header(CONFIG.loinc_username, CONFIG.loinc_password)
    client = download_client or DOWNLOAD_CLIENT
    archive_path = os.path.join(CONFIG.data_dir, _ARCHIVE_PATH)
    try:
        response = await client.get(_API_URL, headers=headers)
        response.raise_for_status()
        metadata = response.json()
        download_url = metadata.get('downloadUrl')
        expected_md5 = str(metadata.get('downloadMD5Hash', '')).lower()
        if not download_url or not expected_md5:
            raise ValueError('LOINC release API response omitted its download URL or MD5 hash.')

        await download_file(
            url=download_url,
            file_path=_ARCHIVE_PATH,
            headers=headers,
            download_client=client,
        )
        actual_md5 = await _md5(archive_path)
        if actual_md5 != expected_md5:
            raise ValueError(
                f'LOINC release checksum mismatch: expected {expected_md5}, got {actual_md5}.',
            )

        await extract_file_from_zip(
            zip_path=archive_path,
            file_mapping=[
                ('*LoincTable/Loinc.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
                ('*LoincTable/MapTo.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[1])),
                ('*AccessoryFiles/PartFile/Part.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[2])),
                (
                    '*AccessoryFiles/ComponentHierarchyBySystem/'
                    'ComponentHierarchyBySystem.csv',
                    os.path.join(CONFIG.data_dir, FILE_PATHS[3]),
                ),
                (
                    '*AccessoryFiles/PartFile/PartRelatedCodeMapping.csv',
                    os.path.join(CONFIG.data_dir, FILE_PATHS[4]),
                ),
                ('*LoincLicense_*.txt', os.path.join(CONFIG.data_dir, FILE_PATHS[5])),
            ],
        )
        verbose_print(
            f'Downloaded and verified LOINC {metadata.get("version", "current")} release.',
        )
    finally:
        try:
            await aiofiles.os.remove(archive_path)
        except FileNotFoundError:
            pass


def _clean(value) -> str | None:
    if pd.isna(value):
        return None
    value = str(value).strip()
    return value or None


def _split_synonyms(*values) -> list[str] | None:
    synonyms = []
    for value in values:
        value = _clean(value)
        if not value:
            continue
        for synonym in value.split(';'):
            synonym = synonym.strip()
            if synonym and synonym not in synonyms:
                synonyms.append(synonym)
    return synonyms or None


def _load_release() -> tuple[list[CONCEPT_CLASS], nx.MultiDiGraph]:
    term_frame = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]), dtype=str, keep_default_na=False,
    )
    part_frame = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[2]), dtype=str, keep_default_na=False,
    )
    hierarchy_frame = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[3]), dtype=str, keep_default_na=False,
    )
    map_to_frame = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[1]), dtype=str, keep_default_na=False,
    )

    concepts: dict[str, CONCEPT_CLASS] = {}
    graph = nx.MultiDiGraph()
    for _, row in iter_progress(
        term_frame.iterrows(), total=len(term_frame), description='Processing LOINC terms',
    ):
        concept_id = row['LOINC_NUM'].strip()
        label = _clean(row.get('LONG_COMMON_NAME')) or _clean(row.get('SHORTNAME'))
        synonyms = _split_synonyms(
            row.get('SHORTNAME'), row.get('CONSUMER_NAME'), row.get('DisplayName'),
            row.get('RELATEDNAMES2'),
        )
        if label and synonyms:
            synonyms = [value for value in synonyms if value != label] or None
        concepts[concept_id] = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=concept_id,
            conceptTypes=[],
            label=label,
            definition=_clean(row.get('DefinitionDescription')),
            synonyms=synonyms,
            status=(
                ConceptStatus.DEPRECATED
                if str(row.get('STATUS', '')).upper() == 'DEPRECATED'
                else ConceptStatus.ACTIVE
            ),
        )
        graph.add_node(concept_id)

    for _, row in map_to_frame.iterrows():
        source_id = str(row['LOINC']).strip()
        replacement_id = str(row['MAP_TO']).strip()
        if source_id and replacement_id:
            graph.add_edge(
                source_id, replacement_id, key=ConceptRelationshipType.REPLACED_BY.value,
                label=ConceptRelationshipType.REPLACED_BY,
            )

    for _, row in iter_progress(
        part_frame.iterrows(), total=len(part_frame), description='Processing LOINC parts',
    ):
        concept_id = str(row['PartNumber']).strip()
        concepts.setdefault(concept_id, CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=concept_id,
            conceptTypes=[],
            label=_clean(row.get('PartName')),
            status=(
                ConceptStatus.DEPRECATED
                if str(row.get('Status', '')).upper() in {'DEPRECATED', 'INACTIVE'}
                else ConceptStatus.ACTIVE
            ),
        ))
        graph.add_node(concept_id)

    for _, row in iter_progress(
        hierarchy_frame.iterrows(), total=len(hierarchy_frame),
        description='Processing LOINC hierarchy',
    ):
        child = str(row.get('CODE', '')).strip()
        parent = str(row.get('IMMEDIATE_PARENT', '')).strip()
        if not child:
            continue
        if child not in concepts:
            concepts[child] = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX, conceptId=child, conceptTypes=[],
                label=_clean(row.get('CODE_TEXT')), status=ConceptStatus.ACTIVE,
            )
        graph.add_node(child)
        if parent:
            graph.add_edge(
                child, parent, key=ConceptRelationshipType.IS_A.value,
                label=ConceptRelationshipType.IS_A,
            )

    return list(concepts.values()), graph


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True,
                                    ):
    """Load LOINC terms, constituent Parts, hierarchy, and replacement links."""
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('LOINC release files not found')
    concepts, graph = _load_release()

    if offline:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX, concepts=concepts,
            build_search_index=build_search_index,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX, concepts=concepts, vocabulary_graph=graph,
        )
        return

    doc_db = doc_db or await get_active_doc_db()
    graph_db = graph_db or get_active_graph_db()
    await doc_db.save_terms(terms=concepts, no_upsert=True)
    await graph_db.save_vocabulary_graph(concepts=concepts, graph=graph)
