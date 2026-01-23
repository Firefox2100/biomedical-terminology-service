import os
from datetime import datetime, timezone
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, get_trud_release_url, \
    download_rf2, rf2_dataframe_deduplicate, iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_NAME = 'OHDSI Standardized Vocabularies'
VOCABULARY_PREFIX = ConceptPrefix.OHDSI
ANNOTATIONS = []
SIMILARITY_METHODS = []
FILE_PATHS: list[str] = [
    # Core OHDSI files
    'ohdsi/CONCEPT.csv',
    'ohdsi/CONCEPT_SYNONYM.csv',
    'ohdsi/CONCEPT_ANCESTOR.csv',
    'ohdsi/CONCEPT_CLASS.csv',
    'ohdsi/CONCEPT_RELATIONSHIP.csv',
    'ohdsi/DOMAIN.csv',
    'ohdsi/RELATIONSHIP.csv',
    'ohdsi/VOCABULARY.csv',
    # Optional extension files
    'ohdsi/DRUG_STRENGTH.csv',
]
TIMESTAMP_FILE = 'ohdsi/.timestamp'
CONCEPT_CLASS = Concept


_CANONICAL_RELATIONSHIP_CACHE: dict[str, str] = {}


def map_vocabulary_prefix(vocabulary_id: str,
                          ) -> ConceptPrefix | str:
    """
    Given a vocabulary ID in OHDSI, map to the corresponding ConceptPrefix in this system.
    If the vocabulary is not fully supported, it will convert it into a string that may
    be used in the future as the basis of a new ConceptPrefix.
    :param vocabulary_id: The vocabulary ID.
    :return: The mapped ConceptPrefix or string.
    """
    mapping = {}

    return mapping.get(vocabulary_id, vocabulary_id.lower())


def map_vocabulary_concept_id(vocabulary_id: str,
                              concept_id: str,
                              ) -> str:
    """
    Given a vocabulary ID in OHDSI and a concept ID, map to the format of concept ID
    used in this system.
    :param vocabulary_id: The vocabulary ID.
    :param concept_id: The concept ID.
    :return: The mapped concept ID.
    """


def _canonicalize_relationship_id(relationship_id: str,
                                  source_concept_id: str,
                                  target_concept_id: str,
                                  ) -> tuple[str, str, str]:
    """
    Given a relationship ID and source/target concept IDs, return the canonical form of
    the relationship.
    :param relationship_id: The relationship ID.
    :param source_concept_id: The source concept ID.
    :param target_concept_id: The target concept ID.
    :return: A tuple of (canonical_relationship_id, canonical_source_concept_id,
        canonical_target_concept_id).
    """
    if not _CANONICAL_RELATIONSHIP_CACHE:
        relationship_df = pd.read_csv(
            os.path.join(CONFIG.data_dir, FILE_PATHS[6]),
            dtype={
                'relationship_id': str,
                'reverse_relationship_id': str,
            },
            usecols=[
                'relationship_id',
                'reverse_relationship_id',
            ],
            sep='\t',
        )

        for _, row in relationship_df.iterrows():
            rel_id = row['relationship_id']
            rev_rel_id = row['reverse_relationship_id']

            if rev_rel_id in _CANONICAL_RELATIONSHIP_CACHE:
                # Reverse relationship already mapped
                continue

            _CANONICAL_RELATIONSHIP_CACHE[rel_id] = rev_rel_id

    if relationship_id in _CANONICAL_RELATIONSHIP_CACHE:
        # Standard relationship
        return relationship_id, source_concept_id, target_concept_id

    # Reverse relationship
    for rel_id, rev_rel_id in _CANONICAL_RELATIONSHIP_CACHE.items():
        if relationship_id == rev_rel_id:
            return rel_id, target_concept_id, source_concept_id

    raise ValueError(f'Unknown relationship ID: {relationship_id}')


def _add_relationship(ohdsi_graph: nx.MultiDiGraph,
                      relationship_id: str,
                      source_concept_id: str,
                      target_concept_id: str,
                      ):
    """
    Add a relationship between two concepts in the OHDSI graph.
    :param ohdsi_graph: The OHDSI graph.
    :param relationship_id: The relationship ID.
    :param source_concept_id: The source concept ID.
    :param target_concept_id: The target concept ID.
    """
    relationship_id, source_concept_id, target_concept_id = _canonicalize_relationship_id(
        relationship_id,
        source_concept_id,
        target_concept_id,
    )

    if relationship_id in [
        'Occurs after',
        'After',
    ]:
        ohdsi_graph.add_edge(
            source_concept_id,
            target_concept_id,
            key='preceded_by',
            label=ConceptRelationshipType.PRECEDED_BY,
        )
    elif relationship_id in [
        'Occurs before',
        'Before',
    ]:
        ohdsi_graph.add_edge(
            target_concept_id,
            source_concept_id,
            key='preceded_by',
            label=ConceptRelationshipType.PRECEDED_BY,
        )
    elif relationship_id in [
        'LOINC replaced by',
        'Concept replaced by',
    ]:
        ohdsi_graph.add_edge(
            source_concept_id,
            target_concept_id,
            key='replaced_by',
            label=ConceptRelationshipType.REPLACED_BY,
        )
    elif relationship_id in [
        'LOINC replaces',
        'Concept replaces',
    ]:
        ohdsi_graph.add_edge(
            target_concept_id,
            source_concept_id,
            key='replaced_by',
            label=ConceptRelationshipType.REPLACED_BY,
        )
    elif relationship_id in [
        'Constitutes',
        'Contained in',
        'Part of',
        'Component of',
    ]:
        ohdsi_graph.add_edge(
            source_concept_id,
            target_concept_id,
            key='part_of',
            label=ConceptRelationshipType.PART_OF,
        )
    elif relationship_id in [
        'Consists of',
        'Contains',
        'Has part of',
        'Has component',
    ]:
        ohdsi_graph.add_edge(
            target_concept_id,
            source_concept_id,
            key='part_of',
            label=ConceptRelationshipType.PART_OF,
        )
    elif relationship_id in [
        'Is a',
    ]:
        ohdsi_graph.add_edge(
            source_concept_id,
            target_concept_id,
            key='is_a',
            label=ConceptRelationshipType.IS_A,
        )
    elif relationship_id in [
        'Subsumes',
    ]:
        ohdsi_graph.add_edge(
            target_concept_id,
            source_concept_id,
            key='is_a',
            label=ConceptRelationshipType.IS_A,
        )
    else:
        ohdsi_graph.add_edge(
            source_concept_id,
            target_concept_id,
            key=relationship_id,
            label=ConceptRelationshipType.OHDSI_RELATIONSHIP,
        )


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the OHDSI Standardized Vocabularies files.

    The OHDSI files does not have a unified download API, and it cannot be redistributed,
    so this function raises an error if the files are not already present.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    raise FilesNotFound(
        message='OHDSI vocabulary files does not have a unified download source. Its license '
                'prohibits free redistribution. Please download the files manually from '
                'Athena system and place them in the data folder.',
    )


def _process_concepts() -> dict[int, CONCEPT_CLASS]:
    """
    Process the OHDSI CONCEPT.csv file and return a mapping of concept IDs to Concept instances.
    :return: A dictionary mapping concept IDs to Concept instances.
    """
    concept_file_path = os.path.join(CONFIG.data_dir, FILE_PATHS[0])
    current_date = datetime.now(timezone.utc)
    date_int = int(current_date.strftime('%Y%m%d'))

    chunks = pd.read_csv(
        str(concept_file_path),
        dtype={
            'concept_id': int,
            'concept_name': str,
            'vocabulary_id': str,
            'concept_code': str,
            'valid_end_date': int,
        },
        usecols=[
            'concept_id',
            'concept_name',
            'vocabulary_id',
            'concept_code',
            'valid_end_date',
        ],
        sep='\t',
        chunksize=100000,
    )

    concepts: dict[int, CONCEPT_CLASS] = {}

    for chunk in iter_progress(chunks, desc='Processing OHDSI concepts'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI concept rows',
                                    total=len(chunk),
                                    ):
            concept = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX,
                conceptId=str(row['concept_id']),
                label=row['concept_name'],
                status=ConceptStatus.DEPRECATED
                       if row['valid_end_date'] < date_int
                       else ConceptStatus.ACTIVE,
            )
            concepts[row['concept_id']] = concept

    return concepts


def _process_synonyms(concepts: dict[int, CONCEPT_CLASS],
                      ) -> dict[int, CONCEPT_CLASS]:
    """
    Process the OHDSI CONCEPT_SYNONYM.csv file and add synonyms to the given concepts.
    :param concepts: A dictionary mapping concept IDs to Concept instances.
    :return: The updated dictionary of concepts with synonyms added.
    """
    synonym_file_path = os.path.join(CONFIG.data_dir, FILE_PATHS[1])
    chunks = pd.read_csv(
        str(synonym_file_path),
        dtype={
            'concept_id': int,
            'concept_synonym_name': str,
        },
        usecols=[
            'concept_id',
            'concept_synonym_name',
        ],
        sep='\t',
        chunksize=100000,
    )

    for chunk in iter_progress(chunks, desc='Processing OHDSI concept synonyms'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI concept synonym rows',
                                    total=len(chunk),
                                    ):
            concept_id = row['concept_id']
            synonym = row['concept_synonym_name']
            if concept_id in concepts:
                concept = concepts[concept_id]
                if concept.synonyms is None:
                    concept.synonyms = []
                concept.synonyms.append(synonym)

    return concepts


def _process_relationships(ohdsi_graph: nx.MultiDiGraph):
    """
    Process the OHDSI CONCEPT_RELATIONSHIP.csv and CONCEPT_ANCESTOR.csv files to add
    relationships to the OHDSI graph.
    :param ohdsi_graph: The OHDSI graph.
    """
    relationship_file_path = os.path.join(CONFIG.data_dir, FILE_PATHS[4])
    ancestor_file_path = os.path.join(CONFIG.data_dir, FILE_PATHS[2])
    current_date = datetime.now(timezone.utc)
    date_int = int(current_date.strftime('%Y%m%d'))

    relationship_chunks = pd.read_csv(
        str(relationship_file_path),
        dtype={
            'concept_id_1': int,
            'concept_id_2': int,
            'relationship_id': str,
            'valid_end_date': int,
        },
        usecols=[
            'concept_id_1',
            'concept_id_2',
            'relationship_id',
            'valid_end_date',
        ],
        sep='\t',
        chunksize=100000,
    )

    for chunk in iter_progress(relationship_chunks, desc='Processing OHDSI concept relationships'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI concept relationship rows',
                                    total=len(chunk),
                                    ):
            if row['valid_end_date'] < date_int:
                continue
            _add_relationship(
                ohdsi_graph,
                row['relationship_id'],
                str(row['concept_id_1']),
                str(row['concept_id_2']),
            )

    ancestor_chunks = pd.read_csv(
        str(ancestor_file_path),
        dtype={
            'ancestor_concept_id': int,
            'descendant_concept_id': int,
            'min_levels_of_separation': int,
        },
        usecols=[
            'ancestor_concept_id',
            'descendant_concept_id',
            'min_levels_of_separation',
        ],
        sep='\t',
        chunksize=100000,
    )

    for chunk in iter_progress(ancestor_chunks, desc='Processing OHDSI concept ancestors'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI concept ancestor rows',
                                    total=len(chunk),
                                    ):
            if row['min_levels_of_separation'] != 0:
                continue

            ohdsi_graph.add_edge(
                str(row['descendant_concept_id']),
                str(row['ancestor_concept_id']),
                key='is_a',
                label=ConceptRelationshipType.IS_A,
            )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the OHDSI vocabulary from files into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('OHDSI release files not found')

    concepts_dict = _process_concepts()
    concepts_dict = _process_synonyms(concepts_dict)

    ohdsi_graph = nx.MultiDiGraph()
    concepts = list(concepts_dict.values())
    del concepts_dict

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    await doc_db.save_terms(
        terms=concepts
    )

    for concept in concepts:
        ohdsi_graph.add_node(concept.concept_id)
    del concepts

    _process_relationships(ohdsi_graph)


