import csv
import os
from datetime import datetime, timezone
from functools import lru_cache
import httpx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, AnnotationType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import OhdsiDrugStrength, OhdsiConcept
from bioterms.model.annotation import Annotation
from bioterms.annotation.utils import AnnotationSource, is_gene_annotation_prefix
from .utils import write_concepts_to_file, write_graph_to_file, write_annotations_to_file


VOCABULARY_NAME = 'OHDSI Standardized Vocabularies'
VOCABULARY_PREFIX = ConceptPrefix.OHDSI
ANNOTATIONS = [
    ConceptPrefix.NCIT,
    ConceptPrefix.RXNORM,
    ConceptPrefix.SNOMED,
]
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
CONCEPT_CLASS = OhdsiConcept


_CANONICAL_RELATIONSHIP_CACHE: dict[str, str] = {}
_REVERSE_RELATIONSHIP_CACHE: dict[str, str] = {}


@lru_cache
def _ohdsi_annotation_source(target_prefix: ConceptPrefix | str) -> AnnotationSource:
    return AnnotationSource('OHDSI Athena', VOCABULARY_PREFIX, target_prefix)


def map_vocabulary_prefix(vocabulary_id: str,
                          ) -> ConceptPrefix | str:
    """
    Given a vocabulary ID in OHDSI, map to the corresponding ConceptPrefix in this system.
    If the vocabulary is not fully supported, it will convert it into a string that may
    be used in the future as the basis of a new ConceptPrefix.
    :param vocabulary_id: The vocabulary ID.
    :return: The mapped ConceptPrefix or string.
    """
    mapping = {
        'HGNC': ConceptPrefix.HGNC,
        'NCIt': ConceptPrefix.NCIT,
        'RxNorm': ConceptPrefix.RXNORM,
        'SNOMED': ConceptPrefix.SNOMED,
    }

    return mapping.get(vocabulary_id, vocabulary_id.lower())


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
            quoting=csv.QUOTE_NONE,
        )

        for _, row in relationship_df.iterrows():
            rel_id = row['relationship_id']
            rev_rel_id = row['reverse_relationship_id']

            if rev_rel_id in _CANONICAL_RELATIONSHIP_CACHE:
                # Reverse relationship already mapped
                continue

            _CANONICAL_RELATIONSHIP_CACHE[rel_id] = rev_rel_id
            _REVERSE_RELATIONSHIP_CACHE[rev_rel_id] = rel_id

    if relationship_id in _CANONICAL_RELATIONSHIP_CACHE:
        # Standard relationship
        return relationship_id, source_concept_id, target_concept_id

    # Reverse relationship. Avoid scanning the relationship catalogue for every row in the
    # multi-gigabyte CONCEPT_RELATIONSHIP file.
    if relationship_id in _REVERSE_RELATIONSHIP_CACHE:
        return (
            _REVERSE_RELATIONSHIP_CACHE[relationship_id],
            target_concept_id,
            source_concept_id,
        )

    raise ValueError(f'Unknown relationship ID: {relationship_id}')


def _relationship_edge(relationship_id: str,
                       source_concept_id: str,
                       target_concept_id: str,
                       ) -> tuple[str, str, str, str]:
    """Convert one OHDSI relationship to a normalized streamed edge tuple."""
    relationship_id, source_concept_id, target_concept_id = _canonicalize_relationship_id(
        relationship_id,
        source_concept_id,
        target_concept_id,
    )

    if relationship_id in [
        'Occurs after',
        'After',
    ]:
        return source_concept_id, target_concept_id, 'preceded_by', 'preceded_by'
    elif relationship_id in [
        'Occurs before',
        'Before',
    ]:
        return target_concept_id, source_concept_id, 'preceded_by', 'preceded_by'
    elif relationship_id in [
        'LOINC replaced by',
        'Concept replaced by',
    ]:
        return source_concept_id, target_concept_id, 'replaced_by', 'replaced_by'
    elif relationship_id in [
        'LOINC replaces',
        'Concept replaces',
    ]:
        return target_concept_id, source_concept_id, 'replaced_by', 'replaced_by'
    elif relationship_id in [
        'Constitutes',
        'Contained in',
        'Part of',
        'Component of',
    ]:
        return source_concept_id, target_concept_id, 'part_of', 'part_of'
    elif relationship_id in [
        'Consists of',
        'Contains',
        'Has part of',
        'Has component',
    ]:
        return target_concept_id, source_concept_id, 'part_of', 'part_of'
    elif relationship_id in [
        'Is a',
        # Vocabulary-qualified variant of 'Is a'/'Subsumes' -- same semantics, own
        # relationship_id/reverse pair in RELATIONSHIP.csv. Confirmed via that file: no
        # other "X is a"/"X subsumes" variant has any actual rows in CONCEPT_RELATIONSHIP.csv
        # for this data extract.
        'RxNorm is a',
    ]:
        return source_concept_id, target_concept_id, 'is_a', 'is_a'
    elif relationship_id in [
        'Subsumes',
    ]:
        return target_concept_id, source_concept_id, 'is_a', 'is_a'
    else:
        return source_concept_id, target_concept_id, 'ohdsi_relationship', relationship_id


def _add_relationship(ohdsi_graph,
                      relationship_id: str,
                      source_concept_id: str,
                      target_concept_id: str,
                      ):
    """Compatibility wrapper for graph-based callers and focused unit tests."""
    source, target, label, key = _relationship_edge(
        relationship_id, source_concept_id, target_concept_id,
    )
    ohdsi_graph.add_edge(
        source, target, key=key, label=ConceptRelationshipType(label),
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
            'valid_end_date': int,
        },
        usecols=[
            'concept_id',
            'concept_name',
            'vocabulary_id',
            'valid_end_date',
        ],
        sep='\t',
        chunksize=100000,
        quoting=csv.QUOTE_NONE,
    )

    concepts: dict[int, CONCEPT_CLASS] = {}

    for chunk in iter_progress(chunks, desc='Processing OHDSI concepts'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI concept rows',
                                    total=len(chunk),
                                    transient=True,
                                    ):
            concept = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX,
                conceptId=str(row['concept_id']),
                label=str(row['concept_name']),
                status=ConceptStatus.DEPRECATED
                       if row['valid_end_date'] < date_int
                       else ConceptStatus.ACTIVE,
                sourceVocabularyId=None if pd.isna(row['vocabulary_id']) else str(row['vocabulary_id']),
            )
            concepts[row['concept_id']] = concept

    return concepts


def _process_synonyms(concepts: dict[int, CONCEPT_CLASS]):
    """
    Process the OHDSI CONCEPT_SYNONYM.csv file and add synonyms to the given concepts.
    :param concepts: A dictionary mapping concept IDs to Concept instances.
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
        quoting=csv.QUOTE_NONE,
    )

    for chunk in iter_progress(chunks, desc='Processing OHDSI concept synonyms'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI concept synonym rows',
                                    total=len(chunk),
                                    transient=True,
                                    ):
            concept_id = row['concept_id']
            synonym = row['concept_synonym_name']
            if concept_id in concepts:
                concept = concepts[concept_id]
                if concept.synonyms is None:
                    concept.synonyms = []
                concept.synonyms.append(str(synonym))


def _apply_drug_strength_row(row,
                             concepts: dict[int, CONCEPT_CLASS],
                             ):
    """
    Build a drug strength entry from one DRUG_STRENGTH.csv row and attach it to its drug concept.
    :param row: The row from the OHDSI DRUG_STRENGTH.csv file.
    :param concepts: A dictionary mapping concept IDs to Concept instances.
    """
    drug_strength = OhdsiDrugStrength(
        ingredientId=str(row['ingredient_concept_id']),
        amountValue=row['amount_value'] if not pd.isna(row['amount_value']) else None,
        numeratorValue=row['numerator_value'] if not pd.isna(row['numerator_value']) else None,
        denominatorValue=row['denominator_value'] if not pd.isna(row['denominator_value']) else None,
    )

    if not pd.isna(row['amount_unit_concept_id']):
        amount_unit = concepts[row['amount_unit_concept_id']].label
        drug_strength.amount_unit = amount_unit
    if not pd.isna(row['numerator_unit_concept_id']):
        numerator_unit = concepts[row['numerator_unit_concept_id']].label
        drug_strength.numerator_unit = numerator_unit
    if not pd.isna(row['denominator_unit_concept_id']):
        denominator_unit = concepts[row['denominator_unit_concept_id']].label
        drug_strength.denominator_unit = denominator_unit

    drug_concept = concepts.get(row['drug_concept_id'])
    if drug_concept:
        if drug_concept.drug_strengths is None:
            drug_concept.drug_strengths = []
        drug_concept.drug_strengths.append(drug_strength)


def _process_drug_strength(concepts: dict[int, CONCEPT_CLASS]):
    """
    Process the OHDSI DRUG_STRENGTH.csv file to extract drug strength information.
    Currently a placeholder function.
    """
    drug_strength_file_path = os.path.join(CONFIG.data_dir, FILE_PATHS[8])

    if not os.path.exists(drug_strength_file_path):
        return

    chunks = pd.read_csv(
        str(drug_strength_file_path),
        dtype={
            'drug_concept_id': int,
            'ingredient_concept_id': int,
            'amount_value': 'Float64',
            'amount_unit_concept_id': 'Int64',
            'numerator_value': 'Float64',
            'numerator_unit_concept_id': 'Int64',
            'denominator_value': 'Float64',
            'denominator_unit_concept_id': 'Int64',
        },
        usecols=[
            'drug_concept_id',
            'ingredient_concept_id',
            'amount_value',
            'amount_unit_concept_id',
            'numerator_value',
            'numerator_unit_concept_id',
            'denominator_value',
            'denominator_unit_concept_id',
        ],
        sep='\t',
        chunksize=100000,
        quoting=csv.QUOTE_NONE,
    )

    for chunk in iter_progress(chunks, desc='Processing OHDSI drug strengths'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI drug strength rows',
                                    total=len(chunk),
                                    transient=True,
                                    ):
            _apply_drug_strength_row(row, concepts)


def _iter_relationship_edges():
    """
    Stream normalized OHDSI edges from CONCEPT_RELATIONSHIP.csv and CONCEPT_ANCESTOR.csv.

    Filtering is vectorized per input chunk and only the retained rows cross the Python
    boundary. The returned tuples can be consumed directly by either database or offline-file
    writers without materialising a NetworkX graph.
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
        quoting=csv.QUOTE_NONE,
    )

    for chunk in iter_progress(relationship_chunks, desc='Processing OHDSI concept relationships'):
        retained = chunk.loc[
            (chunk['valid_end_date'] >= date_int)
            & (chunk['concept_id_1'] != chunk['concept_id_2'])
        ]
        for row in retained.itertuples(index=False):
            yield _relationship_edge(
                row.relationship_id,
                str(row.concept_id_1),
                str(row.concept_id_2),
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
        quoting=csv.QUOTE_NONE,
    )

    for chunk in iter_progress(ancestor_chunks, desc='Processing OHDSI concept ancestors'):
        retained = chunk.loc[chunk['min_levels_of_separation'] == 1]
        for row in retained.itertuples(index=False):
            yield (
                str(row.descendant_concept_id),
                str(row.ancestor_concept_id),
                ConceptRelationshipType.IS_A.value,
                'is_a',
            )


def _process_annotations(target_prefix: ConceptPrefix | None = None) -> list[Annotation]:
    """
    Extract mapping to underlying vocabularies in OHDSI.
    :param target_prefix: Optionally retain mappings to one supported target vocabulary.
    :return: A list of Annotation instances.
    """
    concept_file_path = os.path.join(CONFIG.data_dir, FILE_PATHS[0])

    chunks = pd.read_csv(
        str(concept_file_path),
        dtype={
            'concept_id': int,
            'concept_name': str,
            'vocabulary_id': str,
            'concept_code': str,
        },
        usecols=[
            'concept_id',
            'concept_name',
            'vocabulary_id',
            'concept_code',
        ],
        sep='\t',
        chunksize=100000,
        quoting=csv.QUOTE_NONE,
    )

    annotations = []

    for chunk in iter_progress(chunks, desc='Processing OHDSI annotations'):
        for _, row in iter_progress(chunk.iterrows(),
                                    description='Processing OHDSI annotation rows',
                                    total=len(chunk),
                                    transient=True,
                                    ):
            # Ignore the NaN vocabulary IDs and concept codes
            if pd.isna(row['vocabulary_id']) or pd.isna(row['concept_code']):
                continue

            if str(row['concept_id']) == str(row['concept_code']):
                # Skip self-mapping
                continue

            vocabulary_prefix = map_vocabulary_prefix(row['vocabulary_id'])
            if target_prefix is not None and vocabulary_prefix != target_prefix:
                continue
            if vocabulary_prefix == 'Vocabulary':
                # Special case: these codes are representing vocabularies themselves, and does
                # not even have a unique concept code.
                continue

            # Gene-vocabulary links remain outside provenance-managed annotations.
            if is_gene_annotation_prefix(vocabulary_prefix):
                annotation = Annotation(
                    prefixFrom=VOCABULARY_PREFIX,
                    prefixTo=vocabulary_prefix,
                    conceptIdFrom=str(row['concept_id']),
                    conceptIdTo=str(row['concept_code']),
                    annotationType=AnnotationType.EXACT,
                )
            else:
                annotation = _ohdsi_annotation_source(vocabulary_prefix).create(
                    publisher_concept_id=str(row['concept_id']),
                    other_concept_id=str(row['concept_code']),
                    annotation_type=AnnotationType.EXACT,
                )
            annotations.append(annotation)

    return annotations


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True,
                                    load_annotations: bool = True,
                                    ):
    """
    Load the OHDSI vocabulary from files into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    """
    if not check_files_exist(FILE_PATHS[:7]):
        raise FilesNotFound('OHDSI release files not found')

    concepts_dict = _process_concepts()
    verbose_print(f'Concept file processing completed, loaded {len(concepts_dict)} OHDSI concepts.')
    _process_synonyms(concepts_dict)
    verbose_print('Synonym file processing completed.')
    _process_drug_strength(concepts_dict)
    verbose_print('Drug strength file processing completed.')

    concepts = list(concepts_dict.values())
    del concepts_dict

    if not offline:
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        await doc_db.save_terms(
            terms=concepts,
            no_upsert=True,
        )
        await graph_db.save_vocabulary_graph(
            concepts=concepts,
            graph=_iter_relationship_edges(),
            consume_concepts=True,
        )
        verbose_print('Relationship file processing completed.')

        if load_annotations:
            annotations = _process_annotations()
            verbose_print(f'Saving {len(annotations)} OHDSI annotations to the database...')
            await graph_db.save_annotations(annotations)
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            build_search_index=build_search_index,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=_iter_relationship_edges(),
        )
        verbose_print('Relationship file processing completed.')
        del concepts

        if load_annotations:
            annotations = _process_annotations()
            await write_annotations_to_file(
                prefix_from=VOCABULARY_PREFIX,
                annotations=annotations,
            )
