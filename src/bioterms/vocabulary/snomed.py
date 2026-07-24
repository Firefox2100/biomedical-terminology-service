import os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, SimilarityMethod
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, get_trud_release_url, \
    download_rf2, rf2_dataframe_deduplicate, iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import SnomedConcept
from .utils import write_concepts_to_file, write_graph_to_file


VOCABULARY_NAME = 'SNOMED Clinical Terms'
VOCABULARY_PREFIX = ConceptPrefix.SNOMED
ANNOTATIONS = [
    ConceptPrefix.CTV3,
    ConceptPrefix.MONDO,
    ConceptPrefix.OHDSI,
    ConceptPrefix.ORDO,
]
SIMILARITY_METHODS = [
    SimilarityMethod.RELEVANCE,
    SimilarityMethod.CO_ANNOTATION,
    SimilarityMethod.WEIGHED_RELEVANCE,
]
FILE_PATHS: list[str] = [
    'snomed/international/concept.txt',
    'snomed/international/description.txt',
    'snomed/international/definition.txt',
    'snomed/international/relationship.txt',
    'snomed/uk_clinical/concept.txt',
    'snomed/uk_clinical/description.txt',
    'snomed/uk_clinical/definition.txt',
    'snomed/uk_clinical/relationship.txt',
    'snomed/uk_drug/concept.txt',
    'snomed/uk_drug/description.txt',
    'snomed/uk_drug/definition.txt',
    'snomed/uk_drug/relationship.txt',
]
TIMESTAMP_FILE = 'snomed/.timestamp'
CONCEPT_CLASS = SnomedConcept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the SNOMED vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    if not CONFIG.nhs_trud_api_key:
        raise ValueError('NHS TRUD API key is required to download SNOMED ontology.')
    api_key = CONFIG.nhs_trud_api_key

    international_url = await get_trud_release_url(
        f'https://isd.digital.nhs.uk/trud/api/v1/keys/{api_key}/items/4/releases?latest'
    )
    uk_clinical_url = await get_trud_release_url(
        f'https://isd.digital.nhs.uk/trud/api/v1/keys/{api_key}/items/101/releases?latest'
    )
    uk_drug_url = await get_trud_release_url(
        f'https://isd.digital.nhs.uk/trud/api/v1/keys/{api_key}/items/105/releases?latest'
    )

    await download_rf2(
        release_url=international_url,
        file_mapping=[
            ('SnomedCT_InternationalRF2*/Full/Terminology/sct2_Concept*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
            ('SnomedCT_InternationalRF2*/Full/Terminology/sct2_Description*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[1])),
            ('SnomedCT_InternationalRF2*/Full/Terminology/sct2_TextDefinition*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[2])),
            ('SnomedCT_InternationalRF2*/Full/Terminology/sct2_Relationship_*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[3])),
        ],
        download_client=download_client,
    )

    await download_rf2(
        release_url=uk_clinical_url,
        file_mapping=[
            ('SnomedCT_UKClinicalRF2*/Full/Terminology/sct2_Concept*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[4])),
            ('SnomedCT_UKClinicalRF2*/Full/Terminology/sct2_Description*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[5])),
            ('SnomedCT_UKClinicalRF2*/Full/Terminology/sct2_TextDefinition*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[6])),
            ('SnomedCT_UKClinicalRF2*/Full/Terminology/sct2_Relationship_*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[7])),
        ],
        download_client=download_client,
    )

    await download_rf2(
        release_url=uk_drug_url,
        file_mapping=[
            ('SnomedCT_UKDrugRF2*/Full/Terminology/sct2_Concept*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[8])),
            ('SnomedCT_UKDrugRF2*/Full/Terminology/sct2_Description*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[9])),
            ('SnomedCT_UKDrugRF2*/Full/Terminology/sct2_TextDefinition*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[10])),
            ('SnomedCT_UKDrugRF2*/Full/Terminology/sct2_Relationship_*.txt',
             os.path.join(CONFIG.data_dir, FILE_PATHS[11])),
        ],
        download_client=download_client,
    )


def _process_concepts(concept_file_path: str) -> dict[int, CONCEPT_CLASS]:
    """
    Process RF2 concept file and yield Concept instances.
    :param concept_file_path: The path to the concept file.
    :return: A dictionary mapping concept IDs to Concept instances.
    """
    concept_df = pd.read_csv(concept_file_path, sep='\t')
    concept_df = rf2_dataframe_deduplicate(concept_df)
    concepts = {}

    verbose_print(f'Concept file {concept_file_path} loaded, processing concepts...')

    for _, row in iter_progress(
        concept_df.iterrows(),
        description='Processing SNOMED concepts',
        total=len(concept_df)
    ):
        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=str(row['id']),
            fullyDefined=bool(row['definitionStatusId'] == 900000000000073002),
            status=ConceptStatus.DEPRECATED if bool(row['active'] == 0) else ConceptStatus.ACTIVE,
        )

        concepts[row['id']] = concept

    return concepts


def _process_descriptions(description_file_path: str,
                          concepts: dict[int, CONCEPT_CLASS],
                          ) -> None:
    """
    Process RF2 description file and update Concept instances with labels and synonyms.
    :param description_file_path: The path to the description file.
    :param concepts: The dictionary of Concept instances to update.
    """
    description_df = pd.read_csv(description_file_path, sep='\t')
    description_df = rf2_dataframe_deduplicate(description_df)

    verbose_print(f'Description file {description_file_path} loaded, processing descriptions...')

    for _, row in iter_progress(
        description_df.iterrows(),
        description='Processing SNOMED descriptions',
        total=len(description_df)
    ):
        if not bool(row['active']):
            # See _process_relationships -- deduplication alone does not imply active.
            continue

        if row['conceptId'] not in concepts:
            concepts[row['conceptId']] = CONCEPT_CLASS(
                prefix=VOCABULARY_PREFIX,
                conceptId=str(row['conceptId']),
                status=None,
            )

        if row['typeId'] == 900000000000003001:
            # Label
            concepts[row['conceptId']].label = row['term']
        else:
            # Synonym
            if concepts[row['conceptId']].synonyms is None:
                concepts[row['conceptId']].synonyms = [str(row['term'])]
            else:
                concepts[row['conceptId']].synonyms.append(str(row['term']))


def _process_definitions(definition_file_path: str,
                         concepts: dict[int, CONCEPT_CLASS],
                         ) -> None:
    """
    Process RF2 definition file and update Concept instances with definitions.
    :param definition_file_path: The path to the definition file.
    :param concepts: The dictionary of Concept instances to update.
    """
    definition_df = pd.read_csv(definition_file_path, sep='\t')
    definition_df = rf2_dataframe_deduplicate(definition_df)

    verbose_print(f'Definition file {definition_file_path} loaded, processing definitions...')

    for _, row in iter_progress(
        definition_df.iterrows(),
        description='Processing SNOMED definitions',
        total=len(definition_df)
    ):
        if not bool(row['active']):
            # See _process_relationships -- deduplication alone does not imply active.
            continue

        if row['conceptId'] not in concepts:
            raise ValueError(f'Concept ID {row["conceptId"]} not found in concepts dictionary.')

        concepts[row['conceptId']].definition = row['term']


def _process_relationships(relationship_file_path: str,
                           snomed_graph: nx.DiGraph,
                           ) -> None:
    """
    Process RF2 relationship file and update the SNOMED graph with relationships.
    :param relationship_file_path: The path to the relationship file.
    :param snomed_graph: The SNOMED ontology graph to update.
    """
    relationship_df = pd.read_csv(relationship_file_path, sep='\t')
    relationship_df = rf2_dataframe_deduplicate(relationship_df)

    verbose_print(f'Relationship file {relationship_file_path} loaded, processing relationships...')

    for _, row in iter_progress(
        relationship_df.iterrows(),
        description='Processing SNOMED relationships',
        total=len(relationship_df)
    ):
        if not bool(row['active']):
            # rf2_dataframe_deduplicate only keeps the latest effectiveTime per relationship
            # id -- it does not imply the kept row is active. Unlike _process_concepts, this
            # was previously unfiltered, silently loading retracted/superseded relationships
            # (~36% of is_a rows across the three releases) as if current.
            continue

        if row['typeId'] == 116680003:
            # IS_A relationship
            snomed_graph.add_edge(
                str(row['sourceId']),
                str(row['destinationId']),
                label=ConceptRelationshipType.IS_A
            )
        elif row['typeId'] == 370124000:
            # REPLACED_BY relationship -- NOTE: this typeId does not appear anywhere in the
            # current release's relationship files (verified: 0 rows, active or inactive).
            # SNOMED's actual concept-inactivation history (SAME_AS/REPLACED_BY/WAS_A/
            # MOVED_TO) lives in the separate Association Reference Set files
            # (der2_cRefset_AssociationReferenceFull*.txt), which this loader does not read.
            # Left in place rather than guessing a replacement SCTID; SNOMED replaced_by is
            # effectively non-functional until that's addressed.
            snomed_graph.add_edge(
                str(row['sourceId']),
                str(row['destinationId']),
                label=ConceptRelationshipType.REPLACED_BY
            )


async def _load_snomed_release(concept_file: str,
                               description_file: str,
                               definition_file: str,
                               relationship_file: str,
                               doc_db: DocumentDatabase = None,
                               graph_db: GraphDatabase = None,
                               offline: bool = False,
                               overwrite: bool = False,
                               ) -> None:
    """
    Load a SNOMED release from RF2 files.
    :param concept_file: The path to the concept file.
    :param description_file: The path to the description file.
    :param definition_file: The path to the definition file.
    :param relationship_file: The path to the relationship file.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    :param overwrite: Whether to overwrite existing data files in offline mode.
    """
    concept_full_path = os.path.join(CONFIG.data_dir, concept_file)
    description_full_path = os.path.join(CONFIG.data_dir, description_file)
    definition_full_path = os.path.join(CONFIG.data_dir, definition_file)
    relationship_full_path = os.path.join(CONFIG.data_dir, relationship_file)

    verbose_print('SNOMED release files found, processing...')

    concepts_dict = _process_concepts(concept_full_path)
    _process_descriptions(description_full_path, concepts_dict)
    _process_definitions(definition_full_path, concepts_dict)

    snomed_graph = nx.DiGraph()
    concepts = list(concepts_dict.values())
    del concepts_dict

    for concept in concepts:
        snomed_graph.add_node(concept.concept_id)

    _process_relationships(relationship_full_path, snomed_graph)

    verbose_print('Concept processing complete, saving to databases...')

    if not offline:
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        await doc_db.save_terms(
            terms=concepts
        )

        await graph_db.save_vocabulary_graph(
            concepts=concepts,
            graph=snomed_graph,
        )
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            overwrite=overwrite,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=snomed_graph,
            overwrite=overwrite,
        )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    ):
    """
    Load the SNOMED vocabulary from files into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('SNOMED release files not found')

    # Load International release
    await _load_snomed_release(
        concept_file=FILE_PATHS[0],
        description_file=FILE_PATHS[1],
        definition_file=FILE_PATHS[2],
        relationship_file=FILE_PATHS[3],
        doc_db=doc_db,
        graph_db=graph_db,
        offline=offline,
        overwrite=True,
    )

    # Load UK Clinical release
    await _load_snomed_release(
        concept_file=FILE_PATHS[4],
        description_file=FILE_PATHS[5],
        definition_file=FILE_PATHS[6],
        relationship_file=FILE_PATHS[7],
        doc_db=doc_db,
        graph_db=graph_db,
        offline=offline,
    )

    # Load UK Drug release
    await _load_snomed_release(
        concept_file=FILE_PATHS[8],
        description_file=FILE_PATHS[9],
        definition_file=FILE_PATHS[10],
        relationship_file=FILE_PATHS[11],
        doc_db=doc_db,
        graph_db=graph_db,
        offline=offline,
    )
