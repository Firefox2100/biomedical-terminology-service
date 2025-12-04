import os
import json
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, ConceptType, AnnotationType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, extract_file_from_zip, \
    iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import ReactomeConcept
from bioterms.model.annotation import Annotation


VOCABULARY_NAME = 'Reactome Pathways'
VOCABULARY_PREFIX = ConceptPrefix.REACTOME
ANNOTATIONS = []
SIMILARITY_METHODS = []
FILE_PATHS = [
    'reactome/pathway.csv',
    'reactome/pathway_hierarchy.csv',
    'reactome/reaction.csv',
    'reactome/reaction_order.csv',
    'reactome/reaction_pathway.csv',
    'reactome/gene.csv',
    'reactome/gene_reaction.csv',
    'reactome/gene_mapping.csv',
]
TIMESTAMP_FILE = 'reactome/.timestamp'
CONCEPT_CLASS = ReactomeConcept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the Reactome vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    zip_url = ('https://github.com/firefox2100/biomedical-terminology-service/releases/'
               'latest/download/reactome_data.zip')

    await download_file(
        url=zip_url,
        file_path='reactome/reactome_data.zip',
        download_client=download_client,
    )

    await extract_file_from_zip(
        zip_path=os.path.join(CONFIG.data_dir, 'reactome/reactome_data.zip'),
        file_mapping=[
            ('pathway.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
            ('pathway_hierarchy.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[1])),
            ('reaction.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[2])),
            ('reaction_order.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[3])),
            ('reaction_pathway.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[4])),
            ('gene.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[5])),
            ('gene_reaction.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[6])),
            ('gene_mapping.csv', os.path.join(CONFIG.data_dir, FILE_PATHS[7]))
        ]
    )


def _parse_synonyms(synonym_str: str,
                    label: str,
                    ) -> list[str] | None:
    """
    Parse synonyms from a string.
    :param synonym_str: The synonym string, may be NaN or a JSON encoded array.
    :param label: The primary label of the concept. This will be removed from the synonyms if present.
    :return: A list of synonyms or None.
    """
    if pd.isna(synonym_str):
        return None

    synonyms = json.loads(synonym_str)

    if label in synonyms:
        synonyms.remove(label)

    return synonyms if synonyms else None


def _process_concept_files() -> tuple[list[CONCEPT_CLASS], nx.DiGraph]:
    """
    Process Reactome concept files from disk and construct concepts and graph.
    :return: A tuple of list of concepts and the concept graph.
    """
    pathway_df = pd.read_csv(str(os.path.join(CONFIG.data_dir, FILE_PATHS[0])))
    reaction_df = pd.read_csv(str(os.path.join(CONFIG.data_dir, FILE_PATHS[2])))
    gene_df = pd.read_csv(str(os.path.join(CONFIG.data_dir, FILE_PATHS[5])))

    concepts = []
    reactome_graph = nx.DiGraph()

    verbose_print('Reactome concept files loaded from disk, processing concepts...')

    for _, row in iter_progress(
        pathway_df.iterrows(),
        description='Processing Reactome pathways',
        total=len(pathway_df),
    ):
        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['st_id'],
            conceptTypes=[ConceptType.PATHWAY],
            label=row['display_name'] if not pd.isna(row['display_name']) else None,
            status=ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        reactome_graph.add_node(row['st_id'])

    for _, row in iter_progress(
        reaction_df.iterrows(),
        description='Processing Reactome reactions',
        total=len(reaction_df),
    ):
        label = row['display_name'] if not pd.isna(row['display_name']) else ''
        synonyms = _parse_synonyms(
            synonym_str=row['synonyms'],
            label=label,
        )

        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['st_id'],
            conceptTypes=[ConceptType.REACTION],
            label=label,
            synonyms=synonyms,
            status=ConceptStatus.ACTIVE,
            inferred=row['inferred'],
        )

        concepts.append(concept)
        reactome_graph.add_node(row['st_id'])

    for _, row in iter_progress(
        gene_df.iterrows(),
        description='Processing Reactome genes',
        total=len(gene_df),
    ):
        label = row['display_name'] if not pd.isna(row['display_name']) else ''
        synonyms = _parse_synonyms(
            synonym_str=row['synonyms'],
            label=label,
        )

        concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=row['st_id'],
            conceptTypes=[ConceptType.GENE],
            label=label,
            synonyms=synonyms,
            status=ConceptStatus.ACTIVE,
        )

        concepts.append(concept)
        reactome_graph.add_node(row['st_id'])

    return concepts, reactome_graph


def _process_relationship_files(reactome_graph: nx.DiGraph):
    """
    Process Reactome relationship files from disk and construct the internal relationships graph.
    """
    pathway_hierarchy_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[1]))
    )
    reaction_order_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[3]))
    )
    reaction_pathway_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[4]))
    )
    gene_reaction_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[6]))
    )

    verbose_print('Reactome relationship files loaded from disk, processing relationships...')

    for _, row in iter_progress(
        pathway_hierarchy_df.iterrows(),
        description='Processing Reactome pathway hierarchy relationships',
        total=len(pathway_hierarchy_df),
    ):
        reactome_graph.add_edge(
            row['sub_pathway_st_id'],
            row['parent_st_id'],
            label=ConceptRelationshipType.PART_OF,
        )

    for _, row in iter_progress(
        reaction_order_df.iterrows(),
        description='Processing Reactome reaction order relationships',
        total=len(reaction_order_df),
    ):
        reactome_graph.add_edge(
            row['reaction_id'],
            row['preceding_reaction_id'],
            label=ConceptRelationshipType.PRECEDED_BY,
        )

    for _, row in iter_progress(
        reaction_pathway_df,
        description='Processing Reactome reaction-pathway relationships',
        total=len(reaction_pathway_df),
    ):
        reactome_graph.add_edge(
            row['reaction_id'],
            row['pathway_id'],
            label=ConceptRelationshipType.PART_OF,
        )

    for _, row in iter_progress(
        gene_reaction_df.iterrows(),
        description='Processing Reactome gene-reaction relationships',
        total=len(gene_reaction_df),
    ):
        reactome_graph.add_edge(
            row['reaction_id'],
            row['gene_id'],
            label=ConceptRelationshipType(row['relationship']),
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

    concepts, reactome_graph = _process_concept_files()
    _process_relationship_files(reactome_graph)

    annotations = []
    mapping_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[6])),
    )
    for _, row in iter_progress(
        mapping_df.iterrows(),
        description='Processing Reactome gene symbol mappings',
        total=len(mapping_df),
    ):
        annotations.append(Annotation(
            prefixFrom=VOCABULARY_PREFIX,
            prefixTo=ConceptPrefix.HGNC_SYMBOL,
            conceptIdFrom=row['gene_id'],
            conceptIdTo=row['symbol'],
            annotationType=AnnotationType.HAS_SYMBOL,
        ))

    verbose_print('Reactome concepts constructed, saving to databases...')

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
    await graph_db.save_annotations(annotations)
