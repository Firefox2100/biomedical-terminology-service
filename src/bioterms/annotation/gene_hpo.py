import os
import httpx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_vocabulary_loaded


ANNOTATION_NAME = 'HGNC Gene Symbol Mapping to HPO'
VOCABULARY_PREFIX_1 = ConceptPrefix.HGNC_SYMBOL
VOCABULARY_PREFIX_2 = ConceptPrefix.HPO
FILE_PATHS = ['hpo/gene_mapping.txt']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the SNOMED mapping file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    annotation_url = ('https://github.com/obophenotype/human-phenotype-ontology/releases/'
                      'latest/download/genes_to_phenotype.txt')

    await download_file(
        url=annotation_url,
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the gene to HPO mapping from a file into the primary databases.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    await assert_vocabulary_loaded(
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
        graph_db=graph_db,
    )

    annotation_count = await graph_db.count_annotations(
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
    )

    if annotation_count > 0:
        return  # Annotations already exist, skip loading

    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Gene-HPO mapping file not found. Please download it first.')

    mapping_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        sep='\t',
    )

    annotations = []

    for _, row in mapping_df.iterrows():
        if row['gene_symbol'] == '-':
            # No corresponding HGNC code
            continue

        frequency = 'UN'
        if row['frequency'] != '-':
            frequency_id = row['frequency'].split(':')[-1]

            match frequency_id:
                case '0040285':
                    # Excluded
                    frequency = 'E'
                case '0040284':
                    # Very rare
                    frequency = 'VR'
                case '0040283':
                    # Occasional
                    frequency = 'OC'
                case '0040282':
                    # Frequent
                    frequency = 'F'
                case '0040281':
                    # Very frequent
                    frequency = 'VF'
                case '0040280':
                    # Obligate
                    frequency = 'O'
                case _:
                    frequency = 'UN'

        annotations.append(Annotation(
            prefixFrom=VOCABULARY_PREFIX_1,
            prefixTo=VOCABULARY_PREFIX_2,
            conceptIdFrom=row['gene_symbol'],
            conceptIdTo=row['hpo_id'].split(':')[-1],
            properties={'frequency': frequency},
        ))

    await graph_db.save_annotations(
        annotations=annotations
    )
