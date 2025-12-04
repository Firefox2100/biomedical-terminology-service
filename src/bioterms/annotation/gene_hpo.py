import os
import httpx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, iter_progress, \
    verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'HGNC Gene Symbol Mapping to HPO'
VOCABULARY_PREFIX_1 = ConceptPrefix.HGNC_SYMBOL
VOCABULARY_PREFIX_2 = ConceptPrefix.HPO
FILE_PATHS = ['hpo/gene_mapping.txt']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the HPO gene_to_phenotype mapping file.
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

    await assert_pre_requisite(
        annotation_name=ANNOTATION_NAME,
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
        file_paths=FILE_PATHS,
        graph_db=graph_db,
    )

    mapping_df = pd.read_csv(
        os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        sep='\t',
    )

    verbose_print('HGNC symbol to HPO annotation file loaded from disk, processing annotations...')

    annotations = []

    for _, row in iter_progress(
        mapping_df.iterrows(),
        description='Processing HGNC to HPO annotations',
        total=len(mapping_df),
    ):
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

    verbose_print(f'Inserting {len(annotations)} annotations into the graph database...')

    await graph_db.save_annotations(
        annotations=annotations
    )
