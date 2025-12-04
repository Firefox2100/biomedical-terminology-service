import os
import xml.etree.ElementTree as ElementTree
import httpx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, extract_file_from_gzip, \
    iter_progress, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'ORDO Mapping to HGNC Gene Symbol'
VOCABULARY_PREFIX_1 = ConceptPrefix.ORDO
VOCABULARY_PREFIX_2 = ConceptPrefix.HGNC_SYMBOL
FILE_PATHS = ['ordo/gene_mapping.xml']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the ORDO en_product6 mapping file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    annotation_url = 'https://www.orphadata.com/data/xml/en_product6.xml'

    await download_file(
        url=annotation_url,
        file_path=FILE_PATHS[0],
        download_client=download_client,
    )


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the ORDO to HGNC symbol mapping from a file into the primary databases.
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

    mapping_tree = ElementTree.parse(os.path.join(CONFIG.data_dir, FILE_PATHS[0]))
    mapping_root = mapping_tree.getroot()

    verbose_print('ORDO to HGNC Gene Symbol mapping loaded from disk. Processing annotations...')

    annotations = []
    disorders = mapping_root.find('DisorderList').findall('Disorder')

    for disorder in iter_progress(
        disorders,
        description='Processing ORDO to HGNC Gene Symbol annotations',
        total=len(disorders),
    ):
        orpha_code = disorder.find('OrphaCode').text
        gene_annotation_list = disorder.find('DisorderGeneAssociationList')

        if gene_annotation_list is not None:
            for a in gene_annotation_list.findall('DisorderGeneAssociation'):
                gene_symbol = a.find('Gene').find('Symbol').text

                annotations.append(Annotation(
                    prefixFrom=VOCABULARY_PREFIX_1,
                    prefixTo=VOCABULARY_PREFIX_2,
                    conceptIdFrom=orpha_code,
                    conceptIdTo=gene_symbol,
                ))

    verbose_print(f'Saving {len(annotations)} ORDO to HGNC symbol annotations to the graph database...')

    await graph_db.save_annotations(
        annotations=annotations
    )
