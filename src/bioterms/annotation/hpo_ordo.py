import os
import httpx
from owlready2 import get_ontology

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_vocabulary_loaded


ANNOTATION_NAME = 'HPO - ORDO Ontological Module'
VOCABULARY_PREFIX_1 = ConceptPrefix.ORDO
VOCABULARY_PREFIX_2 = ConceptPrefix.HPO
FILE_PATHS = ['hoom/hoom_orphanet.owl']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the HOOM files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    owl_url = 'https://data.bioontology.org/ontologies/HOOM/download'

    if not CONFIG.bioportal_api_key:
        raise ValueError('BioPortal API key is required to download HOOM mapping.')

    await download_file(
        url=owl_url,
        file_path=FILE_PATHS[0],
        headers={'Authorization': f'apikey token={CONFIG.bioportal_api_key}'},
        download_client=download_client,
    )


def delete_vocabulary_files():
    """
    Delete the HOOM files.
    """
    try:
        os.remove(os.path.join(CONFIG.data_dir, FILE_PATHS[0]))
    except Exception:
        pass


async def load_annotation_from_file(overwrite: bool = False,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the HOOM mapping from a file into the primary databases.
    :param overwrite: Whether to overwrite existing annotation data.
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
        if not overwrite:
            return

        await delete_annotation_data(graph_db=graph_db)

    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('HOOM owl file not found')

    owl_file_path = f'file://{os.path.join(CONFIG.data_dir, FILE_PATHS[0])}'

    hoom_ontology = get_ontology(owl_file_path).load()
    annotations = []

    for hoom_class in hoom_ontology.classes():
        if hoom_class.name.startswith('Orpha:'):
            name_components = hoom_class.name.split('_')
            ordo_id = name_components[0].split(':')[1]
            hpo_id = name_components[1].split(':')[1]
            frequency_code = name_components[2].split(':')[1]

            annotations.append(Annotation(
                prefixFrom=ConceptPrefix.ORDO,
                conceptIdFrom=ordo_id,
                prefixTo=ConceptPrefix.HPO,
                conceptIdTo=hpo_id,
                properties={'frequency': frequency_code},
            ))

    await graph_db.save_annotations(
        annotations=annotations
    )


async def delete_annotation_data(graph_db: GraphDatabase = None,
                                 ):
    """
    Delete all HOOM data from the primary databases.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    await graph_db.delete_annotations(
        prefix_1=VOCABULARY_PREFIX_1,
        prefix_2=VOCABULARY_PREFIX_2,
    )
