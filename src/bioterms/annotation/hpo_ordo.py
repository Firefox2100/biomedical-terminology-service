import os
import httpx
from owlready2 import get_ontology

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, iter_progress, \
    verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from .utils import AnnotationSource, assert_pre_requisite, download_hpoa, hpoa_file_path, \
    load_hpoa_annotations


ANNOTATION_NAME = 'HPO and ORDO phenotype annotations'
VOCABULARY_PREFIX_1 = ConceptPrefix.ORDO
VOCABULARY_PREFIX_2 = ConceptPrefix.HPO
FILE_PATHS = ['hoom/hoom_orphanet.owl', hpoa_file_path()]
_HOOM = AnnotationSource('HOOM', ConceptPrefix.ORDO, ConceptPrefix.HPO)


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the HOOM files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if not check_files_exist([FILE_PATHS[0]]):
        ensure_data_directory()

        if not CONFIG.bioportal_api_key:
            raise ValueError('BioPortal API key is required to download HOOM mapping.')

        await download_file(
            url='https://data.bioontology.org/ontologies/HOOM/download',
            file_path=FILE_PATHS[0],
            headers={'Authorization': f'apikey token={CONFIG.bioportal_api_key}'},
            download_client=download_client,
        )

    await download_hpoa(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the HOOM mapping from a file into the primary databases.
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

    owl_file_path = f'file://{os.path.join(CONFIG.data_dir, FILE_PATHS[0])}'

    hoom_ontology = get_ontology(owl_file_path).load()
    hoom_classes = list(hoom_ontology.classes())
    annotations = []

    verbose_print('HOOM ontology file loaded from disk. Processing annotations...')

    for hoom_class in iter_progress(
        hoom_classes,
        description='Processing HOOM classes',
        total=len(hoom_classes)
    ):
        if hoom_class.name.startswith('Orpha:'):
            name_components = hoom_class.name.split('_')
            ordo_id = name_components[0].split(':')[1]
            hpo_id = name_components[1].split(':')[1]
            frequency_code = name_components[2].split(':')[1]

            annotations.append(_HOOM.create(
                publisher_concept_id=ordo_id,
                other_concept_id=hpo_id,
                properties={'frequency': frequency_code},
            ))

    annotations.extend(load_hpoa_annotations('ORPHA', ConceptPrefix.ORDO))

    verbose_print(
        f'Processed {len(annotations)} HOOM and HPO-published annotations. Saving to database...'
    )

    await graph_db.save_annotations(
        annotations=annotations
    )
