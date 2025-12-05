import os
import json
import httpx
import aiofiles

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, extract_file_from_tarball, \
    iter_progress, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation
from .utils import assert_pre_requisite


ANNOTATION_NAME = 'ORDO - OMIM Alignment Data'
VOCABULARY_PREFIX_1 = ConceptPrefix.ORDO
VOCABULARY_PREFIX_2 = ConceptPrefix.OMIM
FILE_PATHS = ['ordo/alignment.json']


async def download_annotation(download_client: httpx.AsyncClient = None):
    """
    Download the ORDO OMIM alignment file.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    mapping_url = 'https://www.orphadata.com/data/json/en_product1.json.tar.gz'
    tarball_path = 'ordo/omim_alignment.json.tar.gz'

    try:
        await download_file(
            url=mapping_url,
            file_path=tarball_path,
            download_client=download_client,
        )

        await extract_file_from_tarball(
            tarball_path=os.path.join(CONFIG.data_dir, tarball_path),
            output_dir=os.path.join(CONFIG.data_dir, 'ordo'),
            members=['en_product1.json'],
        )

        os.rename(
            os.path.join(CONFIG.data_dir, 'ordo/en_product1.json'),
            os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        )
    finally:
        try:
            os.remove(os.path.join(CONFIG.data_dir, tarball_path))
        except Exception:
            pass


async def load_annotation_from_file(graph_db: GraphDatabase = None,
                                    ):
    """
    Load the ORDO OMIM alignment from a file into the primary databases.
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

    file_path = os.path.join(CONFIG.data_dir, FILE_PATHS[0])
    async with aiofiles.open(file_path) as f:
        alignment_data = await f.read()

    alignment_json = json.loads(alignment_data)

    verbose_print('ORDO alignment file loaded from disk. Processing annotations...')

    annotations = []
    disorders = alignment_json['JDBOR'][0]['DisorderList'][0]['Disorder']

    for disorder in iter_progress(
        disorders,
        description='Processing ORDO-OMIM alignments',
        total=len(disorders),
    ):
        ordo_id = disorder['OrphaCode']

        for l in disorder['ExternalReferenceList']:
            if int(l['count']) == 0:
                continue

            for reference in l['ExternalReference']:
                if reference['Source'] == 'OMIM':
                    omim_id = reference['Reference']

                    annotations.append(Annotation(
                        prefixFrom=VOCABULARY_PREFIX_1,
                        prefixTo=VOCABULARY_PREFIX_2,
                        conceptIdFrom=ordo_id,
                        conceptIdTo=omim_id,
                    ))

    verbose_print(f'Processed {len(annotations)} ORDO-OMIM annotations. Saving to database...')

    await graph_db.save_annotations(
        annotations=annotations
    )
