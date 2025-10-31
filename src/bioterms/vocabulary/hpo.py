import os
import anyio
import networkx as nx
from owlready2 import get_ontology

from bioterms.etc.consts import CONFIG, DOWNLOAD_CLIENT
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory
from bioterms.database import get_active_doc_db, get_active_graph_db
from bioterms.model.concept import Concept


VOCABULARY_PREFIX = ConceptPrefix.HPO
ANNOTATIONS = []
FILE_PATHS = ['hpo/hp.owl']


async def download_vocabulary():
    """
    Download the HPO vocabulary files.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    owl_url = 'https://github.com/obophenotype/human-phenotype-ontology/releases/latest/download/hp.owl'

    async with DOWNLOAD_CLIENT.stream('GET', owl_url, follow_redirects=True) as response:
        response.raise_for_status()

        owl_file_path = os.path.join(CONFIG.data_dir, 'hpo/hp.owl')
        os.makedirs(os.path.dirname(owl_file_path), exist_ok=True)

        async with anyio.open_file(owl_file_path, 'wb') as owl_file:
            async for chunk in response.aiter_bytes():
                await owl_file.write(chunk)


def delete_vocabulary_files():
    """
    Delete the HPO vocabulary files.
    """
    try:
        os.remove(os.path.join(CONFIG.data_dir, 'hpo/hp.owl'))
    except Exception:
        pass


async def load_vocabulary_from_file():
    """
    Load the HPO vocabulary from a file into the primary databases.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('HPO owl file not found')

    owl_file_path = f'file://{os.path.join(CONFIG.data_dir, "hpo/hp.owl")}'

    hpo_ontology = get_ontology(owl_file_path).load()

    hpo_graph = nx.DiGraph()
    concepts = []

    for hpo_class in hpo_ontology.classes():
        if hpo_class.name.startswith('HP_'):
            concept = Concept(
                prefix=ConceptPrefix.HPO,
                conceptTypes=[],
                conceptId=hpo_class.name.split('_')[-1],
                label=hpo_class.label[0]
                    if hasattr(hpo_class, 'label') and hpo_class.label
                    else None,
                definition=hpo_class.IAO_0000115[0]
                    if hasattr(hpo_class, 'IAO_0000115') and hpo_class.IAO_0000115
                    else None,
                comment=hpo_class.comment[0]
                    if hasattr(hpo_class, 'comment') and hpo_class.comment
                    else None,
                status=ConceptStatus.DEPRECATED
                    if hasattr(hpo_class, 'deprecated') and bool(hpo_class.deprecated)
                    else ConceptStatus.ACTIVE,
                synonyms=[],
            )
            concepts.append(concept)

            hpo_graph.add_node(concept.concept_id)
            if hasattr(hpo_class, 'subclasses'):
                for child in hpo_class.subclasses():
                    hpo_graph.add_edge(
                        concept.concept_id,
                        child.name.split('_')[-1],
                        label=ConceptRelationshipType.IS_A
                    )

            if hasattr(hpo_class, 'hasAlternativeId'):
                for replaced_classes in hpo_class.hasAlternativeId:
                    hpo_graph.add_edge(
                        replaced_classes.split(':')[-1],
                        concept.concept_id,
                        label=ConceptRelationshipType.REPLACED_BY
                    )

            if hasattr(hpo_class, 'consider'):
                for replaced_classes in hpo_class.consider:
                    hpo_graph.add_edge(
                        replaced_classes.split(':')[-1],
                        concept.concept_id,
                        label=ConceptRelationshipType.REPLACED_BY
                    )

    doc_db = get_active_doc_db()
    graph_db = get_active_graph_db()

    await doc_db.save_terms(
        label='hpo',
        terms=concepts
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=hpo_graph,
    )


async def create_indexes(overwrite: bool = False):
    """
    Create indexes for the HPO vocabulary in the primary databases.
    :param overwrite: Whether to overwrite existing indexes.
    """

    doc_db = get_active_doc_db()
    graph_db = get_active_graph_db()

    await doc_db.create_index(
        label='hpo',
        field='conceptId',
        unique=True,
        overwrite=overwrite,
    )
    await doc_db.create_index(
        label='hpo',
        field='label',
        overwrite=overwrite,
    )

    await graph_db.create_index()


async def delete_vocabulary_data():
    """
    Delete all HPO vocabulary data from the primary databases.
    """
    doc_db = get_active_doc_db()
    graph_db = get_active_graph_db()

    await doc_db.delete_all_for_label('hpo')
    await graph_db.delete_vocabulary_graph(prefix=ConceptPrefix.HPO)
