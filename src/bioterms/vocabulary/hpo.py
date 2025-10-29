import os
import networkx as nx
from owlready2 import get_ontology

from bioterms.etc.consts import CONFIG, DOWNLOAD_CLIENT
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory
from bioterms.database import get_active_doc_db
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

        with open(owl_file_path, 'wb') as owl_file:
            async for chunk in response.aiter_bytes():
                owl_file.write(chunk)


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
    replaced_by_graph = nx.DiGraph()
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
                    hpo_graph.add_edge(concept.concept_id, child.name.split('_')[-1])

            if hasattr(hpo_class, 'hasAlternativeId'):
                for replaced_classes in hpo_class.hasAlternativeId:
                    replaced_by_graph.add_edge(replaced_classes.split(':')[-1], concept.concept_id)

            if hasattr(hpo_class, 'consider'):
                for replaced_classes in hpo_class.consider:
                    replaced_by_graph.add_edge(replaced_classes.split(':')[-1], concept.concept_id)

    doc_db = get_active_doc_db()

    await doc_db.save_terms(
        label='hpo',
        terms=concepts
    )
