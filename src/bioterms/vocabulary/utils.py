import os
import importlib
from datetime import datetime
import aiofiles

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import VocabularyNotLoaded
from bioterms.etc.utils import check_files_exist
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, get_active_cache, \
    get_active_doc_db, get_active_graph_db
from bioterms.model.vocabulary_status import VocabularyStatus


ALL_VOCABULARIES = {
    ConceptPrefix.CTV3: 'ctv3',
    ConceptPrefix.ENSEMBL: 'ensembl',
    ConceptPrefix.HGNC: 'hgnc',
    ConceptPrefix.HGNC_SYMBOL: 'hgnc_symbol',
    ConceptPrefix.HPO: 'hpo',
    ConceptPrefix.NCIT: 'ncit',
    ConceptPrefix.OMIM: 'omim',
    ConceptPrefix.ORDO: 'ordo',
    ConceptPrefix.REACTOME: 'reactome',
    ConceptPrefix.SNOMED: 'snomed',
}


def get_vocabulary_module(prefix: ConceptPrefix):
    """
    Get the vocabulary module for the given prefix.
    :param prefix: The prefix of the vocabulary.
    :return: The vocabulary module.
    """
    vocabulary_module_name = ALL_VOCABULARIES.get(prefix)
    if not vocabulary_module_name:
        raise ValueError(f'Vocabulary with prefix {prefix} not found.')

    vocabulary_module = importlib.import_module(f'bioterms.vocabulary.{vocabulary_module_name}')

    return vocabulary_module


async def get_vocabulary_status(prefix: ConceptPrefix,
                                cache: Cache = None,
                                doc_db: DocumentDatabase = None,
                                graph_db: GraphDatabase = None,
                                ) -> VocabularyStatus:
    """
    Get the status of the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :return: The vocabulary status.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if cache is None:
        cache = get_active_cache()

    cached_status = await cache.get_vocabulary_status(prefix)
    if cached_status is not None:
        return cached_status

    if doc_db is None:
        doc_db = await get_active_doc_db()

    if graph_db is None:
        graph_db = get_active_graph_db()

    concept_count = await doc_db.count_terms(prefix)
    relationship_count = await graph_db.count_internal_relationships(prefix)
    annotations = vocabulary_module.ANNOTATIONS
    downloaded = check_files_exist(vocabulary_module.FILE_PATHS)

    try:
        timestamp_file_path = os.path.join(CONFIG.data_dir, vocabulary_module.TIMESTAMP_FILE)
        async with aiofiles.open(timestamp_file_path) as f:
            timestamp_str = await f.read()
            download_time = datetime.fromisoformat(timestamp_str.strip())
    except (FileNotFoundError, ValueError):
        download_time = None

    status = VocabularyStatus(
        prefix=prefix,
        name=vocabulary_module.VOCABULARY_NAME,
        fileDownloaded=downloaded,
        fileDownloadTime=download_time if downloaded else None,
        loaded=concept_count > 0,
        conceptCount=concept_count,
        relationshipCount=relationship_count,
        annotations=annotations,
        similarityMethods=vocabulary_module.SIMILARITY_METHODS,
    )

    await cache.save_vocabulary_status(status)

    return status


async def ensure_gene_symbol_loaded(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Ensure that the HGNC gene symbol vocabulary is loaded.

    This is usually the prerequisite for loading other gene-related vocabularies, because they all
    map to the HGNC gene symbols.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :raises VocabularyNotLoaded: If the HGNC vocabulary is not loaded.
    """
    status = await get_vocabulary_status(
        prefix=ConceptPrefix.HGNC_SYMBOL,
        doc_db=doc_db,
        graph_db=graph_db,
    )

    if not status.loaded:
        raise VocabularyNotLoaded('HGNC gene symbol vocabulary is not loaded.')
