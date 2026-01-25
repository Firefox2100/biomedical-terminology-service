import os
import importlib
import json
import io
import csv
import asyncio
from datetime import datetime
from typing import Optional
import aiofiles
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import VocabularyNotLoaded
from bioterms.etc.utils import check_files_exist, edge_iter
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, VectorDatabase, get_active_cache, \
    get_active_doc_db, get_active_graph_db, get_active_vector_db
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.model.concept import Concept
from bioterms.model.annotation import Annotation


ALL_VOCABULARIES = {
    ConceptPrefix.CTV3: 'ctv3',
    ConceptPrefix.ENSEMBL: 'ensembl',
    ConceptPrefix.HGNC: 'hgnc',
    ConceptPrefix.HGNC_SYMBOL: 'hgnc_symbol',
    ConceptPrefix.HPO: 'hpo',
    ConceptPrefix.NCIT: 'ncit',
    ConceptPrefix.OHDSI: 'ohdsi',
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
                                vector_db: VectorDatabase = None,
                                use_cache: bool = True,
                                ) -> VocabularyStatus:
    """
    Get the status of the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param vector_db: The vector database instance.
    :param use_cache: Whether to use the cache. Defaults to True.
    :return: The vocabulary status.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if cache is None:
        cache = get_active_cache()

    if use_cache:
        cached_status = await cache.get_vocabulary_status(prefix)
        if cached_status is not None:
            return cached_status

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()
    if vector_db is None:
        vector_db = get_active_vector_db()

    concept_count = await doc_db.count_terms(prefix)
    relationship_count = await graph_db.count_internal_relationships(prefix)
    vector_count = await vector_db.count_vectors(prefix)
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
        vectorCount=vector_count,
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


async def write_concepts_to_file(prefix: ConceptPrefix,
                                 concepts: list[Concept],
                                 overwrite: bool = True,
                                 ):
    """
    Write the given concepts to an offline file for the specified vocabulary prefix.
    :param prefix: The vocabulary prefix.
    :param concepts: The list of concepts to write.
    :param overwrite: Whether to overwrite the existing file.
    """
    offline_dir = os.path.join(CONFIG.data_dir, 'offline')
    if not os.path.exists(offline_dir):
        os.makedirs(offline_dir, exist_ok=True)

    offline_file_path = os.path.join(offline_dir, f'{prefix.value}.doc.dump')
    async with aiofiles.open(offline_file_path, 'w' if overwrite else 'a') as f:
        for concept in concepts:
            await f.write(json.dumps(concept.model_dump()) + '\n')


def _encode_csv_batch(rows: list[tuple],
                      csv_kwargs: dict,
                      ) -> str:
    """
    Encode a batch of edge rows to CSV format using the given CSV writer arguments.
    :param rows: The list of edge rows to encode.
    :param csv_kwargs: The CSV writer arguments.
    :return: The CSV-encoded string.
    """
    buf = io.StringIO()
    w = csv.writer(buf, **csv_kwargs)
    w.writerows(rows)
    return buf.getvalue()


async def _edge_writer_task(path: str,
                            q: asyncio.Queue[Optional[str]],
                            *,
                            encoding: str = 'utf-8',
                            newline: str = '',
                            ) -> None:
    async with aiofiles.open(path, 'a', encoding=encoding, newline=newline) as f:
        while True:
            chunk = await q.get()
            try:
                if chunk is None:
                    return
                await f.write(chunk)
            finally:
                q.task_done()


async def write_graph_to_file(prefix: ConceptPrefix,
                              vocabulary_graph: nx.DiGraph | nx.MultiDiGraph,
                              overwrite: bool = True,
                              ):
    """
    Write the given vocabulary graph to an offline file for the specified vocabulary prefix.
    :param prefix: The vocabulary prefix.
    :param vocabulary_graph: The vocabulary graph to write.
    :param overwrite: Whether to overwrite the existing file.
    """
    offline_dir = os.path.join(CONFIG.data_dir, 'offline')
    if not os.path.exists(offline_dir):
        os.makedirs(offline_dir, exist_ok=True)

    offline_file_path = os.path.join(offline_dir, f'{prefix.value}.graph.dump')
    if overwrite:
        async with aiofiles.open(offline_file_path, 'w') as f:
            await f.write('')

    q: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=8)
    wt = asyncio.create_task(_edge_writer_task(offline_file_path, q))
    csv_kwargs = {
        'quoting': csv.QUOTE_MINIMAL,
        'lineterminator': '\n',
    }

    batch = []
    for r in edge_iter(vocabulary_graph):
        batch.append(r)
        if len(batch) >= 50000:
            chunk = _encode_csv_batch(batch, csv_kwargs)
            batch.clear()
            await q.put(chunk)

    if batch:
        chunk = _encode_csv_batch(batch, csv_kwargs)
        await q.put(chunk)

    await q.join()
    await q.put(None)
    await wt


async def write_annotations_to_file(prefix_from: ConceptPrefix,
                                    annotations: list[Annotation],
                                    prefix_to: ConceptPrefix | None = None,
                                    overwrite: bool = True,
                                    ):
    """
    Write the given annotations to an offline file for the specified vocabulary prefix.
    :param prefix_from: The vocabulary prefix of the source concepts.
    :param prefix_to: The vocabulary prefix of the target concepts.
    :param annotations: The list of annotations to write.
    :param overwrite: Whether to overwrite the existing file.
    """
    offline_dir = os.path.join(CONFIG.data_dir, 'offline')
    if not os.path.exists(offline_dir):
        os.makedirs(offline_dir, exist_ok=True)

    offline_file_path = os.path.join(
        offline_dir,
        f'{prefix_from.value}{("-" + prefix_to.value if prefix_to is not None else "")}.annotation.dump'
    )
    if overwrite:
        async with aiofiles.open(offline_file_path, 'w') as f:
            await f.write('')

    q: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=8)
    wt = asyncio.create_task(_edge_writer_task(offline_file_path, q))
    csv_kwargs = {
        'quoting': csv.QUOTE_MINIMAL,
        'lineterminator': '\n',
    }

    for i in range(0, len(annotations), 10000):
        batch = annotations[i:i + 10000]
        rows = [
            (
                ann.prefix_from.value if isinstance(ann.prefix_from, ConceptPrefix) else ann.prefix_from,
                ann.concept_id_from,
                ann.prefix_to.value if isinstance(ann.prefix_to, ConceptPrefix) else ann.prefix_to,
                ann.concept_id_to,
                ann.annotation_type.value,
                json.dumps(ann.properties) if ann.properties else '{}',
            )
            for ann in batch
        ]
        chunk = _encode_csv_batch(rows, csv_kwargs)
        await q.put(chunk)

    await q.join()
    await q.put(None)
    await wt
