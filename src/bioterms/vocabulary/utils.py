import os
import importlib
import json
import io
import csv
import asyncio
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from typing import Optional
import aiofiles
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType, AnnotationType
from bioterms.etc.errors import VocabularyNotLoaded
from bioterms.etc.utils import check_files_exist, edge_iter, batch_iterable, verbose_print
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, VectorDatabase, get_active_cache, \
    get_active_doc_db, get_active_graph_db, get_active_vector_db
from bioterms.database.doc_db.utils import generate_extra_data
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.model.concept import Concept
from bioterms.model.annotation import Annotation


ALL_VOCABULARIES = {
    ConceptPrefix.CTV3: 'ctv3',
    ConceptPrefix.ENSEMBL: 'ensembl',
    ConceptPrefix.HGNC: 'hgnc',
    ConceptPrefix.HGNC_SYMBOL: 'hgnc_symbol',
    ConceptPrefix.HPO: 'hpo',
    ConceptPrefix.MONDO: 'mondo',
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
    with ProcessPoolExecutor(
        max_workers=CONFIG.process_limit,
    ) as executor:
        async with aiofiles.open(offline_file_path, 'w' if overwrite else 'a') as f:
            for batch in batch_iterable(concepts):
                extra_data = await generate_extra_data(
                    concepts=batch,
                    executor=executor,
                )
                extra_data = {
                    concept_id: (ngrams, search_text)
                    for concept_id, ngrams, search_text in extra_data
                }
                for concept in batch:
                    payload = concept.model_dump(exclude_none=True)
                    if concept.concept_id in extra_data:
                        ngrams, search_text = extra_data[concept.concept_id]
                        payload['nGrams'] = ngrams
                        payload['searchText'] = search_text
                    await f.write(json.dumps(payload) + '\n')


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


async def _aiofile_writer_task(path: str,
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
                              concepts: list[Concept],
                              vocabulary_graph: nx.DiGraph | nx.MultiDiGraph,
                              overwrite: bool = True,
                              ):
    """
    Write the given vocabulary graph to an offline file for the specified vocabulary prefix.
    :param prefix: The vocabulary prefix.
    :param concepts: The list of concepts in the vocabulary.
    :param vocabulary_graph: The vocabulary graph to write.
    :param overwrite: Whether to overwrite the existing file.
    """
    offline_dir = os.path.join(CONFIG.data_dir, 'offline')
    if not os.path.exists(offline_dir):
        os.makedirs(offline_dir, exist_ok=True)

    offline_file_path = os.path.join(offline_dir, f'{prefix.value}.graph.dump')
    offline_node_id_path = os.path.join(offline_dir, f'{prefix.value}.node_ids.dump')
    if overwrite:
        async with aiofiles.open(offline_file_path, 'w') as f:
            await f.write('')
        async with aiofiles.open(offline_node_id_path, 'w') as f:
            await f.write('')

    q: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=8)
    wt = asyncio.create_task(_aiofile_writer_task(offline_file_path, q))
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

    q = asyncio.Queue(maxsize=8)
    wt = asyncio.create_task(_aiofile_writer_task(offline_node_id_path, q))

    for i in range(0, len(concepts), 10000):
        batch = [c.model_dump() for c in concepts[i:i + 10000]]
        rows = [
            (concept['conceptId'], concept['conceptTypes'])
            for concept in batch
        ]
        chunk = _encode_csv_batch(rows, csv_kwargs)
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
    :param annotation_file_path: Explicit dump path override. Rows outside the
        requested prefix pair are filtered from the returned graph.
    :param annotation_file_path: Explicit dump path override. Rows outside the
        requested prefix pair are filtered from the returned graph.
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
    wt = asyncio.create_task(_aiofile_writer_task(offline_file_path, q))
    csv_kwargs = {
        'quoting': csv.QUOTE_MINIMAL,
        'lineterminator': '\n',
    }

    for i in range(0, len(annotations), 10000):
        batch = annotations[i:i + 10000]
        rows = [
            (
                _prefix_value(ann.prefix_from),
                normalise_annotation_curie(ann.prefix_from, ann.concept_id_from),
                _prefix_value(ann.prefix_to),
                normalise_annotation_curie(ann.prefix_to, ann.concept_id_to),
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


def _prefix_value(prefix: ConceptPrefix | str) -> str:
    """Return a prefix's serialized value."""
    if isinstance(prefix, ConceptPrefix):
        return prefix.value
    value = str(prefix).strip()
    try:
        return ConceptPrefix(value.casefold()).value
    except ValueError:
        return value


def _is_annotation_prefix(value: str,
                          declared_prefix: str = '',
                          ) -> bool:
    """Whether text before a colon is a vocabulary prefix rather than ID text.

    For external/unpacked vocabularies not represented by ``ConceptPrefix``, only
    the row's exact declared prefix is recognized. Other colon-containing values
    are opaque local identifiers and do not participate in conflict checking.
    """
    value = value.strip().casefold()
    declared_prefix = declared_prefix.strip().casefold()
    predefined_prefixes = {prefix.value.casefold() for prefix in ConceptPrefix}
    if not value:
        return False
    if declared_prefix and declared_prefix not in predefined_prefixes:
        return value == declared_prefix
    return value in predefined_prefixes


def normalise_annotation_curie(prefix: ConceptPrefix | str,
                               concept_id: str,
                               ) -> str:
    """Return a canonical CURIE, accepting either a local ID or an existing CURIE.

    Prefix comparison is case-insensitive, which converts source values such as
    ``HGNC:5`` to the service's canonical ``hgnc:5`` representation.
    """
    prefix_value = _prefix_value(prefix).strip()
    concept_id = str(concept_id).strip()
    if not prefix_value:
        raise ValueError('Annotation prefix cannot be empty.')
    if not concept_id:
        raise ValueError('Annotation concept ID cannot be empty.')

    if ':' in concept_id:
        embedded_prefix, local_id = concept_id.split(':', 1)
        if _is_annotation_prefix(embedded_prefix, prefix_value):
            if embedded_prefix.casefold() != prefix_value.casefold():
                raise ValueError(
                    f'Annotation concept ID {concept_id!r} conflicts with prefix {prefix_value!r}.'
                )
            concept_id = local_id
    if not concept_id:
        raise ValueError('Annotation CURIE cannot have an empty local ID.')
    return f'{prefix_value}:{concept_id}'


def parse_annotation_curie(prefix: ConceptPrefix | str | None,
                           concept_id: str,
                           fallback_prefix: ConceptPrefix | str | None = None,
                           ) -> str:
    """Parse legacy or normalized annotation columns into one canonical CURIE.

    ``concept_id`` may be a local ID or a CURIE. The separate prefix column may
    be populated or empty. When both are absent, ``fallback_prefix`` supplies the
    vocabulary requested by the caller.
    """
    explicit_prefix = _prefix_value(prefix).strip() if prefix is not None else ''
    fallback_value = _prefix_value(fallback_prefix).strip() if fallback_prefix is not None else ''
    concept_id = str(concept_id).strip()
    embedded_prefix = ''
    local_id = concept_id
    selected_prefix = explicit_prefix or fallback_value
    if ':' in concept_id:
        candidate_prefix, candidate_local_id = concept_id.split(':', 1)
        if _is_annotation_prefix(candidate_prefix, selected_prefix):
            embedded_prefix = candidate_prefix
            local_id = candidate_local_id

    selected_prefix = selected_prefix or embedded_prefix
    if not selected_prefix:
        raise ValueError(f'Cannot determine annotation prefix for concept ID {concept_id!r}.')
    if embedded_prefix and embedded_prefix.casefold() != selected_prefix.casefold():
        raise ValueError(
            f'Annotation CURIE {concept_id!r} conflicts with prefix {selected_prefix!r}.'
        )
    return normalise_annotation_curie(selected_prefix, local_id)


async def load_graph_from_file(prefix: ConceptPrefix,
                               ) -> nx.DiGraph | nx.MultiDiGraph:
    """
    Load the vocabulary graph from an offline file for the specified vocabulary prefix.
    :param prefix: The vocabulary prefix.
    :return: The vocabulary graph.
    """
    offline_file_path = os.path.join(CONFIG.data_dir, 'offline', f'{prefix.value}.graph.dump')
    offline_node_id_path = os.path.join(CONFIG.data_dir, 'offline', f'{prefix.value}.node_ids.dump')
    if not os.path.exists(offline_file_path):
        raise FileNotFoundError(f'Offline graph file for {prefix.value} not found.')

    # Peak into the file to see if the first row, column 4 has key
    has_edge_properties = False
    async with aiofiles.open(offline_file_path) as f:
        first_line = await f.readline()
        first_row = next(csv.reader([first_line]))
        if len(first_row) >= 4:
            try:
                json.loads(first_row[3])
                has_edge_properties = True
            except json.JSONDecodeError:
                has_edge_properties = False

    if has_edge_properties:
        graph = nx.MultiDiGraph()
    else:
        graph = nx.DiGraph()

    async with aiofiles.open(offline_node_id_path) as f:
        async for line in f:
            row = next(csv.reader([line]))
            concept_id = row[0]
            graph.add_node(concept_id)

    async with aiofiles.open(offline_file_path) as f:
        async for line in f:
            row = next(csv.reader([line]))
            source_id, target_id, relationship_type, edge_key = row

            if has_edge_properties:
                graph.add_edge(
                    source_id,
                    target_id,
                    key=edge_key,
                    label=ConceptRelationshipType(relationship_type),
                )
            else:
                graph.add_edge(
                    source_id,
                    target_id,
                    label=ConceptRelationshipType(relationship_type),
                )

    return graph


async def load_annotation_from_file(prefix_from: ConceptPrefix,
                                    prefix_to: ConceptPrefix | None = None,
                                    annotation_file_path: str | os.PathLike | None = None,
                                    ) -> nx.DiGraph:
    """
    Load the annotation graph from an offline file for the specified vocabulary prefix.
    :param prefix_from: The vocabulary prefix of the source concepts.
    :param prefix_to: The vocabulary prefix of the target concepts.
    :return: The annotation graph.
    """
    if annotation_file_path is not None:
        offline_file_path = os.fspath(annotation_file_path)
        if not os.path.exists(offline_file_path):
            raise FileNotFoundError(f'Offline annotation file not found: {offline_file_path}')
    else:
        offline_file_path = os.path.join(
            CONFIG.data_dir,
            'offline',
            f'{prefix_from.value}{("-" + prefix_to.value if prefix_to is not None else "")}.annotation.dump'
        )
    if annotation_file_path is None and not os.path.exists(offline_file_path):
        if prefix_to is not None:
            offline_file_path = os.path.join(
                CONFIG.data_dir,
                'offline',
                f'{prefix_to.value}-{prefix_from.value}.annotation.dump'
            )
            if not os.path.exists(offline_file_path):
                raise FileNotFoundError(
                    f'Offline annotation file for {prefix_from.value}-{prefix_to.value} not found.'
                )
        else:
            raise FileNotFoundError(
                f'Offline annotation file for {prefix_from.value} '
                f'{("-" + prefix_to.value if prefix_to is not None else "")} not found.'
            )

    graph = nx.DiGraph()
    requested_prefixes = (
        frozenset((prefix_from.value, prefix_to.value))
        if prefix_to is not None else None
    )
    loaded_count = 0
    filtered_count = 0

    async with aiofiles.open(offline_file_path) as f:
        async for line in f:
            row = next(csv.reader([line]))

            if len(row) < 6:
                # Skip malformed rows instead of failing the whole load.
                continue

            source_prefix, source_id, target_prefix, target_id, annotation_type, properties_str = row
            source_curie = parse_annotation_curie(source_prefix, source_id, prefix_from)
            target_curie = parse_annotation_curie(target_prefix, target_id, prefix_to)
            if requested_prefixes is not None:
                edge_prefixes = frozenset((
                    source_curie.split(':', 1)[0],
                    target_curie.split(':', 1)[0],
                ))
                if edge_prefixes != requested_prefixes:
                    filtered_count += 1
                    continue
            graph.add_edge(
                source_curie,
                target_curie,
                label=AnnotationType(annotation_type),
                properties=json.loads(properties_str),
            )
            loaded_count += 1

    verbose_print(
        f'Loaded {loaded_count} annotations from {offline_file_path}; '
        f'filtered out {filtered_count} outside the requested prefix pair.'
    )

    return graph
