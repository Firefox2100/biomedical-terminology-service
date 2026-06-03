import os
import io
import csv
import importlib
import aiofiles
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.etc.utils import verbose_print
from bioterms.database import Cache, DocumentDatabase, GraphDatabase, get_active_cache, get_active_doc_db, \
    get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status
from bioterms.vocabulary.utils import load_graph_from_file, load_annotation_from_file
from bioterms.annotation import get_annotation_status
from bioterms.model.similarity_status import SimilarityCount, SimilarityStatus


ALL_SIMILARITY_METHODS = {
    SimilarityMethod.CO_ANNOTATION: 'co_annotation',
    SimilarityMethod.RELEVANCE: 'relevance',
    SimilarityMethod.WEIGHED_RELEVANCE: 'relevance_weight',
}


def get_similarity_module(method: SimilarityMethod):
    """
    Get the similarity module for the given method.
    :param method: The similarity method.
    :return: The similarity module.
    """
    similarity_module_name = ALL_SIMILARITY_METHODS.get(method)
    if not similarity_module_name:
        raise ValueError(f'Similarity method {method} not found.')

    similarity_module = importlib.import_module(f'bioterms.similarity.{similarity_module_name}')

    return similarity_module


def get_similarity_method_config(method: SimilarityMethod) -> dict:
    """
    Get the similarity method configuration for the given method.
    :param method: The similarity method.
    :return: The similarity method configuration.
    """
    similarity_module = get_similarity_module(method)

    return {
        'name': similarity_module.METHOD_NAME,
        'defaultThreshold': similarity_module.DEFAULT_SIMILARITY_THRESHOLD,
        'corpusRequired': similarity_module.CORPUS_REQUIRED,
        'corpusGraphRequired': similarity_module.CORPUS_GRAPH_REQUIRED,
    }


def get_all_similarity_combinations(annotations: list[ConceptPrefix],
                                    similarity_methods: list[SimilarityMethod],
                                    ) -> list[tuple[SimilarityMethod, ConceptPrefix | None]]:
    """
    Get all similarity combinations for the given prefix, annotations, and similarity methods.
    :param annotations: The prefixes of the vocabularies that are used to annotate the target vocabulary.
    :param similarity_methods: The similarity methods supported by the target vocabulary.
    :return: A list of tuples containing the similarity method and the corpus prefix
        (or None for intrinsic similarity).
    """
    combinations = []

    if SimilarityMethod.RELEVANCE in similarity_methods:
        combinations.extend([
            (SimilarityMethod.RELEVANCE, annotation_prefix)
            for annotation_prefix in annotations
        ])
    if SimilarityMethod.CO_ANNOTATION in similarity_methods:
        combinations.extend([
            (SimilarityMethod.CO_ANNOTATION, annotation_prefix)
            for annotation_prefix in annotations
        ])
    if SimilarityMethod.WEIGHED_RELEVANCE in similarity_methods:
        combinations.extend([
            (SimilarityMethod.WEIGHED_RELEVANCE, annotation_prefix)
            for annotation_prefix in annotations
        ])

    return combinations


async def calculate_similarity(method: SimilarityMethod,
                               target_prefix: ConceptPrefix,
                               corpus_prefix: ConceptPrefix = None,
                               similarity_threshold: float | None = None,
                               offline: bool = False,
                               doc_db: DocumentDatabase = None,
                               graph_db: GraphDatabase = None,
                               ):
    """
    Calculate similarity between two vocabularies using the specified method.
    :param method: The similarity calculation method to use.
    :param target_prefix: The target vocabulary prefix.
    :param corpus_prefix: The corpus vocabulary prefix.
    :param similarity_threshold: The similarity threshold to apply.
        If None, use the default threshold for the method.
    :param offline: Whether to run the calculation in offline mode.
    :param doc_db: The document database instance to use. If None, use the active document database.
    :param graph_db: The graph database instance to use. If None, use the active graph database.
    """
    similarity_module = get_similarity_module(method)
    similarity_config = get_similarity_method_config(method)
    cache = get_active_cache()

    if similarity_threshold is None:
        similarity_threshold = similarity_config['defaultThreshold']

    if not offline:
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        if not (await get_vocabulary_status(
            target_prefix,
            doc_db=doc_db,
            graph_db=graph_db
        )).loaded:
            raise ValueError(f'Target vocabulary {target_prefix.value} is not loaded.')

        if similarity_config['corpusRequired'] and corpus_prefix is None:
            raise ValueError(f'Similarity method {method} requires a corpus prefix.')

        if corpus_prefix is not None and similarity_config['corpusRequired']:
            if not (await get_vocabulary_status(
                corpus_prefix,
                doc_db=doc_db,
                graph_db=graph_db
            )).loaded:
                raise ValueError(f'Corpus vocabulary {corpus_prefix.value} is not loaded.')

            if not (await get_annotation_status(
                prefix_1=target_prefix,
                prefix_2=corpus_prefix,
                graph_db=graph_db,
            )).loaded:
                raise ValueError(
                    f'Annotation between {target_prefix.value} and {corpus_prefix.value} '
                    f'is not loaded.'
                )

    verbose_print(f'Loading vocabulary graph for {target_prefix.value}...')

    if offline:
        target_graph = await load_graph_from_file(target_prefix)
        if not isinstance(target_graph, nx.MultiDiGraph):
            target_graph = nx.MultiDiGraph(target_graph)
    else:
        target_graph = await graph_db.get_vocabulary_graph(target_prefix)
    if corpus_prefix is not None and similarity_config['corpusRequired']:
        verbose_print(f'Loading annotation graph between {target_prefix.value} and {corpus_prefix.value}...')
        if offline:
            annotation_graph = await load_annotation_from_file(
                prefix_from=target_prefix,
                prefix_to=corpus_prefix,
            )
        else:
            annotation_graph = await graph_db.get_annotation_graph(
                prefix_1=target_prefix,
                prefix_2=corpus_prefix,
            )

        if similarity_config['corpusGraphRequired']:
            verbose_print(f'Loading corpus vocabulary graph for {corpus_prefix.value}...')
            if offline:
                corpus_graph = await load_graph_from_file(corpus_prefix)
                if not isinstance(corpus_graph, nx.MultiDiGraph):
                    corpus_graph = nx.MultiDiGraph(corpus_graph)
            else:
                corpus_graph = await graph_db.get_vocabulary_graph(corpus_prefix)
        else:
            corpus_graph = None
    else:
        corpus_graph = None
        annotation_graph = None

    results = []
    offline_file_path = os.path.join(
        CONFIG.data_dir,
        'offline',
        f'{target_prefix.value}-{method}{("-" + corpus_prefix.value) if corpus_prefix else ""}.similarity.dump'
    )

    async def write_batch_to_file(f):
        if not results:
            return

        buf = io.StringIO()
        w = csv.writer(
            buf,
            lineterminator='\n',
        )
        w.writerows(results)
        await f.write(buf.getvalue())
        buf.close()

    if offline:
        offline_file = await aiofiles.open(offline_file_path, mode='w')
    else:
        offline_file = None

    try:
        async for result in similarity_module.calculate_similarity(
            target_graph=target_graph,
            target_prefix=target_prefix,
            corpus_graph=corpus_graph,
            corpus_prefix=corpus_prefix,
            annotation_graph=annotation_graph,
        ):
            if result[2] >= similarity_threshold:
                results.append(result)

            if len(results) >= 10000:
                if offline and offline_file is not None:
                    await write_batch_to_file(offline_file)
                else:
                    await graph_db.save_similarity_scores(
                        prefix_from=target_prefix,
                        prefix_to=target_prefix,
                        similarity_scores=results,
                        similarity_method=method,
                        corpus_prefix=corpus_prefix,
                    )
                results.clear()

        if results:
            if offline and offline_file is not None:
                await write_batch_to_file(offline_file)
            else:
                await graph_db.save_similarity_scores(
                    prefix_from=target_prefix,
                    prefix_to=target_prefix,
                    similarity_scores=results,
                    similarity_method=method,
                    corpus_prefix=corpus_prefix,
                )
    finally:
        if offline and offline_file is not None:
            await offline_file.close()

        await cache.rotate_dataset_version()


async def get_similarity_status(prefix: ConceptPrefix,
                                cache: Cache = None,
                                doc_db: DocumentDatabase = None,
                                graph_db: GraphDatabase = None,
                                use_cache: bool = True,
                                ) -> SimilarityStatus:
    """
    Get the similarity status for the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary.
    :param cache: The cache instance.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :param use_cache: Whether to use the cache. Defaults to True.
    :return: The SimilarityStatus object containing similarity counts.
    """
    if cache is None:
        cache = get_active_cache()

    if use_cache:
        cached_status = await cache.get_similarity_status(prefix)
        if cached_status is not None:
            return cached_status

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    vocab_status = await get_vocabulary_status(
        prefix=prefix,
        cache=cache,
        doc_db=doc_db,
        graph_db=graph_db,
        use_cache=use_cache,
    )

    annotations = vocab_status.annotations
    supported_methods = vocab_status.similarity_methods

    similarity_combinations = get_all_similarity_combinations(
        annotations=annotations,
        similarity_methods=supported_methods,
    )

    similarity_counts = await graph_db.count_similarity_relationships(
        prefix_from=prefix,
        prefix_to=prefix,
        configurations=similarity_combinations,
    )

    status = SimilarityStatus(
        prefix=prefix,
        similarityCounts=[
            SimilarityCount(
                method=count[0],
                corpus=count[1],
                count=count[2],
            )
            for count in similarity_counts
        ],
    )

    await cache.save_similarity_status(
        status=status,
    )

    return status
