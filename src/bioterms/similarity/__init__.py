import importlib

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status
from bioterms.annotation import get_annotation_status


ALL_SIMILARITY_METHODS = {
    SimilarityMethod.CO_ANNOTATION: 'co_annotation',
    SimilarityMethod.RELEVANCE: 'relevance',
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
    }


async def calculate_similarity(method: SimilarityMethod,
                               target_prefix: ConceptPrefix,
                               corpus_prefix: ConceptPrefix = None,
                               similarity_threshold: float | None = None,
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
    :param doc_db: The document database instance to use. If None, use the active document database.
    :param graph_db: The graph database instance to use. If None, use the active graph database.
    """
    similarity_module = get_similarity_module(method)

    if similarity_threshold is None:
        similarity_threshold = similarity_module.DEFAULT_SIMILARITY_THRESHOLD
    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    if not (await get_vocabulary_status(target_prefix, doc_db=doc_db, graph_db=graph_db)).loaded:
        raise ValueError(f'Target vocabulary {target_prefix.value} is not loaded.')
    if not (await get_vocabulary_status(corpus_prefix, doc_db=doc_db, graph_db=graph_db)).loaded:
        raise ValueError(f'Corpus vocabulary {corpus_prefix.value} is not loaded.')
    if not (await get_annotation_status(
        prefix_1=target_prefix,
        prefix_2=corpus_prefix,
        graph_db=graph_db,
    )).loaded:
        raise ValueError(
            f'Annotation between {target_prefix.value} and {corpus_prefix.value} is not loaded.'
        )

    target_graph = await graph_db.get_vocabulary_graph(target_prefix)
    if corpus_prefix is not None:
        corpus_graph = await graph_db.get_vocabulary_graph(corpus_prefix)
        annotation_graph = await graph_db.get_annotation_graph(
            prefix_1=target_prefix,
            prefix_2=corpus_prefix,
        )
    else:
        corpus_graph = None
        annotation_graph = None

    similarity_df = await similarity_module.calculate_similarity(
        target_graph=target_graph,
        target_prefix=target_prefix,
        corpus_graph=corpus_graph,
        corpus_prefix=corpus_prefix,
        annotation_graph=annotation_graph,
    )

    similarity_df = similarity_df[similarity_df['similarity'] >= similarity_threshold]

    await graph_db.save_similarity_scores(
        prefix_from=target_prefix,
        prefix_to=target_prefix,
        similarity_df=similarity_df,
        similarity_method=method.value,
        corpus_prefix=corpus_prefix,
    )
