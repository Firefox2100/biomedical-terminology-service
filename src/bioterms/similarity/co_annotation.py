import math
import itertools
from copy import deepcopy
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from typing import AsyncIterator, Iterable, Iterator, TypeVar
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import verbose_print, schedule_tasks
from .utils import count_annotation_for_graph, filter_edges_by_relationship


METHOD_NAME = 'Co-Annotation Vector Method'
DEFAULT_SIMILARITY_THRESHOLD = 0.2
CORPUS_REQUIRED = True
CORPUS_GRAPH_REQUIRED = False


_pruned_target_graph: nx.DiGraph | None = None
_annotation_graph: nx.Graph | None = None
_target_prefix: ConceptPrefix | None = None
_corpus_prefix: ConceptPrefix | None = None
_total_annotation_count: int | None = None
_PAIR_BATCH_SIZE = 256
_T = TypeVar('_T')


def _calculate_co_annotation(node_1: str,
                             node_2: str,
                             target_graph: nx.DiGraph,
                             target_prefix: ConceptPrefix,
                             corpus_prefix: ConceptPrefix,
                             annotation_graph: nx.Graph,
                             total_annotation_count: int,
                             ) -> float | None:
    """
    Calculate the co-annotation similarity between two nodes.

    This function uses a combination of normalised pairwise mutual information and Jaccard index
    to compute the similarity based on their annotation sets.
    :param node_1: The first node.
    :param node_2: The second node.
    :param target_graph: The directed graph of the target vocabulary.
    :param target_prefix: The prefix of the target vocabulary.
    :param corpus_prefix: The prefix of the corpus vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :param total_annotation_count: The total number of annotations in the annotation graph.
    :return: The co-annotation similarity score, or None if either node has zero annotations.
    """
    # Get all descendants including the node itself
    descendants_1 = set(nx.ancestors(target_graph, node_1)) | {node_1}
    descendants_2 = set(nx.ancestors(target_graph, node_2)) | {node_2}

    # Find their annotation sets
    annotation_set_1 = set()
    for desc in descendants_1:
        annotation_name = f'{target_prefix.value}:{desc}'
        if annotation_name in annotation_graph:
            annotation_set_1.update(
                neighbor for neighbor in annotation_graph.neighbors(annotation_name)
                if neighbor.startswith(f'{corpus_prefix.value}:')
            )
    annotation_set_2 = set()
    for desc in descendants_2:
        annotation_name = f'{target_prefix.value}:{desc}'
        if annotation_name in annotation_graph:
            annotation_set_2.update(
                neighbor for neighbor in annotation_graph.neighbors(annotation_name)
                if neighbor.startswith(f'{corpus_prefix.value}:')
            )

    if total_annotation_count == 0:
        return None
    if len(annotation_set_1) == 0 or len(annotation_set_2) == 0:
        return None

    annotation_intersection = annotation_set_1 & annotation_set_2
    annotation_union = annotation_set_1 | annotation_set_2

    if len(annotation_intersection) == 0:
        return 0.0

    intersection_len = len(annotation_intersection)

    # 0 division error when intersection_len == total_annotation_count, in this case it's likely
    # close to root node, which should have max similarity
    if math.isclose(total_annotation_count, intersection_len):
        npmi = 1.0
    else:
        numerator = (intersection_len * total_annotation_count) / (len(annotation_set_1) * len(annotation_set_2))
        try:
            num_log = math.log(numerator)
            denom_log = math.log(total_annotation_count / intersection_len)
            if denom_log == 0:
                npmi = 1.0
            else:
                npmi = (1 + num_log / denom_log) / 2
        except (ValueError, ZeroDivisionError):
            npmi = 0.0

    jaccard_index = len(annotation_intersection) / len(annotation_union)
    similarity = npmi * jaccard_index

    return similarity


@lru_cache(maxsize=None)
def _annotation_set(node: str) -> frozenset[str]:
    """Return all corpus annotations below a node, cached per worker."""
    descendants = nx.ancestors(_pruned_target_graph, node) | {node}
    corpus_prefix = f'{_corpus_prefix.value}:'
    annotations = set()
    for descendant in descendants:
        annotation_name = f'{_target_prefix.value}:{descendant}'
        if annotation_name in _annotation_graph:
            annotations.update(
                neighbor for neighbor in _annotation_graph.neighbors(annotation_name)
                if neighbor.startswith(corpus_prefix)
            )
    return frozenset(annotations)


def _calculate_co_annotation_cached(node_1: str, node_2: str) -> float | None:
    """Calculate co-annotation similarity using cached node annotation sets."""
    annotation_set_1 = _annotation_set(node_1)
    annotation_set_2 = _annotation_set(node_2)

    if _total_annotation_count == 0 or not annotation_set_1 or not annotation_set_2:
        return None

    annotation_intersection = annotation_set_1 & annotation_set_2
    if not annotation_intersection:
        return 0.0

    intersection_len = len(annotation_intersection)
    if math.isclose(_total_annotation_count, intersection_len):
        npmi = 1.0
    else:
        numerator = (intersection_len * _total_annotation_count) / (
            len(annotation_set_1) * len(annotation_set_2)
        )
        try:
            num_log = math.log(numerator)
            denom_log = math.log(_total_annotation_count / intersection_len)
            npmi = 1.0 if denom_log == 0 else (1 + num_log / denom_log) / 2
        except (ValueError, ZeroDivisionError):
            npmi = 0.0

    return npmi * intersection_len / len(annotation_set_1 | annotation_set_2)


def _co_annotation_worker(
    node_pairs: tuple[tuple[str, str], ...],
) -> list[tuple[str, str, float | None]]:
    """
    Worker function to calculate co-annotation similarity for a batch of pairs.
    """
    return [
        (node_1, node_2, _calculate_co_annotation_cached(node_1, node_2))
        for node_1, node_2 in node_pairs
    ]


def _batched(iterable: Iterable[_T], size: int) -> Iterator[tuple[_T, ...]]:
    """Yield fixed-size tuples without materialising every node pair."""
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, size)):
        yield batch


def _worker_init(target_graph: nx.DiGraph,
                 target_prefix: ConceptPrefix,
                 corpus_prefix: ConceptPrefix,
                 annotation_graph: nx.Graph,
                 total_annotation_count: int
                 ):
    """
    Initialise global variables for worker processes.
    :param target_graph: The directed graph of the target vocabulary.
    :param target_prefix: The prefix of the target vocabulary.
    :param corpus_prefix: The prefix of the corpus vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :param total_annotation_count: The total number of annotations in the annotation graph.
    """
    global _pruned_target_graph, _annotation_graph, _target_prefix, _corpus_prefix, _total_annotation_count

    _pruned_target_graph = target_graph
    _annotation_graph = annotation_graph
    _target_prefix = target_prefix
    _corpus_prefix = corpus_prefix
    _total_annotation_count = total_annotation_count
    _annotation_set.cache_clear()


async def calculate_similarity(target_graph: nx.MultiDiGraph,
                               target_prefix: ConceptPrefix,
                               corpus_graph: nx.MultiDiGraph = None,
                               corpus_prefix: ConceptPrefix = None,
                               annotation_graph: nx.DiGraph = None,
                               ) -> AsyncIterator[tuple[str, str, float]]:
    """
    Calculate semantic similarity scores between terms in the target graph with co-annotation vectors.

    The similarity between two terms is calculated based on the overlap of their annotation vectors
    in the annotation graph, using a combination of NPMI and Jaccard index.
    :param target_graph: The directed graph of the target vocabulary.
    :param target_prefix: The prefix of the target vocabulary.
    :param corpus_graph: The directed graph of the corpus vocabulary.
    :param corpus_prefix: The prefix of the corpus vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :return: A generator yielding tuples of (concept_from, concept_to, similarity_score).
    """
    target_graph = deepcopy(target_graph)
    filter_edges_by_relationship(
        graph=target_graph,
        relationship_types={ConceptRelationshipType.IS_A, ConceptRelationshipType.PART_OF},
    )
    verbose_print(f'Relationship filtered down to {len(target_graph.edges)} edges in target graph.')

    target_graph = nx.DiGraph(target_graph)
    annotation_graph = annotation_graph.to_undirected()

    # Count the annotations for each node in the target graph
    count_annotation_for_graph(
        target_graph=target_graph,
        annotation_graph=annotation_graph,
        target_prefix=target_prefix,
    )

    # Prune the graph to only include nodes with annotations
    pruned_target_graph = target_graph.copy()
    nodes_to_remove = [
        node for node in pruned_target_graph.nodes
        if pruned_target_graph.nodes[node].get('annotation_count', 0) == 0
    ]

    verbose_print(f'Pruning {len(nodes_to_remove)} nodes with zero annotations from target graph.')
    pruned_target_graph.remove_nodes_from(nodes_to_remove)

    total_annotation_count = sum(
        1 for node in annotation_graph.nodes
        if node.startswith(f'{corpus_prefix.value}:')
    )
    verbose_print(f'Total annotation count in annotation graph: {total_annotation_count}')

    nodes = list(pruned_target_graph.nodes)
    node_pairs = itertools.combinations(nodes, 2)
    pair_count = len(nodes) * (len(nodes) - 1) // 2
    pair_batches = _batched(node_pairs, _PAIR_BATCH_SIZE)
    batch_count = math.ceil(pair_count / _PAIR_BATCH_SIZE)
    verbose_print(f'Calculating similarity for {len(nodes)} nodes, '
                  f'total {pair_count} pairs.')

    with ProcessPoolExecutor(
        max_workers=CONFIG.process_limit,
        initializer=_worker_init,
        initargs=(pruned_target_graph, target_prefix, corpus_prefix, annotation_graph, total_annotation_count),
    ) as executor:
        async for results in schedule_tasks(
            executor=executor,
            func=_co_annotation_worker,
            iterable=pair_batches,
            description='Calculate co-annotation similarity scores between terms in the target graph.',
            total=batch_count,
        ):
            for concept_from, concept_to, similarity in results:

                if similarity is not None:
                    yield concept_from, concept_to, similarity
