import math
import itertools
from copy import deepcopy
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from typing import AsyncIterator, Iterable, Iterator, TypeVar
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import iter_progress, verbose_print, schedule_tasks
from .utils import filter_edges_by_relationship


METHOD_NAME = 'Weighed Relevance Method'
DEFAULT_SIMILARITY_THRESHOLD = 0.2
CORPUS_REQUIRED = True
CORPUS_GRAPH_REQUIRED = True

_populated_target_graph: nx.DiGraph | None = None
_max_annotation_sum: float | None = None
_tune_factor = 0.5
_convergence_threshold = 1e-3
_PAIR_BATCH_SIZE = 256
_T = TypeVar('_T')


def _sum_annotation_for_graph(target_graph: nx.DiGraph,
                              corpus_graph: nx.DiGraph,
                              annotation_graph: nx.Graph,
                              target_prefix: ConceptPrefix,
                              corpus_prefix: ConceptPrefix,
                              is_first_iteration: bool = False,
                              ) -> None:
    """
    Sum annotation contribution weights for each node in the target graph, and add them as node attributes.
    :param target_graph: The directed graph of the target vocabulary.
    :param corpus_graph: The directed graph of the corpus vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :param target_prefix: The prefix of the target vocabulary.
    :param corpus_prefix: The prefix of the corpus vocabulary.
    :param is_first_iteration: Whether this is the first iteration of calculation.
    """
    order = list(nx.topological_sort(target_graph))

    for node in iter_progress(
        order,
        description='Calculating annotation sums for target graph',
        total=len(order),
    ):
        annotation_name = f'{target_prefix.value}:{node}'

        direct_annotation_nodes = annotation_graph.neighbors(annotation_name) \
            if annotation_name in annotation_graph else []
        direct_annotation_sum = 0
        for annotation_node in direct_annotation_nodes:
            if annotation_node.startswith(f'{corpus_prefix.value}:'):
                corpus_node = annotation_node.split(':', 1)[1]
                if 'ic' in corpus_graph.nodes[corpus_node]:
                    direct_annotation_sum += math.exp(_tune_factor * corpus_graph.nodes[corpus_node]['ic'])
                else:
                    # Corpus node does not have a sum
                    if is_first_iteration:
                        # In the first iteration, we assign a default contribution weight
                        direct_annotation_sum += 0.5
                    else:
                        # A directly annotated corpus node must have an IC
                        raise ValueError(f'Corpus node {corpus_node} does not have IC value.')

        # The IS_A relationship is from child to parent, so the predecessors are the children
        child_annotation_sum = 0
        for child in target_graph.predecessors(node):
            if 'annotation_sum' in target_graph.nodes[child]:
                child_annotation_sum += target_graph.nodes[child]['annotation_sum']
            else:
                # A child should have been processed before the parent in topological order
                raise ValueError(f'Child node {child} has not been processed yet.')

        total_annotation_sum = direct_annotation_sum + child_annotation_sum
        target_graph.nodes[node]['annotation_sum'] = total_annotation_sum


def _calculate_ic(target_graph: nx.DiGraph) -> float:
    """
    Calculate the information content for each node in the target graph.
    :param target_graph: The directed graph of the target vocabulary.
    :return: The maximum change in IC values across all nodes.
    """
    max_annotation_sum = max(
        target_graph.nodes[node].get('annotation_sum', 0) for node in target_graph.nodes
    )

    max_delta = 0

    for node in iter_progress(
        target_graph.nodes,
        description='Calculating Information Content',
        total=target_graph.number_of_nodes(),
        transient=True,
    ):
        if 'annotation_sum' not in target_graph.nodes[node] or target_graph.nodes[node]['annotation_sum'] == 0:
            # The node and its descendants have no annotations, IC is infinite
            continue

        new_ic = -1 * math.log(
            target_graph.nodes[node]['annotation_sum'] / max_annotation_sum
        )
        if 'ic' in target_graph.nodes[node]:
            delta = abs(new_ic - target_graph.nodes[node]['ic'])
            if delta > max_delta:
                max_delta = delta

        target_graph.nodes[node]['ic'] = new_ic

    return max_delta


@lru_cache(maxsize=None)
def _ancestors(node: str) -> frozenset[str]:
    """Return the ontology ancestors of a node, cached within each worker."""
    return frozenset(nx.descendants(_populated_target_graph, node)) | {node}


def _calculate_relevance_cached(node_1: str, node_2: str) -> float | None:
    """Calculate weighed relevance without repeating graph traversals."""
    common_ancestors = _ancestors(node_1).intersection(_ancestors(node_2))
    mica = max(
        (node for node in common_ancestors if 'ic' in _populated_target_graph.nodes[node]),
        key=lambda node: _populated_target_graph.nodes[node]['ic'],
        default=None,
    )
    if mica is None:
        return None

    return 2 * _populated_target_graph.nodes[mica]['ic'] / (
        _populated_target_graph.nodes[node_1]['ic'] + _populated_target_graph.nodes[node_2]['ic']
    ) * (1 - _populated_target_graph.nodes[mica]['annotation_sum'] / _max_annotation_sum)


def _relevance_worker(
    node_pairs: tuple[tuple[str, str], ...],
) -> list[tuple[str, str, float | None]]:
    """
    Worker function to calculate weighed Relevance for a batch of node pairs.
    """
    return [
        (node_1, node_2, _calculate_relevance_cached(node_1, node_2))
        for node_1, node_2 in node_pairs
    ]


def _batched(iterable: Iterable[_T], size: int) -> Iterator[tuple[_T, ...]]:
    """Yield fixed-size tuples without materialising every node pair."""
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, size)):
        yield batch


def _worker_init(target_graph: nx.DiGraph,
                 max_annotation_sum: float,
                 ):
    """
    Initialise the worker with the target graph and maximum annotation count.
    :param target_graph: The directed graph of the target vocabulary.
    :param max_annotation_sum: The maximum annotation count in the target graph.
    """
    global _populated_target_graph, _max_annotation_sum

    _populated_target_graph = target_graph
    _max_annotation_sum = max_annotation_sum
    _ancestors.cache_clear()


async def calculate_similarity(target_graph: nx.MultiDiGraph,
                               target_prefix: ConceptPrefix,
                               corpus_graph: nx.MultiDiGraph = None,
                               corpus_prefix: ConceptPrefix = None,
                               annotation_graph: nx.DiGraph = None,
                               ) -> AsyncIterator[tuple[str, str, float]]:
    """
    Calculate semantic similarity scores between terms in the target graph with weighed Relevance method.

    This method extend the standard Relevance method by assigned a contribution weight to each annotation,
    based on how many nodes in the target graph are annotated with it. Annotations that are more specific
    (i.e., annotate fewer target nodes) contribute more to the similarity score than more general annotations.
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

    corpus_graph = deepcopy(corpus_graph)
    filter_edges_by_relationship(
        graph=corpus_graph,
        relationship_types={ConceptRelationshipType.IS_A, ConceptRelationshipType.PART_OF},
    )
    verbose_print(f'Relationship filtered down to {len(corpus_graph.edges)} edges in corpus graph.')

    target_graph = nx.DiGraph(target_graph)
    corpus_graph = nx.DiGraph(corpus_graph)
    annotation_graph = annotation_graph.to_undirected()

    # Iteratively calculate annotation sums and information content until convergence
    iteration = 0
    while True:
        verbose_print(f'Iteration {iteration + 1}: Calculating annotation sums on target graph...')
        _sum_annotation_for_graph(
            target_graph=target_graph,
            corpus_graph=corpus_graph,
            annotation_graph=annotation_graph,
            target_prefix=target_prefix,
            corpus_prefix=corpus_prefix,
            is_first_iteration=(iteration == 0),
        )

        verbose_print(f'Iteration {iteration + 1}: Calculating information content on target graph...')
        max_delta_target = _calculate_ic(target_graph=target_graph)
        verbose_print(f'Iteration {iteration + 1}: Maximum IC change on target = {max_delta_target:.6f}')

        verbose_print(f'Iteration {iteration + 1}: Calculating annotation sums on corpus graph...')
        _sum_annotation_for_graph(
            target_graph=corpus_graph,
            corpus_graph=target_graph,
            annotation_graph=annotation_graph,
            target_prefix=corpus_prefix,
            corpus_prefix=target_prefix,
        )

        verbose_print(f'Iteration {iteration + 1}: Calculating IC sum on corpus graph...')
        max_delta_corpus = _calculate_ic(target_graph=corpus_graph)
        verbose_print(f'Iteration {iteration + 1}: Maximum IC change on corpus = {max_delta_corpus:.6f}')

        if iteration > 0 and max(max_delta_target, max_delta_corpus) < _convergence_threshold:
            verbose_print(f'Convergence reached after {iteration + 1} iterations.')
            break

        iteration += 1

    # IC is ready, use the same formula as standard Relevance method
    nodes_with_ic = [node for node in target_graph.nodes if 'ic' in target_graph.nodes[node]]
    node_pairs = itertools.combinations(nodes_with_ic, 2)
    pair_count = len(nodes_with_ic) * (len(nodes_with_ic) - 1) // 2
    pair_batches = _batched(node_pairs, _PAIR_BATCH_SIZE)
    batch_count = math.ceil(pair_count / _PAIR_BATCH_SIZE)
    max_annotation_sum = max(
        target_graph.nodes[node].get('annotation_sum', 0) for node in target_graph
    )

    verbose_print(f'Calculating similarity for {len(nodes_with_ic)} nodes, '
                  f'total {pair_count} pairs.')

    with ProcessPoolExecutor(
        max_workers=CONFIG.process_limit,
        initializer=_worker_init,
        initargs=(target_graph, max_annotation_sum),
    ) as executor:
        async for results in schedule_tasks(
            executor=executor,
            func=_relevance_worker,
            iterable=pair_batches,
            description='Calculate weighed relevance similarity scores between terms in the target graph.',
            total=batch_count,
        ):
            for concept_from, concept_to, similarity in results:

                if similarity is not None:
                    yield concept_from, concept_to, similarity
