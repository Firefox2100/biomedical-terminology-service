import math
import itertools
from copy import deepcopy
from concurrent.futures import ProcessPoolExecutor
from typing import AsyncIterator
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import iter_progress, verbose_print, schedule_tasks
from .utils import filter_edges_by_relationship, calculate_relevance


METHOD_NAME = 'Weighed Relevance Method'
DEFAULT_SIMILARITY_THRESHOLD = 0.2
CORPUS_REQUIRED = True
CORPUS_GRAPH_REQUIRED = True

_populated_target_graph: nx.DiGraph | None = None
_max_annotation_sum: float | None = None
_tune_factor = 0.5
_convergence_threshold = 1e-3


def sum_annotation_for_graph(target_graph: nx.DiGraph,
                             corpus_graph: nx.DiGraph,
                             annotation_graph: nx.DiGraph,
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
        annotation_name = f'{target_prefix}:{node}'

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
                        direct_annotation_sum += 1
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

        total_annotation_sum= direct_annotation_sum + child_annotation_sum
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


def _relevance_worker(node_pair: tuple[str, str]) -> tuple[str, str, float | None]:
    """
    Worker function to calculate the weighed Relevance similarity for a pair of nodes.
    :param node_pair: A tuple of two node IDs.
    :return: A tuple containing the two node IDs and their Relevance similarity score.
    """
    node_1, node_2 = node_pair

    relevance = calculate_relevance(
        node_1=node_1,
        node_2=node_2,
        graph=_populated_target_graph,
        max_annotation=_max_annotation_sum,
        annotation_attribute='annotation_sum',
    )

    return node_1, node_2, relevance


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


async def calculate_similarity(target_graph: nx.DiGraph,
                               target_prefix: ConceptPrefix,
                               corpus_graph: nx.DiGraph = None,
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

    # Iteratively calculate annotation sums and information content until convergence
    iteration = 0
    while True:
        verbose_print(f'Iteration {iteration + 1}: Calculating annotation sums on target graph...')
        sum_annotation_for_graph(
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
        sum_annotation_for_graph(
            target_graph=corpus_graph,
            corpus_graph=corpus_graph,
            annotation_graph=annotation_graph,
            target_prefix=corpus_prefix,
            corpus_prefix=corpus_prefix,
        )

        verbose_print(f'Iteration {iteration + 1}: Calculating IC sum on corpus graph...')
        max_delta_corpus = _calculate_ic(target_graph=corpus_graph)
        verbose_print(f'Iteration {iteration + 1}: Maximum IC change on corpus = {max_delta_corpus:.6f}')

        if max(max_delta_target, max_delta_corpus) < _convergence_threshold:
            verbose_print(f'Convergence reached after {iteration + 1} iterations.')
            break

    # IC is ready, use the same formula as standard Relevance method
    nodes_with_ic = [node for node in target_graph.nodes if 'ic' in target_graph.nodes[node]]
    node_pairs = itertools.combinations(nodes_with_ic, 2)
    max_annotation_sum = max(
        target_graph.nodes[node].get('annotation_sum', 0) for node in target_graph
    )

    verbose_print(f'Calculating similarity for {len(nodes_with_ic)} nodes, '
                  f'total {len(nodes_with_ic) * (len(nodes_with_ic) - 1) // 2} pairs.')

    with ProcessPoolExecutor(
        max_workers=CONFIG.process_limit,
        initializer=_worker_init,
        initargs=(target_graph, max_annotation_sum),
    ) as executor:
        async for result in schedule_tasks(
            executor=executor,
            func=_relevance_worker,
            iterable=node_pairs,
            description='Calculate weighed relevance similarity scores between terms in the target graph.',
            total=len(nodes_with_ic) * (len(nodes_with_ic) - 1) // 2,
        ):
            concept_from, concept_to, similarity = result

            if similarity is not None:
                yield concept_from, concept_to, similarity
