import math
import itertools
from copy import deepcopy
from concurrent.futures import ProcessPoolExecutor
from typing import AsyncIterator
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import iter_progress, verbose_print, schedule_tasks
from .utils import count_annotation_for_graph, filter_edges_by_relationship, calculate_relevance


METHOD_NAME = 'Relevance Method'
DEFAULT_SIMILARITY_THRESHOLD = 0.2
CORPUS_REQUIRED = True
CORPUS_GRAPH_REQUIRED = False

_populated_target_graph: nx.DiGraph | None = None
_max_annotation_count: int | None = None


def _calculate_ic(target_graph: nx.DiGraph,
                  max_annotation_count: int,
                  ) -> None:
    """
    Calculate the information content for each node in the target graph.
    :param target_graph: The directed graph of the target vocabulary.
    :param max_annotation_count: The maximum annotation count in the target graph.
    """
    for node in iter_progress(
        target_graph.nodes,
        description='Calculating Information Content',
        total=target_graph.number_of_nodes(),
    ):
        if 'annotation_count' not in target_graph.nodes[node] or target_graph.nodes[node]['annotation_count'] == 0:
            # The node and its descendants have no annotations, IC is infinite
            continue

        target_graph.nodes[node]['ic'] = -1 * math.log(
            target_graph.nodes[node]['annotation_count'] / max_annotation_count
        )


def _relevance_worker(node_pair: tuple[str, str]) -> tuple[str, str, float | None]:
    """
    Worker function to calculate the Relevance similarity for a pair of nodes.
    :param node_pair: A tuple of two node IDs.
    :return: A tuple containing the two node IDs and their Relevance similarity score.
    """
    node_1, node_2 = node_pair

    relevance = calculate_relevance(
        node_1=node_1,
        node_2=node_2,
        graph=_populated_target_graph,
        max_annotation=_max_annotation_count,
    )

    return node_1, node_2, relevance


def _worker_init(target_graph: nx.DiGraph,
                 max_annotation_count: int,
                 ):
    """
    Initialise the worker with the target graph and maximum annotation count.
    :param target_graph: The directed graph of the target vocabulary.
    :param max_annotation_count: The maximum annotation count in the target graph.
    """
    global _populated_target_graph, _max_annotation_count

    _populated_target_graph = target_graph
    _max_annotation_count = max_annotation_count


async def calculate_similarity(target_graph: nx.MultiDiGraph,
                               target_prefix: ConceptPrefix,
                               corpus_graph: nx.MultiDiGraph = None,
                               corpus_prefix: ConceptPrefix = None,
                               annotation_graph: nx.DiGraph = None,
                               ) -> AsyncIterator[tuple[str, str, float]]:
    """
    Calculate semantic similarity scores between terms in the target graph with Relevance method.
    :param target_graph: The directed graph of the target vocabulary.
    :param target_prefix: The prefix of the target vocabulary.
    :param corpus_graph: The directed graph of the corpus vocabulary.
    :param corpus_prefix: The prefix of the corpus vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :return: A generator yielding tuples of (concept_from, concept_to, similarity_score).
    """
    # Count the annotations for each node in the target graph
    target_graph = deepcopy(target_graph)
    filter_edges_by_relationship(
        graph=target_graph,
        relationship_types={ConceptRelationshipType.IS_A, ConceptRelationshipType.PART_OF},
    )
    verbose_print(f'Relationship filtered down to {len(target_graph.edges)} edges in target graph.')

    target_graph = nx.DiGraph(target_graph)
    annotation_graph = annotation_graph.to_undirected()

    count_annotation_for_graph(
        target_graph=target_graph,
        annotation_graph=annotation_graph,
        target_prefix=target_prefix,
    )

    max_annotation_count = max(
        target_graph.nodes[node]['annotation_count'] for node in target_graph.nodes
    )

    verbose_print(f'Max annotation count in target graph: {max_annotation_count}')

    # Calculate the information content for each node in the target graph
    _calculate_ic(
        target_graph=target_graph,
        max_annotation_count=max_annotation_count,
    )

    nodes_with_ic = [node for node in target_graph.nodes if 'ic' in target_graph.nodes[node]]
    node_pairs = itertools.combinations(nodes_with_ic, 2)

    verbose_print(f'Calculating similarity for {len(nodes_with_ic)} nodes, '
                  f'total {len(nodes_with_ic)*(len(nodes_with_ic)-1)//2} pairs.')

    with ProcessPoolExecutor(
        max_workers=CONFIG.process_limit,
        initializer=_worker_init,
        initargs=(target_graph, max_annotation_count),
    ) as executor:
        async for result in schedule_tasks(
            executor=executor,
            func=_relevance_worker,
            iterable=node_pairs,
            description='Calculate relevance similarity scores between terms in the target graph.',
            total=len(nodes_with_ic)*(len(nodes_with_ic)-1)//2,
        ):
            concept_from, concept_to, similarity = result

            if similarity is not None:
                yield concept_from, concept_to, similarity
