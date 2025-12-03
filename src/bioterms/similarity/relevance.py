import asyncio
import math
import itertools
from copy import deepcopy
from concurrent.futures import ProcessPoolExecutor
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from .utils import count_annotation_for_graph, filter_edges_by_relationship


METHOD_NAME = 'Relevance Method'
DEFAULT_SIMILARITY_THRESHOLD = 0.2

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
    for node in target_graph.nodes:
        if 'annotation_count' not in target_graph.nodes[node] or target_graph.nodes[node]['annotation_count'] == 0:
            # The node and its descendants have no annotations, IC is infinite
            continue

        target_graph.nodes[node]['ic'] = -1 * math.log(
            target_graph.nodes[node]['annotation_count'] / max_annotation_count
        )


def _find_mica(node_1: str,
               node_2: str,
               graph: nx.DiGraph,
               ) -> str | None:
    """
    Find the Most Informative Common Ancestor (MICA) of two nodes in the graph.
    :param node_1: The first node.
    :param node_2: The second node.
    :param graph: The directed graph.
    :return: The MICA node ID, or None if no common ancestor is found.
    """
    node_1_ancestors = set(nx.descendants(graph, node_1)) | {node_1}
    node_2_ancestors = set(nx.descendants(graph, node_2)) | {node_2}

    common_ancestors = node_1_ancestors.intersection(node_2_ancestors)

    if not common_ancestors:
        return None

    max_ic = -1
    mica = None

    for ancestor in common_ancestors:
        if 'ic' in graph.nodes[ancestor] and graph.nodes[ancestor]['ic'] > max_ic:
            max_ic = graph.nodes[ancestor]['ic']
            mica = ancestor

    return mica


def _calculate_relevance(node_1: str,
                         node_2: str,
                         graph: nx.DiGraph,
                         max_annotation_count: int,
                         ) -> float | None:
    """
    Calculate the Relevance similarity between two nodes.
    :param node_1: The first node.
    :param node_2: The second node.
    :param graph: The directed graph.
    :param max_annotation_count: The maximum annotation count in the graph.
    :return: The Relevance similarity score, or None if it cannot be calculated.
    """
    mica = _find_mica(
        node_1=node_1,
        node_2=node_2,
        graph=graph,
    )

    if mica is None:
        return None

    return 2 * graph.nodes[mica]['ic'] / (graph.nodes[node_1]['ic'] + graph.nodes[node_1]['ic']) * \
        (1 - graph.nodes[mica]['annotation_count'] / max_annotation_count)


def _relevance_worker(node_pair: tuple[str, str]) -> tuple[str, str, float | None]:
    """
    Worker function to calculate the Relevance similarity for a pair of nodes.
    :param node_pair: A tuple of two node IDs.
    :return: A tuple containing the two node IDs and their Relevance similarity score.
    """
    node_1, node_2 = node_pair

    relevance = _calculate_relevance(
        node_1=node_1,
        node_2=node_2,
        graph=_populated_target_graph,
        max_annotation_count=_max_annotation_count,
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


async def calculate_similarity(target_graph: nx.DiGraph,
                               target_prefix: ConceptPrefix,
                               corpus_graph: nx.DiGraph = None,
                               corpus_prefix: ConceptPrefix = None,
                               annotation_graph: nx.DiGraph = None,
                               ) -> pd.DataFrame:
    """
    Calculate semantic similarity scores between terms in the target graph with Relevance method.
    :param target_graph: The directed graph of the target vocabulary.
    :param target_prefix: The prefix of the target vocabulary.
    :param corpus_graph: The directed graph of the corpus vocabulary.
    :param corpus_prefix: The prefix of the corpus vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :return: A pandas DataFrame with similarity scores.
    """
    # Count the annotations for each node in the target graph
    target_graph = deepcopy(target_graph)
    filter_edges_by_relationship(
        graph=target_graph,
        relationship_types={ConceptRelationshipType.IS_A, ConceptRelationshipType.PART_OF},
    )

    count_annotation_for_graph(
        target_graph=target_graph,
        annotation_graph=annotation_graph,
        target_prefix=target_prefix,
    )

    max_annotation_count = max(
        target_graph.nodes[node]['annotation_count'] for node in target_graph.nodes
    )

    # Calculate the information content for each node in the target graph
    _calculate_ic(
        target_graph=target_graph,
        max_annotation_count=max_annotation_count,
    )

    nodes_with_ic = [node for node in target_graph.nodes if 'ic' in target_graph.nodes[node]]
    node_pairs = itertools.combinations(nodes_with_ic, 2)

    # asyncio compatible multiprocessing
    loop = asyncio.get_running_loop()

    with ProcessPoolExecutor(
        max_workers=CONFIG.process_limit,
        initializer=_worker_init,
        initargs=(target_graph, max_annotation_count),
    ) as executor:
        tasks = [
            loop.run_in_executor(
                executor,
                _relevance_worker,
                node_pair,
            ) for node_pair in node_pairs
        ]

        results = await asyncio.gather(*tasks)

    # Compile results into a DataFrame
    sim_data = [
        {
            'concept_from': n1,
            'concept_to': n2,
            'similarity': relevance,
        } for n1, n2, relevance in results if relevance is not None
    ]

    similarity_df = pd.DataFrame(sim_data)
    return similarity_df
