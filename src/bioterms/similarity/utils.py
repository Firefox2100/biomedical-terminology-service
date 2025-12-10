import networkx as nx

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import iter_progress, verbose_print


def filter_edges_by_relationship(graph: nx.MultiDiGraph | nx.DiGraph,
                                 relationship_types: set[ConceptRelationshipType],
                                 ):
    """
    Filter edges in the graph by relationship types.
    """
    edges_to_remove = []

    if isinstance(graph, nx.DiGraph):
        for u, v, data in iter_progress(
            graph.edges(data=True),
            description='Filtering edges by relationship types',
            total=graph.number_of_edges(),
        ):
            if data.get('label') not in relationship_types:
                edges_to_remove.append((u, v))
    elif isinstance(graph, nx.MultiDiGraph):
        for u, v, key, data in iter_progress(
            graph.edges(keys=True, data=True),
            description='Filtering edges by relationship types',
            total=graph.number_of_edges(),
        ):
            if data.get('label') not in relationship_types:
                edges_to_remove.append((u, v, key))
    else:
        raise TypeError('Graph must be a DiGraph or MultiDiGraph.')

    verbose_print(f'Removing {len(edges_to_remove)} edges not matching specified relationship types.')

    graph.remove_edges_from(edges_to_remove)


def count_annotation_for_graph(target_graph: nx.DiGraph,
                               annotation_graph: nx.Graph,
                               target_prefix: ConceptPrefix,
                               ) -> None:
    """
    Calculate annotation counts for each node in the target graph, and add them as node attributes.
    :param target_graph: The directed graph of the target vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :param target_prefix: The prefix of the target vocabulary.
    """
    order = list(nx.topological_sort(target_graph))

    for node in iter_progress(
        order,
        description='Calculating annotation counts for target graph',
        total=len(order),
    ):
        annotation_name = f'{target_prefix}:{node}'

        direct_annotation_count = annotation_graph.degree[annotation_name] \
            if annotation_name in annotation_graph else 0

        # The IS_A relationship is from child to parent, so the predecessors are the children
        child_annotation_sum = 0
        for child in target_graph.predecessors(node):
            if 'annotation_count' in target_graph.nodes[child]:
                child_annotation_sum += target_graph.nodes[child]['annotation_count']
            else:
                # A child should have been processed before the parent in topological order
                raise ValueError(f'Child node {child} has not been processed yet.')

        total_annotation_count = direct_annotation_count + child_annotation_sum
        target_graph.nodes[node]['annotation_count'] = total_annotation_count


def find_mica(node_1: str,
              node_2: str,
              graph: nx.DiGraph,
              property_name: str = 'ic',
              ) -> str | None:
    """
    Find the Most Informative Common Ancestor (MICA) of two nodes in the graph.
    :param node_1: The first node.
    :param node_2: The second node.
    :param graph: The directed graph.
    :param property_name: The node property to use for information content. Defaults to 'ic'.
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
        if property_name in graph.nodes[ancestor] and graph.nodes[ancestor][property_name] > max_ic:
            max_ic = graph.nodes[ancestor][property_name]
            mica = ancestor

    return mica


def calculate_relevance(node_1: str,
                        node_2: str,
                        graph: nx.DiGraph,
                        max_annotation: int | float,
                        annotation_attribute: str = 'annotation_count',
                        ic_attribute: str = 'ic',
                        ) -> float | None:
    """
    Calculate the Relevance similarity between two nodes.
    :param node_1: The first node.
    :param node_2: The second node.
    :param graph: The directed graph.
    :param max_annotation: The maximum (equivalent) annotation count in the graph.
    :param annotation_attribute: The node attribute for annotation count. Defaults to 'annotation_count'.
    :param ic_attribute: The node attribute for information content. Defaults to 'ic'.
    :return: The Relevance similarity score, or None if it cannot be calculated.
    """
    mica = find_mica(
        node_1=node_1,
        node_2=node_2,
        graph=graph,
    )

    if mica is None:
        return None

    return 2 * graph.nodes[mica][ic_attribute] / \
        (graph.nodes[node_1][ic_attribute] + graph.nodes[node_2][ic_attribute]) * \
        (1 - graph.nodes[mica][annotation_attribute] / max_annotation)
