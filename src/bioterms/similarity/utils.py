import networkx as nx

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import iter_progress, verbose_print


def filter_edges_by_relationship(graph: nx.DiGraph,
                                 relationship_types: set[ConceptRelationshipType],
                                 ):
    """
    Filter edges in the graph by relationship types.
    """
    edges_to_remove = []

    for u, v, data in iter_progress(
        graph.edges(data=True),
        description='Filtering edges by relationship types',
        total=graph.number_of_edges(),
    ):
        if data.get('label') not in relationship_types:
            edges_to_remove.append((u, v))

    verbose_print(f'Removing {len(edges_to_remove)} edges not matching specified relationship types.')

    graph.remove_edges_from(edges_to_remove)


def count_annotation_for_graph(target_graph: nx.DiGraph,
                               annotation_graph: nx.DiGraph,
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
