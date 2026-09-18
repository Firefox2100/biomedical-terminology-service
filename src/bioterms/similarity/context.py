"""Compact, stable input contract shared by similarity implementations."""

from dataclasses import dataclass
from collections import deque
from collections.abc import Iterable

import numpy as np
from pyroaring import BitMap

from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType


_ONTOLOGY_RELATIONSHIPS = {
    ConceptRelationshipType.IS_A.value,
    ConceptRelationshipType.PART_OF.value,
}


@dataclass(slots=True)
class OntologyIndex:
    """Integer-indexed DAG with CSR adjacency in both directions."""

    node_ids: tuple[str, ...]
    node_to_index: dict[str, int]
    successor_ptr: np.ndarray
    successor_ids: np.ndarray
    predecessor_ptr: np.ndarray
    predecessor_ids: np.ndarray
    topological_order: np.ndarray

    @classmethod
    def build(cls,
              node_ids: Iterable[str],
              edges: Iterable[tuple[str, str, str | None, str | None]],
              ) -> 'OntologyIndex':
        nodes = list(dict.fromkeys(str(node) for node in node_ids))
        index = {node: i for i, node in enumerate(nodes)}
        successors = [set() for _ in nodes]
        predecessors = [set() for _ in nodes]
        for source, target, relationship, _key in edges:
            relationship = relationship.value if isinstance(relationship, ConceptRelationshipType) else relationship
            if relationship not in _ONTOLOGY_RELATIONSHIPS:
                continue
            source, target = str(source), str(target)
            if source not in index:
                index[source] = len(nodes)
                nodes.append(source)
                successors.append(set())
                predecessors.append(set())
            if target not in index:
                index[target] = len(nodes)
                nodes.append(target)
                successors.append(set())
                predecessors.append(set())
            a, b = index[source], index[target]
            successors[a].add(b)
            predecessors[b].add(a)

        indegree = np.fromiter((len(row) for row in predecessors), dtype=np.int64)
        queue = deque(i for i, degree in enumerate(indegree) if degree == 0)
        topo = []
        while queue:
            node = queue.popleft()
            topo.append(node)
            for parent in successors[node]:
                indegree[parent] -= 1
                if indegree[parent] == 0:
                    queue.append(parent)
        if len(topo) != len(nodes):
            raise ValueError('Filtered ontology must be a DAG.')

        def to_csr(rows):
            ptr = np.empty(len(rows) + 1, np.int64)
            ptr[0] = 0
            for i, row in enumerate(rows):
                ptr[i + 1] = ptr[i] + len(row)
            ids = np.empty(int(ptr[-1]), np.int32)
            for i, row in enumerate(rows):
                start = int(ptr[i])
                ids[start:start + len(row)] = sorted(row)
            return ptr, ids

        successor_ptr, successor_ids = to_csr(successors)
        predecessor_ptr, predecessor_ids = to_csr(predecessors)
        return cls(
            tuple(nodes), index, successor_ptr, successor_ids,
            predecessor_ptr, predecessor_ids, np.asarray(topo, np.int32),
        )

    def successors(self, node: int):
        return self.successor_ids[self.successor_ptr[node]:self.successor_ptr[node + 1]]

    def predecessors(self, node: int):
        return self.predecessor_ids[self.predecessor_ptr[node]:self.predecessor_ptr[node + 1]]


@dataclass(slots=True)
class AnnotationIndex:
    """Direct cross-vocabulary annotations indexed in both directions."""

    target_to_corpus: tuple[BitMap, ...]
    corpus_to_target: tuple[BitMap, ...]

    @classmethod
    def build(cls,
              target: OntologyIndex,
              corpus: OntologyIndex,
              pairs: Iterable[tuple[str, str]],
              ) -> 'AnnotationIndex':
        forward = [BitMap() for _ in target.node_ids]
        reverse = [BitMap() for _ in corpus.node_ids]
        for target_id, corpus_id in pairs:
            target_index = target.node_to_index.get(str(target_id))
            corpus_index = corpus.node_to_index.get(str(corpus_id))
            if target_index is None or corpus_index is None:
                continue
            forward[target_index].add(corpus_index)
            reverse[corpus_index].add(target_index)
        return cls(tuple(forward), tuple(reverse))


@dataclass(slots=True)
class SimilarityContext:
    """Versioned internal contract consumed by every similarity method."""

    target_prefix: ConceptPrefix
    target: OntologyIndex
    corpus_prefix: ConceptPrefix | None = None
    corpus: OntologyIndex | None = None
    annotations: AnnotationIndex | None = None
    threshold: float | None = None


def graph_edges(graph):
    """Adapt a legacy graph object at the I/O boundary during the 2.0 migration."""
    if getattr(graph, 'is_multigraph', lambda: False)():
        for source, target, key, data in graph.edges(keys=True, data=True):
            label = data.get('label')
            yield source, target, label.value if hasattr(label, 'value') else label, key
    else:
        for source, target, data in graph.edges(data=True):
            label = data.get('label')
            yield source, target, label.value if hasattr(label, 'value') else label, None


def build_similarity_context(target_prefix: ConceptPrefix,
                             target_graph,
                             corpus_prefix: ConceptPrefix | None = None,
                             corpus_graph=None,
                             annotation_graph=None,
                             ) -> SimilarityContext:
    """Build the compact contract from current graph-reader results."""
    target = OntologyIndex.build(target_graph.nodes, graph_edges(target_graph))
    if corpus_prefix is None:
        return SimilarityContext(target_prefix=target_prefix, target=target)

    pairs = []
    corpus_ids = set()
    target_tag = f'{target_prefix.value}:'
    corpus_tag = f'{corpus_prefix.value}:'
    if annotation_graph is not None:
        for left, right in annotation_graph.edges:
            left, right = str(left), str(right)
            if left.startswith(target_tag) and right.startswith(corpus_tag):
                pair = left[len(target_tag):], right[len(corpus_tag):]
            elif right.startswith(target_tag) and left.startswith(corpus_tag):
                pair = right[len(target_tag):], left[len(corpus_tag):]
            else:
                continue
            pairs.append(pair)
            corpus_ids.add(pair[1])
    corpus = (
        OntologyIndex.build(corpus_graph.nodes, graph_edges(corpus_graph))
        if corpus_graph is not None
        else OntologyIndex.build(corpus_ids, ())
    )
    annotations = AnnotationIndex.build(target, corpus, pairs)
    return SimilarityContext(target_prefix, target, corpus_prefix, corpus, annotations)
