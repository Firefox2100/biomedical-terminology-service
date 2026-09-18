"""Compact write-side graph representation.

Vocabulary loaders only need to collect normalized edges before handing them to a database or
offline writer.  A NetworkX graph is substantially more expensive because it maintains nested
node/adjacency dictionaries and graph-algorithm metadata that ingestion never reads.
"""

from collections.abc import Iterator
from dataclasses import dataclass, field

from bioterms.etc.enums import ConceptRelationshipType


EdgeTuple = tuple[str, str, str | None, str | None]


class _EdgeView:
    """Small read-only compatibility view used by loader unit tests and diagnostics."""

    def __init__(self, edges: list[EdgeTuple]):
        self._edges = edges

    def __getitem__(self, key):
        source, target, *edge_key = key
        for candidate_source, candidate_target, label, candidate_key in self._edges:
            if candidate_source != str(source) or candidate_target != str(target):
                continue
            if edge_key and candidate_key != edge_key[0]:
                continue
            return {
                'label': ConceptRelationshipType(label) if label is not None else None,
            }
        raise KeyError(key)


@dataclass(slots=True)
class EdgeBuffer:
    """Append-only normalized edge buffer compatible with graph database writers."""

    _edges: list[EdgeTuple] = field(default_factory=list)

    def add_node(self, _node_id: str) -> None:
        """Nodes are persisted from concept objects, so write-side node registration is free."""

    def add_nodes_from(self, _node_ids) -> None:
        """Nodes are persisted from concept objects, so write-side node registration is free."""

    def add_edge(self,
                 source: str,
                 target: str,
                 *,
                 key: str | None = None,
                 label: ConceptRelationshipType | str | None = None,
                 **_properties,
                 ) -> None:
        relationship_type = label.value if isinstance(label, ConceptRelationshipType) else label
        self._edges.append((str(source), str(target), relationship_type, key))

    def __iter__(self) -> Iterator[EdgeTuple]:
        return iter(self._edges)

    def __len__(self) -> int:
        return len(self._edges)

    @property
    def edges(self) -> _EdgeView:
        return _EdgeView(self._edges)

    def has_edge(self, source: str, target: str, key: str | None = None) -> bool:
        return any(
            s == str(source) and t == str(target) and (key is None or edge_key == key)
            for s, t, _, edge_key in self._edges
        )

    def number_of_edges(self, source: str | None = None, target: str | None = None) -> int:
        if source is None and target is None:
            return len(self._edges)
        return sum(
            (source is None or s == str(source)) and (target is None or t == str(target))
            for s, t, _, _ in self._edges
        )

    def copy(self) -> 'EdgeBuffer':
        return EdgeBuffer(self._edges.copy())
