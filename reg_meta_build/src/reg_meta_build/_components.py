"""Deterministic union-find shared by the SCB triage component builders
(`sources/scb.py`: contested-column clustering, rule-2 column connectivity) and
the concept-group edge pass (`concept_groups.py`, #303).

One implementation so the determinism rule cannot diverge between the triage
components and the browse groups that mirror them: ``union`` parents the
larger root under the smaller (lex-min root), so component roots — and
everything keyed on them — are stable regardless of edge insertion order.
"""

from __future__ import annotations


# Nodes must be hashable AND orderable (str / int / homogeneous tuples) — the
# lex-min-root rule compares roots. Not expressible as a type-param bound, so
# the contract lives here in prose.
class DisjointSet[T]:
    """Union-find with path compression and the deterministic lex-min-root
    rule. ``add`` registers a node (idempotent); ``find`` requires the node to
    be registered."""

    __slots__ = ("_parent",)

    def __init__(self) -> None:
        self._parent: dict[T, T] = {}

    def add(self, node: T) -> None:
        self._parent.setdefault(node, node)

    def find(self, node: T) -> T:
        parent = self._parent
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:  # path compression
            parent[node], node = root, parent[node]
        return root

    def union(self, a: T, b: T) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # Orderability is the documented node contract (module docstring);
            # not expressible as a type-param bound.
            lo, hi = (ra, rb) if ra <= rb else (rb, ra)  # ty: ignore[unsupported-operator]
            self._parent[hi] = lo

    def components(self) -> dict[T, list[T]]:
        """root → members (insertion-ordered; callers sort as needed)."""
        comps: dict[T, list[T]] = {}
        for node in self._parent:
            comps.setdefault(self.find(node), []).append(node)
        return comps
