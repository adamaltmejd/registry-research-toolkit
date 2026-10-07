"""Catalog.resolve_terminal_successor across binding, register and
classification grains."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import build_slugged_db
from reg_meta.catalog import Catalog

if TYPE_CHECKING:
    import sqlite3


class TestResolveTerminalSuccessor:
    """#355 PART 2 / #412 / #571: walk a succession chain from a (possibly
    dead/renamed) FQID to its terminal successor — the chain end with no outbound
    edge. The walk dispatches on FQID kind: bindings walk `variable_replaced_by`
    (raw string triples), registers walk `register_replaced_by` (raw string
    pairs), classifications walk `classification_replaced_by` (raw edition slugs).
    A DEAD predecessor needs no live row (the whole point — its row is gone after
    the rename)."""

    @staticmethod
    def _add_edge(
        conn: sqlite3.Connection,
        predecessor: tuple[str, str, str],
        successor: tuple[str, str, str],
    ) -> None:
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, note) "
            "VALUES (?,?,?,?,?,?,'auto:test')",
            (*predecessor, *successor),
        )
        conn.commit()

    @staticmethod
    def _add_register_edge(
        conn: sqlite3.Connection,
        predecessor: tuple[str, str],
        successor: tuple[str, str],
    ) -> None:
        conn.execute(
            "INSERT INTO register_replaced_by ("
            "predecessor_provider, predecessor_register, "
            "successor_provider, successor_register, note) "
            "VALUES (?,?,?,?,'auto:test')",
            (*predecessor, *successor),
        )
        conn.commit()

    def test_cycle_guard_terminates(self) -> None:
        # Malformed double-rename loop A→B→A. The walk must terminate (not hang)
        # and land deterministically on B (start=A hops to B, B→A is already
        # seen → stop).
        conn = build_slugged_db()
        self._add_edge(conn, ("scb", "lisa", "loop-a"), ("scb", "lisa", "loop-b"))
        self._add_edge(conn, ("scb", "lisa", "loop-b"), ("scb", "lisa", "loop-a"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/lisa/loop-a")
        assert terminal is not None
        assert str(terminal) == "scb/lisa/loop-b"

    def test_split_pick_is_lexicographically_first(self) -> None:
        # Deterministic split pick: when a predecessor has TWO distinct
        # successors, the walk takes the lexicographically-FIRST per
        # `_first_successor_triple`'s `ORDER BY successor_provider,
        # successor_register, successor_variable LIMIT 1`. Both successors are
        # dead leaves (no variable rows, no further edges) so each is itself
        # terminal — this isolates the split pick, not the walk depth.
        conn = build_slugged_db()
        self._add_edge(conn, ("scb", "lisa", "split-src"), ("scb", "lisa", "zzz-high"))
        self._add_edge(conn, ("scb", "lisa", "split-src"), ("scb", "lisa", "aaa-low"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/lisa/split-src")
        assert terminal is not None
        # "aaa-low" < "zzz-high" → the lower-sorted successor wins.
        assert str(terminal) == "scb/lisa/aaa-low"

    # #412: register-grain renames now redirect too. These mirror the binding
    # tests above on `register_replaced_by`; dead predecessor registers need no
    # `register` row (same dead-slug premise), and `scb/lisa` is the live,
    # edge-free terminal from build_slugged_db.

    def test_register_cycle_guard_terminates(self) -> None:
        # Malformed double-rename loop A→B→A. The walk must terminate (not hang)
        # and land deterministically on B (start=A hops to B, B→A is already
        # seen → stop).
        conn = build_slugged_db()
        self._add_register_edge(conn, ("scb", "loop-reg-a"), ("scb", "loop-reg-b"))
        self._add_register_edge(conn, ("scb", "loop-reg-b"), ("scb", "loop-reg-a"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/loop-reg-a")
        assert terminal is not None
        assert str(terminal) == "scb/loop-reg-b"

    def test_register_split_pick_is_lexicographically_first(self) -> None:
        # Deterministic split pick: when a predecessor register has TWO distinct
        # successors, the walk takes the lexicographically-FIRST per
        # `_first_register_successor_pair`'s `ORDER BY successor_provider,
        # successor_register LIMIT 1`. Both successors are dead leaves (no register
        # rows, no further edges) so each is itself terminal — this isolates the
        # split pick, not the walk depth.
        conn = build_slugged_db()
        self._add_register_edge(conn, ("scb", "split-reg"), ("scb", "zzz-reg"))
        self._add_register_edge(conn, ("scb", "split-reg"), ("scb", "aaa-reg"))
        terminal = Catalog(conn).resolve_terminal_successor("scb/split-reg")
        assert terminal is not None
        # "aaa-reg" < "zzz-reg" → the lower-sorted successor wins.
        assert str(terminal) == "scb/aaa-reg"

    # #571: classification-edition renames now redirect too. These mirror the
    # binding/register tests on `classification_replaced_by` (raw edition slugs);
    # dead predecessor editions need no `classification` row.

    @staticmethod
    def _add_class_edge(
        conn: sqlite3.Connection,
        predecessor: str,
        successor: str,
        effective_year: int | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES (?, ?, ?, 'derived:test')",
            (predecessor, successor, effective_year),
        )
        conn.commit()

    def test_classification_multi_hop_chain_returns_terminal(self) -> None:
        # ssyk1996 → ssyk2001 → ssyk2012. Predecessor editions are dead (no
        # `classification` rows, only edges); ssyk2012 is the terminal.
        conn = build_slugged_db()
        self._add_class_edge(conn, "ssyk1996", "ssyk2001")
        self._add_class_edge(conn, "ssyk2001", "ssyk2012")
        terminal = Catalog(conn).resolve_terminal_successor("class/ssyk1996")
        assert terminal is not None
        assert str(terminal) == "class/ssyk2012"

    def test_classification_future_successor_not_terminal_until_as_of_year(
        self,
    ) -> None:
        conn = build_slugged_db()
        self._add_class_edge(conn, "icd-10-se", "icd-11-se", effective_year=2027)

        assert (
            Catalog(conn, classification_as_of_year=2026).resolve_terminal_successor(
                "class/icd-10-se"
            )
            is None
        )
        terminal = Catalog(
            conn, classification_as_of_year=2027
        ).resolve_terminal_successor("class/icd-10-se")
        assert terminal is not None
        assert str(terminal) == "class/icd-11-se"

    def test_classification_cycle_guard_terminates(self) -> None:
        # Malformed double-rename loop A→B→A: terminate and land on B.
        conn = build_slugged_db()
        self._add_class_edge(conn, "loop-cls-a", "loop-cls-b")
        self._add_class_edge(conn, "loop-cls-b", "loop-cls-a")
        terminal = Catalog(conn).resolve_terminal_successor("class/loop-cls-a")
        assert terminal is not None
        assert str(terminal) == "class/loop-cls-b"

    def test_classification_split_pick_is_lexicographically_first(self) -> None:
        # A predecessor edition with TWO successors takes the lexicographically
        # first (ORDER BY successor_slug LIMIT 1); both are dead terminal leaves.
        conn = build_slugged_db()
        self._add_class_edge(conn, "split-cls", "zzz-cls")
        self._add_class_edge(conn, "split-cls", "aaa-cls")
        terminal = Catalog(conn).resolve_terminal_successor("class/split-cls")
        assert terminal is not None
        assert str(terminal) == "class/aaa-cls"
