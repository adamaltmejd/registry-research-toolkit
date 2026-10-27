#!/usr/bin/env python3
"""rebuild_compare — what users see in the old catalog and the rebuilt one, per register.

    uv run --no-project scripts/rebuild_compare.py <old db> <new db> <report.json> [provider...]

Temporary (REBUILD.md, milestone A; deleted at F). `<old db>` is a released
`reg_meta.db`, `<new db>` a `reg-build` catalog and `<report.json>` its build report.
For every register of the given providers (default: every provider in the new catalog)
it compares tables, the columns each documents and their periods, the column-to-variable
mapping, labels, value-set codes, flags and coverage, and puts each difference in one
class. Prints Markdown: a summary per class and register, then examples.

SCB registers are `scb/r<RegisterId>` in the new catalog until D binds their names; the
old catalog's are matched to them by the `native_id` of their register file in
`reg_meta_build/curation/registers/scb/`. A register whose old catalog has only the
implicit table and whose new catalog has one named table compares those two as one.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tomllib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

EXAMPLES = 8
PINS = Path(__file__).resolve().parents[1] / "reg_meta_build/curation/registers/scb"
# The old catalog's name for a register's one unnamed table, the new implicit table.
OLD_IMPLICIT = "_default"


def fold(text: str) -> str:
    return text.strip().lower()


def words(text: str | None) -> str:
    return " ".join((text or "").split()).lower()


def year(day: int, end: bool) -> int:
    # New catalogs store half-open day numbers counted from 1970-01-01.
    return (date(1970, 1, 1) + timedelta(days=day - 1 if end else day)).year


@dataclass
class View:
    tables: set[str] = field(default_factory=set)
    # column fold -> variable slugs; (column fold, variable) -> tables
    columns: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    placed: dict[tuple[str, str], set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    labels: dict[str, str] = field(default_factory=dict)
    codes: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # variable -> the sensitive and identifier values its rows state
    sensitive: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    identifier: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    years: dict[str, tuple[int, int]] = field(default_factory=dict)


def old_views(db: sqlite3.Connection, providers: set[str]) -> dict[str, View]:
    views: dict[str, View] = defaultdict(View)
    rows = db.execute(
        "SELECT p.slug || '/' || r.slug, v.slug FROM register_variant v"
        " JOIN register r USING (register_id) JOIN provider p USING (provider_id)"
    )
    for reg, table in rows:
        if reg.split("/")[0] in providers:
            views[reg].tables.add("" if table == OLD_IMPLICIT else table)
    rows = db.execute(
        "SELECT p.slug || '/' || r.slug, v.slug, v.name, v.is_sensitive, v.is_identifier,"
        " s.delivery_column_name, t.slug, s.valid_from, s.valid_to, s.value_set_id"
        " FROM variable_state s JOIN variable v USING (variable_id)"
        " JOIN register r ON r.register_id = v.register_id JOIN provider p USING (provider_id)"
        " JOIN register_variant t ON t.register_variant_id = s.register_variant_id"
    )
    sets: dict[str, list] = defaultdict(list)
    for reg, var, label, sens, ident, column, table, lo, hi, vs in rows:
        if reg.split("/")[0] not in providers:
            continue
        view = views[reg]
        table = "" if table == OLD_IMPLICIT else table
        view.columns[fold(column)].add(var)
        view.placed[(fold(column), var)].add(table)
        view.labels[var] = label or ""
        view.sensitive[var].add(bool(sens))
        view.identifier[var].add(bool(ident))
        # An unstated old bound is open.
        lo, hi = int((lo or "0001")[:4]), int((hi or "9999")[:4])
        old = view.years.get(var, (lo, hi))
        view.years[var] = (min(old[0], lo), max(old[1], hi))
        if vs is not None:
            sets[reg].append((var, vs))
    codes = db.execute(
        "SELECT m.value_set_id, c.code FROM value_set_member m JOIN value_code c USING (code_id)"
    )
    members: dict[int, set[str]] = defaultdict(set)
    for vs, code in codes:
        members[vs].add(code)
    for reg, pairs in sets.items():
        for var, vs in pairs:
            views[reg].codes[var] |= members[vs]
    return views


def new_views(db: sqlite3.Connection, providers: set[str]) -> dict[str, View]:
    views: dict[str, View] = defaultdict(View)
    for reg, pid, implicit in db.execute(
        "SELECT r.pid, t.pid, t.implicit FROM tbl t JOIN register r ON r.id = t.register_id"
    ):
        if reg.split("/")[0] in providers:
            views[reg].tables.add("" if implicit else pid.rsplit("/", 1)[1])
    rows = db.execute(
        "SELECT r.pid, v.pid, x.body, s.sensitive, s.identifier, sp.exact, t.pid, t.implicit,"
        " s.start, s.end, s.valueset_id"
        " FROM segment s JOIN variable v ON v.id = s.variable_id"
        " JOIN register r ON r.id = v.register_id JOIN tbl t ON t.id = s.table_id"
        " JOIN spelling sp ON sp.id = s.spelling_id JOIN text x ON x.id = v.label_text"
    )
    items: dict[int, set[str]] = defaultdict(set)
    for vs, code in db.execute(
        "SELECT valueset_id, c.body FROM vs_item JOIN text c ON c.id = code_text"
    ):
        items[vs].add(code)
    for reg, vpid, label, sens, ident, column, tpid, implicit, lo, hi, vs in rows:
        if reg.split("/")[0] not in providers:
            continue
        view, var = views[reg], vpid.rsplit("/", 1)[1]
        table = "" if implicit else tpid.rsplit("/", 1)[1]
        view.columns[fold(column)].add(var)
        view.placed[(fold(column), var)].add(table)
        view.labels[var] = label
        view.sensitive[var].add(None if sens is None else bool(sens))
        view.identifier[var].add(bool(ident))
        span = (year(lo, False), min(year(hi, True), 9999))
        old = view.years.get(var, span)
        view.years[var] = (min(old[0], span[0]), max(old[1], span[1]))
        if vs is not None:
            view.codes[var] |= items[vs]
    return views


def statuses(db: sqlite3.Connection) -> dict[tuple[str, str], set[str]]:
    """(register pid, column fold) -> statuses of the new catalog's occurrences."""
    out: dict[tuple[str, str], set[str]] = defaultdict(set)
    for reg, column, status in db.execute(
        "SELECT r.pid, sp.exact, o.status FROM occurrence o JOIN tbl t ON t.id = o.table_id"
        " JOIN register r ON r.id = t.register_id JOIN spelling sp ON sp.id = o.spelling_id"
    ):
        out[(reg, fold(column))].add(status)
    return out


def compare(reg: str, old: View, new: View, status, unminted: set[str], diffs) -> None:
    def add(cls: str, detail: str) -> None:
        diffs[cls].append((reg, detail))

    # The old catalog left a one-table register's table unnamed; the new one may name it.
    if old.tables == {""} and len(new.tables) == 1:
        only = next(iter(new.tables))
        new.tables = {""}
        new.placed = defaultdict(
            set,
            {k: {"" if t == only else t for t in ts} for k, ts in new.placed.items()},
        )

    for t in sorted(old.tables - new.tables):
        add("table only in old", t or "(implicit)")
    for t in sorted(new.tables - old.tables):
        add("table only in new", t or "(implicit)")
    shared: set[tuple[str, str]] = set()
    for column in sorted(old.columns.keys() | new.columns.keys()):
        o, n = old.columns.get(column, set()), new.columns.get(column, set())
        if not n:
            found = status.get((reg, column), set())
            cls = (
                "column withheld or rejected (outside its table's coverage)"
                if found & {"withheld", "rejected"}
                else "column only in old"
            )
            add(cls, f"{column} -> {', '.join(sorted(o))}")
        elif not o:
            add("column only in new", f"{column} -> {', '.join(sorted(n))}")
        elif o != n:
            if len(o) > 1:
                cls = "old split the column into several variables"
            elif n <= unminted:
                cls = "new variable without a ledger id"
            else:
                cls = "column mapped to another variable"
            add(cls, f"{column}: {', '.join(sorted(o))} -> {', '.join(sorted(n))}")
        for var in o & n:
            shared.add((column, var))
            if old.placed[(column, var)] != new.placed[(column, var)]:
                add(
                    "column in other tables",
                    f"{column}: {sorted(old.placed[(column, var)])}"
                    f" -> {sorted(new.placed[(column, var)])}",
                )
    for var in sorted({v for _, v in shared}):
        if words(old.labels[var]) != words(new.labels[var]):
            add("label differs", f"{var}: {old.labels[var]!r} -> {new.labels[var]!r}")
        if old.codes[var] != new.codes[var]:
            o, n = old.codes[var], new.codes[var]
            cls = (
                "codes only in old"
                if not n
                else "codes only in new"
                if not o
                # The blank code is SCB's "Uppgift saknas" item; D marks it a sentinel.
                else "codes: expected, the old catalog dropped the blank code"
                if n == o | {""}
                else "codes differ"
            )
            add(cls, f"{var}: {len(o)} -> {len(n)} codes, {len(o & n)} shared")
        os, stated = old.sensitive[var], new.sensitive[var] - {None}
        if not stated:
            add(
                "sensitive: unstated in the source, the old catalog defaulted it",
                f"{var}: {sorted(os)}",
            )
        elif stated != os:
            add("sensitive differs", f"{var}: {sorted(os)} -> {sorted(stated)}")
        oi, ni = old.identifier[var], new.identifier[var]
        if oi != ni:
            add("identifier differs", f"{var}: {sorted(oi)} -> {sorted(ni)}")
        if old.years[var] != new.years[var]:
            olo, nlo = old.years[var][0], new.years[var][0]
            cls = (
                "coverage: open start in new (a table states no start)"
                if nlo == 1 and olo != 1
                else "coverage: end differs"
                if olo == nlo
                else "coverage: start differs"
            )
            add(cls, f"{var}: {old.years[var]} -> {new.years[var]}")


VIA_TABLE = "JOIN tbl t ON t.id = x.table_id JOIN register r ON r.id = t.register_id"
# Columns of one count each: (heading, FROM clause naming the rows `x` and their
# register `r`, WHERE condition).
PLAIN_COUNTS = [
    ("tables", "tbl x JOIN register r ON r.id = x.register_id", "1"),
    (
        "timeless tables",
        "tbl x JOIN register r ON r.id = x.register_id",
        "x.timeless = 1",
    ),
    ("releases", f"release x {VIA_TABLE}", "1"),
    ("unparsed releases", f"release x {VIA_TABLE}", "x.parse_state = 'unparsed'"),
    (
        "clamped",
        "override x JOIN occurrence o ON o.id = x.subject_id"
        " JOIN tbl t ON t.id = o.table_id JOIN register r ON r.id = t.register_id",
        "x.subject_kind = 'occurrence' AND x.field = 'period' AND x.rule_code = 'T1'",
    ),
    ("variables", "variable x JOIN register r ON r.id = x.register_id", "1"),
    ("gaps", f"gap x {VIA_TABLE}", "1"),
]
# Columns that split one table by a key: (heading prefix, FROM clause, key
# expression, key values).
GROUPED_COUNTS = [
    (
        "occurrences",
        f"occurrence x {VIA_TABLE}",
        "x.status",
        ["used", "shadowed", "gap_marker", "rejected", "unplaced"],
    ),
    ("segments", f"segment x {VIA_TABLE}", "x.state", ["ambiguous", "preliminary"]),
    (
        "findings",
        "finding x JOIN register r ON r.id = x.register_id",
        # finding.code is a rule code plus its sub-kind ('F1:type-change').
        "substr(x.code, 1, instr(x.code || ':', ':') - 1)",
        ["F1", "F2", "F5", "F6", "F7", "F8", "F9", "T1"],
    ),
]


def counts(db: sqlite3.Connection, providers: set[str]) -> None:
    """Print the new catalog's build counts, one row per provider and a total row."""
    by_provider = " JOIN provider p ON p.id = r.provider_id"
    columns = [name for name, _, _ in PLAIN_COUNTS]
    columns += [f"{prefix} {k}" for prefix, _, _, keys in GROUPED_COUNTS for k in keys]
    rows = {p: dict.fromkeys(columns, 0) for p in sorted(providers)}
    for name, source, where in PLAIN_COUNTS:
        for pid, n in db.execute(
            f"SELECT p.pid, count(*) FROM {source}{by_provider} WHERE {where} GROUP BY 1"
        ):
            if pid in rows:
                rows[pid][name] = n
    for prefix, source, key, keys in GROUPED_COUNTS:
        for pid, value, n in db.execute(
            f"SELECT p.pid, {key}, count(*) FROM {source}{by_provider} GROUP BY 1, 2"
        ):
            if pid in rows and value in keys:
                rows[pid][f"{prefix} {value}"] = n
    print("\n## Build counts\n")
    print("| provider | " + " | ".join(columns) + " |")
    print("|---|" + "---:|" * len(columns))
    for pid, row in rows.items():
        print(f"| {pid} | " + " | ".join(str(row[c]) for c in columns) + " |")
    totals = (str(sum(row[c] for row in rows.values())) for c in columns)
    print("| total | " + " | ".join(totals) + " |")


def scb_handles() -> dict[str, str]:
    """`scb/<old slug>` -> `scb/r<RegisterId>`, from the register's `native_id`."""
    out = {}
    for path in sorted(PINS.glob("*.toml")):
        if path.name.endswith(".auto.toml"):
            continue
        register = tomllib.loads(path.read_text(encoding="utf-8")).get("register", {})
        if "native_id" in register:
            out[f"scb/{register['slug']}"] = f"scb/r{register['native_id']}"
    return out


def main() -> int:
    old_path, new_path, report_path, *wanted = sys.argv[1:]
    old_db, new_db = sqlite3.connect(old_path), sqlite3.connect(new_path)
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))["registers"]
    providers = set(wanted) or {
        p for (p,) in new_db.execute("SELECT pid FROM provider")
    }
    handles = scb_handles()
    old = {
        handles.get(reg, reg): view
        for reg, view in old_views(old_db, providers).items()
    }
    new = new_views(new_db, providers)
    status = statuses(new_db)
    diffs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for reg in sorted(old.keys() | new.keys()):
        if reg not in new:
            diffs["register only in old"].append((reg, ""))
            continue
        if reg not in old:
            diffs["register only in new"].append((reg, ""))
            continue
        unminted = set(report.get(reg, {}).get("unminted", []))
        compare(reg, old[reg], new[reg], status, unminted, diffs)

    print(f"# Rebuild comparison: {', '.join(sorted(providers))}\n")
    print("| class | differences | registers |")
    print("|---|---:|---|")
    for cls, found in sorted(diffs.items()):
        regs = Counter(r for r, _ in found)
        top = ", ".join(f"{r} {n}" for r, n in regs.most_common(6))
        print(f"| {cls} | {len(found)} | {top} |")
    counts(new_db, providers)
    for cls, found in sorted(diffs.items()):
        print(f"\n## {cls}\n")
        for reg, detail in found[:EXAMPLES]:
            print(f"- {reg}: {detail}")
        if len(found) > EXAMPLES:
            print(f"- … {len(found) - EXAMPLES} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
