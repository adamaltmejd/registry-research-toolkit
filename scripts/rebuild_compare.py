#!/usr/bin/env python3
"""rebuild_compare — what users see in the old catalog and the rebuilt one, per register.

    uv run --no-project scripts/rebuild_compare.py registers <old db> <new db> <report.json> [provider...]
    uv run --no-project scripts/rebuild_compare.py orders --old-url URL --new-url URL \
        --old-db OLD_DB --new-db NEW_DB --out DIR [--seed 0] [--n 20] [--provider P ...]
    uv run --no-project scripts/rebuild_compare.py holdings --old-db OLD_SWECOV.db \
        --new-base reg_meta.db --new-holdings reg_meta_swecov.db --out DIR \
        --old-census-sha256 SHA256 --new-census-path INPUT_PINS_KEY

Temporary (REBUILD.md, milestone A; deleted at F). `<old db>` is a released
`reg_meta.db`, `<new db>` a `reg-build` catalog and `<report.json>` its build report.
For every register of the given providers (default: every provider in the new catalog)
it compares tables, the columns each documents and their periods, the column-to-variable
mapping, labels, value-set codes, flags and coverage, and puts each difference in one
class. The registers/orders modes print Markdown summaries and examples. The holdings
mode writes private census/difference reports and prints aggregate counts only.
Its required old census digest is census_sha256 in the pinned old scope.toml;
use .local/release-0.64.0/ev/holdings-pins.txt to locate that input revision.
The release pins file identifies the input manifest, not the raw census digest.

Registers are matched by pid: a new register's public name comes from its id ledger,
which took it from the old slug. A register whose old catalog has only the implicit table and whose new
catalog has one named table compares those two as one.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import random
import re
import sqlite3
import sys
import tomllib
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import partial
from itertools import pairwise
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

EXAMPLES = 8
TIMELESS_START = -719162
TIMELESS_END = 2932897
# The old catalog's name for a register's one unnamed table, the new implicit table.
OLD_IMPLICIT = "_default"


def connect_ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


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
    # A segment whose occurrences disagree (F2) keeps no set; each occurrence keeps its own.
    for reg, vpid, vs in db.execute(
        "SELECT r.pid, v.pid, o.valueset_id FROM occurrence o"
        " JOIN segment s ON s.id = o.segment_id AND s.valueset_id IS NULL"
        " JOIN variable v ON v.id = s.variable_id JOIN register r ON r.id = v.register_id"
        " WHERE o.valueset_id IS NOT NULL"
    ):
        if reg.split("/")[0] in providers:
            views[reg].codes[vpid.rsplit("/", 1)[1]] |= items[vs]
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


def implicit_table_alias(old_tables: set[str], new_tables: set[str]) -> str | None:
    """The v4 unnamed table and v5's sole named table are the same coordinate."""
    if old_tables == {""} and len(new_tables) == 1:
        return next(iter(new_tables))
    return None


def compare(reg: str, old: View, new: View, status, unminted: set[str], diffs) -> None:
    def add(cls: str, detail: str) -> None:
        diffs[cls].append((reg, detail))

    # The old catalog left a one-table register's table unnamed; the new one may name it.
    if (only := implicit_table_alias(old.tables, new.tables)) is not None:
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
        ["used", "shadowed", "withheld", "gap_marker", "rejected", "unplaced"],
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


def registers(args) -> int:
    old_path, new_path, report_path, wanted = (
        args.old_db,
        args.new_db,
        args.report_json,
        args.providers,
    )
    old_db, new_db = sqlite3.connect(old_path), sqlite3.connect(new_path)
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))["registers"]
    providers = set(wanted) or {
        p for (p,) in new_db.execute("SELECT pid FROM provider")
    }
    old = old_views(old_db, providers)
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


def request_json(url: str, project=None, destination: Path | None = None):
    # HTTP accepts the project itself; the {project: ...} wrapper belongs to MCP.
    payload = None if project is None else json.dumps(project).encode()
    request = Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        response = urlopen(request, timeout=120)
    except HTTPError as error:
        response = error
    with response:
        raw, status = response.read(), response.code
    body = json.loads(raw)
    if destination is not None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw)
    if "data" not in body and "error" not in body:
        raise ValueError(f"{url}: HTTP {status} is not an API result")
    if project is None and (status != 200 or "error" in body):
        raise ValueError(f"{url}: request failed: HTTP {status}, {body}")
    return status, body


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def binding_type(dtype: str | None, identifier, coded) -> str:
    # Project types describe output semantics, not the catalog's storage dtype.
    if identifier:
        return "id"
    if coded:
        return "categorical"
    token = re.split(r"[ (]", (dtype or "").lower())[0]
    if token in {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "hugeint",
        "decimal",
        "numeric",
        "real",
        "float",
        "double",
        "money",
        "smallmoney",
        "bit",
        "heltal",
        "decimaltal",
        "numerisk",
    }:
        return "numeric"
    if token in {"datetime", "datetime2", "timestamp", "smalldatetime"}:
        return "datetime"
    if token in {"date", "datum"}:
        return "date"
    if token in {"identifierare", "uniqueidentifier"}:
        return "id"
    return "opaque"


def iso_day(day: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=day)).isoformat()


def project_period(lo: str, hi: str, source: str, adjustments: list):
    # Project grammar has finite bounds even when catalog coverage is open.
    period = {"from": max(lo, "1900-01-01"), "to": min(hi, "2099-12-31")}
    if period != {"from": lo, "to": hi}:
        adjustments.append(
            {"source": source, "catalog": {"from": lo, "to": hi}, "requested": period}
        )
    return period


def new_project(db, register: str, version: str, adjustments: list, omissions: list):
    sources = []
    for tid, pid, timeless, lo, hi in db.execute(
        "SELECT t.id, t.pid, t.timeless, min(s.start), max(s.end) FROM tbl t"
        " JOIN register r ON r.id = t.register_id"
        " LEFT JOIN tbl_span s ON s.table_id = t.id WHERE r.pid = ?"
        " GROUP BY t.id ORDER BY t.pid",
        (register,),
    ):
        bindings = {}
        for var, dtype, identifier, coded in db.execute(
            "SELECT v.pid, s.dtype, s.identifier, s.coded FROM segment s"
            " JOIN variable v ON v.id = s.variable_id WHERE s.table_id = ?"
            " ORDER BY v.pid, s.start",
            (tid,),
        ):
            bindings.setdefault(
                var,
                {
                    "variable": var,
                    "type": binding_type(dtype, identifier, coded == "yes"),
                },
            )
        if not bindings:
            omissions.append(pid)
            continue
        # A table with no spans uses its stated hull; a refusal stays evidence.
        if lo is None:
            lo, hi = db.execute(
                "SELECT start, end FROM tbl WHERE id = ?", (tid,)
            ).fetchone()
        sources.append(
            {
                "name": f"source-{len(sources)}",
                "table": pid,
                "period": "_default"
                if timeless
                else project_period(iso_day(lo), iso_day(hi - 1), pid, adjustments),
                "bindings": list(bindings.values()),
            }
        )
    return {
        "schema_version": "5.1.0",
        "reg_meta_version": version,
        "name": "compare-" + register.replace("/", "-"),
        "sources": sources,
    }


def old_states(url, variable, variant, period, evidence):
    query = {"variant": variant, "scope": "reference", "limit": 200}
    if period != "_default":
        query["period"] = f"{period['from']}..{period['to']}"
    found = []
    while True:
        _, body = request_json(
            url.rstrip("/") + f"/api/states/{variable}?" + urlencode(query),
            destination=evidence / f"{len(found):04}.json",
        )
        found.extend(body["data"]["items"])
        # 0.64.0 silently stops paging at depth 1000, even with unseen states.
        if len(found) >= 1000:
            raise ValueError(
                f"{variable} ({variant}): old states reached the 1000-item cap; completeness is unknown"
            )
        cursor = body["data"]["next_cursor"]
        if cursor is None:
            return [
                state
                for state in found
                if period != "_default" or state["period_scope"] == "year_independent"
            ]
        query["cursor"] = cursor


def old_project(
    db,
    register: str,
    version: str,
    adjustments: list,
    omissions: list,
    *,
    url,
    evidence,
):
    sources = []
    for tid, slug in db.execute(
        "SELECT t.register_variant_id, t.slug FROM register_variant t"
        " JOIN register r USING (register_id) JOIN provider p USING (provider_id)"
        " WHERE p.slug || '/' || r.slug = ? ORDER BY t.slug",
        (register,),
    ):
        states = list(
            db.execute(
                "SELECT v.slug, s.valid_from, s.valid_to FROM variable_state s"
                " JOIN variable v USING (variable_id) WHERE s.register_variant_id = ?"
                " ORDER BY v.slug, s.valid_from, s.delivery_column_name",
                (tid,),
            )
        )
        dated = [(lo, hi) for _, lo, hi in states if lo is not None]
        period = (
            project_period(
                min(lo for lo, _ in dated),
                max(hi for _, hi in dated),
                f"{register}/{slug}",
                adjustments,
            )
            if dated
            else "_default"
        )
        bindings = {}
        for var in sorted({var for var, _, _ in states}):
            variable = f"{register}/{var}"
            representations = old_states(
                url, variable, slug, period, evidence / register / slug / var
            )
            if not representations:
                # Keep unavailable variables in the request for the order to refuse.
                bindings[variable, None] = {"variable": variable, "type": "opaque"}
            for state in representations:
                spelling = state["delivery_column_name"]
                binding = {
                    "variable": variable,
                    "type": binding_type(
                        state["data_type"],
                        state["is_identifier"],
                        state["value_set_id"] is not None,
                    ),
                }
                if spelling is not None:
                    binding["representation"] = spelling
                bindings.setdefault((variable, spelling), binding)
        if not bindings:
            omissions.append(f"{register}/{slug}")
            continue
        sources.append(
            {
                "name": f"source-{len(sources)}",
                "register_variant": f"{register}/{slug}",
                "period": period,
                "bindings": list(bindings.values()),
            }
        )
    return {
        "schema_version": "4.0.0",
        "steward": "global",
        "reg_meta_version": version,
        "name": "compare-" + register.replace("/", "-"),
        "sources": sources,
    }


def old_token_bounds(token: str) -> tuple[str, str | None]:
    """Old inclusive period token to half-open ISO days (interval.rs::render)."""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", token):
        end = (
            None
            if token == "9999-12-31"
            else (date.fromisoformat(token) + timedelta(days=1)).isoformat()
        )
        return token, end
    if token[:2] in {"HT", "VT", "LA"}:
        y = int(token[2:])
        if token.startswith("VT"):
            return f"{y:04}-01-01", f"{y:04}-07-01"
        if token.startswith("HT"):
            return f"{y:04}-07-01", f"{y + 1:04}-01-01"
        return f"{y:04}-07-01", f"{y + 1:04}-07-01"
    match = re.fullmatch(r"(\d{4})(?:-(\d{2}|Q[1-4]|H[12]))?", token)
    if not match:
        raise ValueError(f"unrecognised old manifest period token: {token!r}")
    y, part = int(match[1]), match[2]
    month, width = 1, 12
    if part:
        width = 3 if part.startswith("Q") else 6 if part.startswith("H") else 1
        month = (int(part[1:]) - 1) * width + 1 if part[0] in "QH" else int(part)
    # Validate month/year without constructing year 10000 for the open sentinel.
    calendar.monthrange(y, month)
    end_y, end_m = y + (month - 1 + width) // 12, (month - 1 + width) % 12 + 1
    return f"{y:04}-{month:02}-01", (
        None if end_y == 10000 else f"{end_y:04}-{end_m:02}-01"
    )


def old_periods(wire: str):
    if wire == "_default":
        return [(None, None)]
    intervals = []
    for piece in wire.split(","):
        endpoints = piece.strip().split("..")
        lo = old_token_bounds(endpoints[0])[0]
        hi = old_token_bounds(endpoints[-1])[1]
        intervals.append((None if lo == "0001-01-01" else lo, hi))
    return intervals


def table_names(register: str, old: View, new: View):
    """Correspond catalog tables, including omitted sources, without mutating views."""
    old_tables = {f"{register}/{table or OLD_IMPLICIT}": table for table in old.tables}
    new_tables = {
        f"{register}/{table}" if table else register: table for table in new.tables
    }
    if set(old_tables.values()) == {""} and len(new_tables) == 1:
        new_tables = dict.fromkeys(new_tables, "")
    return old_tables, new_tables


def order_rows(body, tables, old: bool):
    rows = defaultdict(set)
    for entry in body.get("data", {}).get("entries", []):
        if old:
            logical = entry["logical"]
            tid = "/".join(logical[k] for k in ("provider", "register", "variant"))
            variable = logical["variable"]
            spelling = entry["physical"]["column"]
            periods = old_periods(entry["requested_period"])
        else:
            tid, variable, spelling = (
                entry["table"],
                entry["variable"],
                entry["spelling"],
            )
            periods = [(entry["period"]["start"], entry["period"]["end"])]
        # A succession may return a table not named by a source.
        table = tables.get(
            tid, "" if tid.endswith("/" + OLD_IMPLICIT) else tid.rsplit("/", 1)[1]
        )
        for lo, hi in periods:
            rows[(table, variable, lo, hi, spelling)].add(entry["source"])
    return dict(rows)


def blocking_issues(body, project):
    error = body.get("error", {})
    fields = error.get("fields", {})
    issues = fields.get("findings", fields.get("issues", []))
    blocked = defaultdict(list)
    for issue in issues:
        if issue.get("level", "error") != "error":
            continue
        source = issue.get("source")
        path = issue.get("path", "").split("/")
        if (
            source is None
            and len(path) > 2
            and path[1] == "sources"
            and path[2].isdigit()
        ):
            source = project["sources"][int(path[2])]["name"]
        blocked[source].append(issue)
    if error and not blocked:
        blocked[None].append(error)
    return dict(blocked)


def row_value(row):
    if row is None:
        return None
    table, variable, start, end, spelling = row
    return {
        "table": table,
        "variable": variable,
        "period": {"start": start, "end": end},
        "spelling": spelling,
    }


def form_boundary(db, table, variable, boundary, tables):
    """Adjacent catalog form and class-version changes, independent of order partitions."""
    before, after = set(), set()
    for tid, start, end, dtype, valueset, version in db.execute(
        "SELECT t.pid, s.start, s.end, s.dtype, s.valueset_id, s.class_version_id FROM segment s"
        " JOIN variable v ON v.id = s.variable_id JOIN tbl t ON t.id = s.table_id"
        " WHERE v.pid = ? AND (s.start = ? OR s.end = ?)",
        (variable, boundary, boundary),
    ):
        if tables.get(tid, tid.rsplit("/", 1)[-1]) == table:
            if end == boundary:
                before.add((dtype, valueset, version))
            if start == boundary:
                after.add((dtype, valueset, version))
    if len(before) != 1 or len(after) != 1:
        return False, False
    left, right = next(iter(before)), next(iter(after))
    return left[:2] != right[:2], left[2] != right[2]


def row_differences(old, new, db, tables):
    """Compare physical columns on shared day slices, retaining original IDs."""
    groups = defaultdict(lambda: [[], []])
    for side, rows in enumerate((old, new)):
        for row in sorted(rows, key=lambda r: tuple(v or "" for v in r)):
            groups[row[0], row[4].lower()][side].append(row)
    for (table, _), sides in sorted(groups.items()):
        if sides[0] == sides[1]:
            continue
        if not sides[0] or not sides[1]:
            for i, side in enumerate(sides):
                for row in side:
                    yield (row, None, None) if i == 0 else (None, row, None)
            continue
        spans = [[row_days(r) for r in side] for side in sides]
        partitions = [{point for span in side for point in span} for side in spans]
        equal_coverage = all(
            not uncovered(span, spans[1 - i]) for i in (0, 1) for span in spans[i]
        )
        extra = partitions[1] - partitions[0]
        # Every new interior partition needs catalog evidence, including spelling
        # changes between resolved runs. A flag-only split is still unexplained.
        supported = {}
        for point in extra:
            adjacent = [r for r in sides[1] if point in row_days(r)]
            changes = [form_boundary(db, table, r[1], point, tables) for r in adjacent]
            forms = any(change[0] for change in changes)
            versions = any(change[1] for change in changes)
            before = {
                r[4] for r in sides[1] if row_days(r)[0] < point <= row_days(r)[1]
            }
            after = {r[4] for r in sides[1] if row_days(r)[0] <= point < row_days(r)[1]}
            spelling = bool(before and after and before != after) and all(
                latest_spelling(db, r, tables) for r in adjacent
            )
            old_identity = {
                r[1] for r in sides[1] if row_days(r)[0] < point <= row_days(r)[1]
            }
            new_identity = {
                r[1] for r in sides[1] if row_days(r)[0] <= point < row_days(r)[1]
            }
            identity = bool(
                old_identity and new_identity and old_identity != new_identity
            )
            supported[point] = (
                not (before and after) or forms or versions or spelling or identity,
                forms,
                versions,
            )
        for lo, hi in pairwise(sorted(partitions[0] | partitions[1])):
            active = [
                [r for r in side if row_days(r)[0] <= lo and hi <= row_days(r)[1]]
                for side in sides
            ]
            pairs = []
            used = set()
            for left in active[0]:
                matches = [r for r in active[1] if r[1] == left[1]] or active[1]
                if len(matches) == 1:
                    pairs.append((left, matches[0]))
                    used.add(matches[0])
                else:
                    pairs.append((left, None))
            pairs.extend((None, r) for r in active[1] if r not in used)
            for left, right in pairs:
                if left == right:
                    continue
                pair = [
                    None
                    if r is None
                    else (
                        table,
                        r[1],
                        None if lo == TIMELESS_START else iso_day(lo),
                        None if hi == TIMELESS_END else iso_day(hi),
                        r[4],
                    )
                    for r in (left, right)
                ]
                boundaries = [
                    p
                    for p in extra
                    if left and row_days(left)[0] < p < row_days(left)[1]
                ]
                valid = all(supported[p][0] for p in boundaries)
                forms = any(supported[p][1] for p in boundaries)
                versions = any(supported[p][2] for p in boundaries)
                # A removed old boundary is explained by a case or identity change,
                # never by a redundant split with the same spelling and ID.
                removed = [
                    p
                    for p in partitions[0] - partitions[1]
                    if right and row_days(right)[0] < p < row_days(right)[1]
                ]
                for point in removed:
                    before = {
                        (r[1], r[4])
                        for r in sides[0]
                        if row_days(r)[0] < point <= row_days(r)[1]
                    }
                    after = {
                        (r[1], r[4])
                        for r in sides[0]
                        if row_days(r)[0] <= point < row_days(r)[1]
                    }
                    valid &= not (before and after) or before != after
                if pair[0] == pair[1] and valid and not forms and not versions:
                    continue
                yield (
                    *pair,
                    {
                        "supported": valid,
                        "equal_coverage": equal_coverage,
                        "form_boundary": forms,
                        "classification_version": versions,
                        "original_old": row_value(left),
                        "original_new": row_value(right),
                    },
                )


CLASSES = {
    "blocked window": "intended",
    "preliminary data": "intended",
    "old blocked": "intended",
    "identity change": "intended",
    "old curation": "D curation",
    "unplaced release": "D curation",
    "clipped period": "intended",
    "form boundary": "intended",
    "classification version": "intended",
    "spelling": "intended",
    "representation only": "intended",
    "table split or merge": "defect",
    "missing in new": "defect",
    "missing in old": "defect",
    "other": "defect",
}


def period_days(period):
    epoch = date(1970, 1, 1)
    return tuple(
        (date.fromisoformat(value) - epoch).days if value else sentinel
        for value, sentinel in (
            (period["start"], TIMELESS_START),
            (period["end"], TIMELESS_END),
        )
    )


def uncovered(span, coverage):
    """The half-open pieces of span not covered by coverage."""
    start, end = span
    out = []
    for lo, hi in sorted(coverage):
        if hi <= start or lo >= end:
            continue
        if lo > start:
            out.append((start, lo))
        start = max(start, hi)
    if start < end:
        out.append((start, end))
    return out


def row_days(row):
    return period_days({"start": row[2], "end": row[3]})


def order_findings(body):
    fields = body.get("error", {}).get("fields", {})
    return body.get("data", {}).get(
        "findings", fields.get("findings", fields.get("issues", []))
    )


def finding_periods(body, code, row, tables, sources, *, variables=()):
    spans = []
    for issue in order_findings(body):
        table = tables.get(
            issue.get("table"), (issue.get("table") or "").rsplit("/", 1)[-1]
        )
        if (
            issue["code"] == code
            and (
                issue.get("variable") == row[1]
                or issue.get("variable") in variables
                or (
                    code == "table_succession_undated" and issue.get("variable") is None
                )
            )
            and table == row[0]
            and issue.get("source") in sources
            and issue.get("period") is not None
        ):
            spans.append(period_days(issue["period"]))
    return spans


def placement_explanations(register, catalogs, tables):
    # Catalog placements are independent evidence for delivered table movements.
    views = [View(), View()]
    for i, (view, catalog) in enumerate(zip(views, catalogs, strict=True)):
        names = {
            table: tables[i].get(
                f"{register}/{table or OLD_IMPLICIT}"
                if i == 0
                else f"{register}/{table}"
                if table
                else register,
                table,
            )
            for table in catalog.tables
        }
        view.tables.update(names.values())
        for (column, slug), placed in catalog.placed.items():
            for table in placed:
                table = names[table]
                variable = f"{register}/{slug}"
                view.columns[column].add(variable)
                view.placed[column, variable].add(table)
                view.labels[variable], view.years[variable] = "", (0, 0)
    diffs = defaultdict(list)
    compare("", *views, {}, set(), diffs)
    return views, {detail for _, detail in diffs["column in other tables"]}


def latest_spelling(db, row, tables):
    # The emitted segment is the resolver's latest used-occurrence winner; do not
    # invent another edition tie-break here. It must cover the whole compared row.
    lo, hi = row_days(row)
    spans = []
    for tid, start, end, spelling, state in db.execute(
        "SELECT t.pid, s.start, s.end, sp.exact, s.state FROM segment s"
        " JOIN variable v ON v.id = s.variable_id JOIN tbl t ON t.id = s.table_id"
        " JOIN spelling sp ON sp.id = s.spelling_id"
        " WHERE v.pid = ? AND s.start < ? AND s.end > ?",
        (row[1], hi, lo),
    ):
        if tables.get(tid, tid.rsplit("/", 1)[-1]) == row[0]:
            if spelling != row[4] or state == "ambiguous":
                return False
            spans.append((start, end))
    return not uncovered((lo, hi), spans)


def column_corrections(db, register, tables, records, curation):
    """Read explicit header errata from the released source; generic tags are insufficient."""
    if curation is None:
        return []
    path = curation / "registers" / (register + ".toml")
    if not path.is_file():
        return []
    source = tomllib.loads(path.read_text())
    info = source["register"]
    if f"{info['provider']}/{info['slug']}" != register:
        raise ValueError(f"{path}: register does not match {register}")
    variants = {v["native_id"]: v["slug"] for v in source.get("variant", [])}
    partitions = {
        p["variable"]: p["columns"]
        for p in source.get("identity", {}).get("partition", [])
    }
    variables = dict(
        db.execute(
            "SELECT v.provider_key, v.slug FROM variable v JOIN register r USING (register_id)"
            " JOIN provider p USING (provider_id) WHERE p.slug || '/' || r.slug = ?"
            " AND v.provider_key IS NOT NULL",
            (register,),
        )
    )
    corrections = []
    for index, entry in enumerate(source.get("errata", {}).get("field", []), 1):
        if entry.get("field") != "column_name":
            continue
        original, corrected = entry["column"], entry["value"]
        if not any(
            f.get("name") == "column_name"
            and f.get("status") == "value"
            and f.get("value") == original
            for f in entry.get("expected_fields", [])
        ):
            continue
        native = entry["variable"]
        if native in partitions:
            targets = partitions[native]
            if original not in targets or targets.get(original) != targets.get(
                corrected
            ):
                continue
            native = targets[original]
        elif native not in source.get("variable", {}):
            continue
        if not native.startswith(info["native_id"] + "."):
            continue
        variable = variables.get(native.split(".", 1)[1])
        slug = variants.get(entry["variant"])
        if variable is None or slug is None:
            continue
        # simplify: only explicit annual edition guards; add other selectors when a comparison needs them.
        editions, periods = (
            entry.get("editions", []),
            entry.get("expected_period_text", {}),
        )
        if (
            not editions
            or entry.get("expected_records")
            or not isinstance(periods, dict)
        ):
            continue
        years = [periods.get(edition) for edition in editions]
        if not all(isinstance(y, str) and re.fullmatch(r"\d{4}", y) for y in years):
            continue
        scope = (
            tables[f"{register}/{slug}"],
            f"{register}/{variable}",
            corrected.lower(),
        )
        spans = [
            period_days({"start": a, "end": b})
            for y in years
            for a, b in old_periods(y)
        ]
        for item in records:
            if (
                item["table"],
                item["variable"],
                item["column"],
            ) != scope or "errata:field" not in item["tags"]:
                continue
            overlap = [
                (max(a, c), min(b, d))
                for a, b in spans
                for c, d in item["spans"]
                if max(a, c) < min(b, d)
            ]
            if overlap:
                corrections.append(
                    item
                    | {
                        "spans": overlap,
                        "header_from": original.lower(),
                        "tags": {
                            f"errata:field column_name ({path}#/errata.field/{index})"
                        },
                    }
                )
    return corrections


def curation_evidence(db, register, tables, curation=None):
    """Only published provenance for physical additions/periods/representations."""
    records = []
    families = {}
    for relation in ("variable_state", "variable_alias_window"):
        for slug, variable, column, lo, hi, provenance in db.execute(
            f"SELECT t.slug, v.slug, s.delivery_column_name, s.valid_from, s.valid_to, s.provenance"
            f" FROM {relation} s JOIN variable v USING (variable_id)"
            " JOIN register r USING (register_id) JOIN provider p USING (provider_id)"
            " JOIN register_variant t ON t.register_variant_id = s.register_variant_id"
            " WHERE p.slug || '/' || r.slug = ?",
            (register,),
        ):
            table = tables.get(
                f"{register}/{slug}", "" if slug == OLD_IMPLICIT else slug
            )
            tags = set((provenance or "").splitlines())
            spans = [
                period_days({"start": a, "end": b})
                for a, b in old_periods(f"{lo}..{hi}" if lo else "_default")
            ]
            item = {
                "table": table,
                "variable": f"{register}/{variable}",
                "column": (column or "").lower(),
                "spans": spans,
                "tags": tags,
            }
            records.append(item)
            if "curated:representation.period_family" in tags:
                families.setdefault((table, item["variable"]), []).extend(spans)
    for item in records:
        family = families.get((item["table"], item["variable"]))
        if family is not None:
            # Aliases inherit only their recorded family's transformation, not
            # arbitrary curation of another spelling of the same variable.
            item["family_spans"] = family
    records.extend(column_corrections(db, register, tables, records, curation))
    for variable, slug, column, lo, hi, evidence in db.execute(
        "SELECT v.slug, t.slug, w.delivery_column_name, w.valid_from, w.valid_to, w.evidence_json"
        " FROM data_warning w JOIN register r USING (register_id) JOIN provider p USING (provider_id)"
        " JOIN variable v ON v.variable_id = w.variable_id"
        " LEFT JOIN register_variant t ON t.register_variant_id = w.register_variant_id"
        " WHERE p.slug || '/' || r.slug = ? AND w.code = 'unresolved_column_representation'",
        (register,),
    ):
        evidence = json.loads(evidence)
        acknowledged = evidence.get("acknowledged_by")
        if (
            "state" not in evidence.get("withheld_output", [])
            or "column_name" not in evidence.get("fields", [])
            or not isinstance(acknowledged, str)
            or not acknowledged.startswith("curation/")
        ):
            continue
        records.append(
            {
                "table": tables[f"{register}/{slug}"] if slug is not None else None,
                "variable": f"{register}/{variable}",
                "column": column.lower() if column is not None else None,
                "spans": [
                    period_days(
                        {"start": lo, "end": old_token_bounds(hi)[1] if hi else None}
                    )
                ],
                "tags": {
                    f"data_warning:unresolved_column_representation; acknowledged by {acknowledged}"
                },
                "withheld": True,
            }
        )
    return records


def unplaced_columns(db, register, tables):
    return {
        (tables.get(tid, tid.rsplit("/", 1)[-1]), spelling.lower())
        for tid, spelling in db.execute(
            "SELECT t.pid, sp.exact FROM occurrence o JOIN tbl t ON t.id = o.table_id"
            " JOIN register r ON r.id = t.register_id JOIN release rel ON rel.id = o.release_id"
            " JOIN spelling sp ON sp.id = o.spelling_id"
            " WHERE r.pid = ? AND o.status = 'unplaced' AND rel.parse_state = 'unparsed'"
            " AND NOT EXISTS (SELECT 1 FROM segment s JOIN spelling placed ON placed.id = s.spelling_id"
            " WHERE s.table_id = o.table_id AND placed.fold_id = sp.fold_id)",
            (register,),
        )
    }


def old_curation(row, curation, *, added=False, family_only=False, counterparts=()):
    if row is None:
        return None
    physical_tags = {
        "errata:omitted-column-in-version",
        "errata:additional-physical-column-in-version",
        "errata:steward-holdings",
        "errata:scb-docs",
        "errata:occurrence_period",
        "errata:occurrence_carry",
        "errata:blank-column-name-in-version",
        "curated:representations",
    }
    spans, reasons = [], set()
    for item in curation:
        if item.get("withheld"):
            if (
                added
                and not family_only
                and item["variable"] == row[1]
                and item["table"] in (None, row[0])
                and item["column"] in (None, row[4].lower())
            ):
                spans.extend(item["spans"])
                reasons.update(item["tags"])
            continue
        if item["table"] != row[0]:
            continue
        same_column = item["column"] == row[4].lower()
        if same_column and item.get("family_spans"):
            spans.extend(item["family_spans"])
            reasons.add("curated:representation.period_family")
        if family_only:
            continue
        relevant = item["tags"] & physical_tags
        header = (
            "header_from" in item
            and item["variable"] == row[1]
            and (
                (
                    added
                    and row[4].lower() == item["header_from"]
                    and any(
                        r[0] == row[0]
                        and r[1] == row[1]
                        and r[4].lower() == item["column"]
                        and not uncovered(row_days(row), [row_days(r)])
                        for r in counterparts
                    )
                )
                or (
                    not added
                    and same_column
                    and any(
                        r[0] == row[0]
                        and r[1] == row[1]
                        and r[4].lower() == item["header_from"]
                        and not uncovered(row_days(row), [row_days(r)])
                        for r in counterparts
                    )
                )
            )
        )
        if (same_column and not added and relevant) or header:
            spans.extend(item["spans"])
            reasons.update(item["tags"] if header else relevant)
    if reasons and not uncovered(row_days(row), spans):
        return "; ".join(sorted(reasons))
    return None


def classify_difference(
    record,
    old=None,
    new=None,
    *,
    rows,
    bodies,
    tables,
    dbs,
    placements,
    curation,
    unplaced,
    correspondences=(),
):
    def result(cls, reason, owner=None, review=None):
        classified = record | {
            "class": cls,
            "owner": owner or CLASSES[cls],
            "reason": reason,
        }
        if review:
            classified["review"] = review
        return classified

    issues = [
        (
            [i for i in order_findings(body) if i.get("level", "error") == "error"]
            or [body["error"]]
        )
        if "error" in body
        else []
        for body in bodies
    ]
    if record["kind"] == "register_only":
        return result("missing in old", "The sampled register has no old counterpart.")
    # A refusal suppresses every source: no row on that side proves availability.
    if record["kind"] == "refusal" or any(issues):
        if (
            rows[0]
            and issues[1]
            and all(i["code"] == "ambiguous_window" for i in issues[1])
        ):
            return result(
                "blocked window",
                "The new order refuses ambiguous windows; old entries exist.",
            )
        if rows[1] and issues[0]:
            return result(
                "old blocked",
                "The old order refused; the new order delivers entries.",
                review="Operator: determine whether the old refusal was a bug.",
            )
        return result(
            "other",
            "No delivered counterpart proves the refusal's cause.",
            review="Operator: inspect both refusal responses.",
        )
    alignment = record.get("alignment") or {}
    # Direct table-scoped identity matches explain findings and retained
    # representations without rewriting either side's evidence or inferring transitive IDs.
    new_ids = (
        (
            {old[1]}
            | {
                new_id
                for table, old_id, new_id in correspondences
                if table == old[0] and old_id == old[1]
            }
        )
        if old
        else set()
    )
    family = old_curation(old or new, curation, family_only=True)
    if family and not alignment.get("equal_coverage", True):
        return result(
            "old curation",
            "Old catalog records " + family + "; monthly and annual coverage differ.",
        )
    if (
        old
        and new
        and old[1] != new[1]
        and old[0] == new[0]
        and old[2:4] == new[2:4]
        and old[4].lower() == new[4].lower()
        and alignment.get("supported", False)
    ):
        return result(
            "identity change",
            "The same physical column covers these days under different variable IDs.",
        )
    if old is not None and new is None:
        if (old[0], old[4].lower()) in unplaced:
            return result(
                "unplaced release",
                "The new catalog records this column as unplaced in an unparsed release; no dated coverage is inferred.",
            )
        curated = old_curation(old, curation, counterparts=rows[1])
        if curated:
            return result("old curation", "Old catalog provenance: " + curated + ".")
    if new is not None and old is None:
        curated = old_curation(new, curation, added=True, counterparts=rows[0])
        if curated:
            return result(
                "old curation",
                "The old catalog's curation or acknowledged withholding explains the raw counterpart: "
                + curated
                + ".",
            )
    if new and (old is None or (old[:2] == new[:2] and old[4] == new[4])):
        previous = [
            row_days(r)
            for r in rows[0]
            if r[0] == new[0] and r[4].lower() == new[4].lower()
        ]
        added = uncovered(row_days(new), previous)
        preliminary = finding_periods(
            bodies[1], "preliminary_data", new, tables[1], record["sources"]["new"]
        )
        # An unrelated spelling or removed period is not explained by new preliminary coverage.
        removed = uncovered(row_days(old), [row_days(new)]) if old else []
        same_spelling = [
            row_days(r) for r in rows[0] if r[:2] == new[:2] and r[4] == new[4]
        ]
        changed = uncovered(row_days(new), same_spelling)
        if (
            added
            and not removed
            and all(not uncovered(span, preliminary) for span in added)
            and all(not uncovered(span, added) for span in changed)
        ):
            return result(
                "preliminary data",
                "Only warned preliminary windows add coverage absent from the old order.",
            )
    if old and (
        new is None or (old[0] == new[0] and new[1] in new_ids and old[4] == new[4])
    ):
        removed = uncovered(
            row_days(old),
            [
                row_days(r)
                for r in rows[1]
                if r[0] == old[0] and r[4].lower() == old[4].lower()
            ],
        )
        added = (
            uncovered(
                row_days(new),
                [
                    row_days(r)
                    for r in rows[0]
                    if r[0] == new[0] and r[4].lower() == new[4].lower()
                ],
            )
            if new
            else []
        )
        clips = finding_periods(
            bodies[1],
            "period_clipped",
            old,
            tables[1],
            record["sources"]["new"],
            variables=new_ids,
        )
        clips += finding_periods(
            bodies[1],
            "table_succession_undated",
            old,
            tables[1],
            record["sources"]["new"],
            variables=new_ids,
        )
        if (
            removed
            and not added
            and all(not uncovered(span, clips) for span in removed)
        ):
            return result(
                "clipped period", "New clipping findings account for every removed day."
            )
    if (
        old
        and new
        and old[:4] == new[:4]
        and old[4] != new[4]
        and old[4].lower() == new[4].lower()
        and alignment.get("supported", True)
        and latest_spelling(dbs[1], new, tables[1])
    ):
        return result(
            "spelling",
            "Case-only column spelling uses the resolver's latest used-occurrence spelling."
            + (
                " Equal coverage also retains catalog type/value-set boundaries."
                if record.get("alignment", {}).get("form_boundary")
                else ""
            ),
        )
    if old is not None and new is not None and record.get("alignment") is not None:
        if (
            old[4] == new[4]
            and alignment["supported"]
            and alignment["classification_version"]
        ):
            return result(
                "classification version",
                "Equal covered days; catalog classification-version changes explain additional splits, alongside any type/value-set changes.",
            )
        if old[4] == new[4] and alignment["supported"] and alignment["form_boundary"]:
            return result(
                "form boundary",
                "Equal covered days; every additional new split has a catalog type/value-set change.",
            )
        return result(
            "other", "Equal coverage has an unexplained partition or spelling change."
        )
    if old and new is None:
        alternatives = {r[4] for r in rows[0] if r[:4] == old[:4]}
        retained = {
            r[4]
            for r in rows[1]
            if r[0] == old[0] and r[1] in new_ids and r[2:4] == old[2:4]
        }
        if len(alternatives) > 1 and retained and retained <= alternatives:
            return result(
                "representation only",
                "The old order delivers parallel representations; the new order retains one of them.",
            )
    row = old or new
    if row is None:
        raise ValueError("a row difference must have an old or new row")
    other_rows = rows[1] if old else rows[0]
    if (old is None or new is None) and any(
        r[0] != row[0] and r[1:] == row[1:] for r in other_rows
    ):
        views, explanations = placements
        key = fold(row[4]), row[1]
        detail = f"{key[0]}: {sorted(views[0].placed[key])} -> {sorted(views[1].placed[key])}"
        side = 0 if old else 1
        explained = (
            detail in explanations
            and row[0] in views[side].placed[key]
            and any(
                r[1:] == row[1:] and r[0] in views[1 - side].placed[key]
                for r in other_rows
            )
        )
        return result(
            "table split or merge" if explained else "other",
            "registers: column in other tables: " + detail
            if explained
            else "An identical row moves tables without a registers placement explanation.",
            owner="intended" if explained else "defect",
            review=None
            if explained
            else "Operator: the moved row has no registers placement explanation.",
        )
    if new is None:
        return result("missing in new", "No rule explains the old-only row.")
    if old is None:
        return result("missing in old", "No rule explains the new-only row.")
    return result(
        "other",
        "The remaining period/spelling change is unexplained.",
        review="Operator: inspect the retained response and catalog evidence.",
    )


def classification_summary(records, mappings):
    summary = [
        "",
        "## Variable ID mapping",
        "",
        f"Identity correspondences: {len(mappings)} (distinct register/table/old ID/new ID pairs on shared column/day slices).",
    ]
    summary += [
        f"- `{m['register']}` / `{m['table'] or '(implicit)'}`: `{m['old']}` → `{m['new']}`"
        for m in mappings[:5]
    ]
    summary += [
        "",
        "## Classes",
        "",
        "Whole-order refusals take precedence. Otherwise: old monthly curation, identity change, unplaced release, old curation, preliminary data, clipped period, spelling, classification version, form boundary, representation only, table split or merge, missing, other.",
        "Refusal records are counted separately from affected rows. Old refusals and unexplained differences require Operator review.",
        "",
        "| class | rows | refusals | register-only records | registers | owner |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for cls, default_owner in CLASSES.items():
        found = [r for r in records if r["class"] == cls]
        counts = Counter(r["kind"] for r in found)
        owners = "; ".join(sorted({r["owner"] for r in found})) or default_owner
        summary.append(
            f"| {cls} | {counts['row']} | {counts['refusal']} | {counts['register_only']} | "
            f"{len({r['register'] for r in found})} | {owners} |"
        )
    for cls in CLASSES:
        found = [r for r in records if r["class"] == cls]
        if not found:
            continue
        summary += ["", f"### {cls}", ""]
        for record in found[:5]:
            old, new = record.get("old"), record.get("new")
            if record["kind"] == "row":
                row = old or new
                summary.append(
                    f"- `{record['register']}` / `{row['table'] or '(implicit)'}` / `{row['variable']}`; "
                    f"period {old['period'] if old else None} → {new['period'] if new else None}; "
                    f"spelling {old['spelling'] if old else None!r} → {new['spelling'] if new else None!r}. "
                    f"{record['reason']}"
                )
            else:
                summary.append(
                    f"- `{record['register']}`: {record['kind']}. {record['reason']}"
                )
            if record.get("review"):
                summary.append(f"  {record['review']}")
    return summary


def orders(args) -> int:
    with (
        closing(connect_ro(args.old_db)) as old_db,
        closing(connect_ro(args.new_db)) as new_db,
    ):
        candidates = [
            pid
            for (pid,) in new_db.execute("SELECT pid FROM register ORDER BY pid")
            if not args.provider or pid.split("/")[0] in args.provider
        ]
        if not 0 < args.n <= len(candidates):
            raise ValueError(f"--n must be between 1 and {len(candidates)}")
        sampled = random.Random(args.seed).sample(candidates, args.n)
        providers = {pid.split("/")[0] for pid in sampled}
        catalog_views = [old_views(old_db, providers), new_views(new_db, providers)]
        old_registers = {
            pid
            for (pid,) in old_db.execute(
                "SELECT p.slug || '/' || r.slug FROM register r JOIN provider p USING (provider_id)"
            )
        }
        contexts = [
            request_json(url.rstrip("/") + "/api/context")[1]
            for url in (args.old_url, args.new_url)
        ]
        args.out.mkdir(parents=True, exist_ok=False)
        summary = ["# Orders comparison", "", f"Seed: {args.seed}; n: {args.n}.", ""]
        for side, context in zip(("old", "new"), contexts, strict=True):
            summary.append(
                f"- {side}: reg-meta {context['data']['reg_meta_version']}; "
                f"catalog {context['data'].get('catalog_version', 'not exposed')}; "
                f"meta.generation `{context['meta']['generation']}`"
            )
        summary.append(
            f"- old curation source: {args.old_curation or 'not supplied; header corrections are unexplained'}"
        )
        summary += [
            "",
            "Periods are half-open ISO days; null endpoints are open/timeless.",
            "Rows are distinct (table, variable, period, spelling); the equal column counts original exact matches, differing counts aligned slices or remaining unmatched rows.",
            "Physical columns align by table, case-folded spelling and shared day endpoints across variable IDs; source responses retain original partitions.",
            "A blocked order has no partial entries; refusal records are separate from row differences.",
            "Dated requests intersect catalog hulls with the project grammar's 1900–2099 range; adjustments are listed below.",
            "",
            "| register | rows old | rows new | equal | differing | blocked old | blocked new | result |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
        adjustments, omissions, mappings, classified_records = [], [], [], []
        with (args.out / "diff.jsonl").open("w") as output:

            def emit(record):
                classified_records.append(record)
                output.write(
                    json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                )

            for reg in sampled:
                if reg not in old_registers:
                    emit(
                        {
                            "register": reg,
                            "kind": "register_only",
                            "old": None,
                            "new": reg,
                            "class": "missing in old",
                            "owner": "defect",
                            "reason": "The sampled register has no old counterpart.",
                        }
                    )
                    summary.append(f"| {reg} | 0 | 0 | 0 | 0 | 0 | 0 | only in new |")
                    continue
                projects = []
                for side, build, db, context in zip(
                    ("old", "new"),
                    (
                        partial(
                            old_project,
                            url=args.old_url,
                            evidence=args.out / "metadata",
                        ),
                        new_project,
                    ),
                    (old_db, new_db),
                    contexts,
                    strict=True,
                ):
                    bounds, omitted = [], []
                    projects.append(
                        build(
                            db,
                            reg,
                            context["data"]["reg_meta_version"],
                            bounds,
                            omitted,
                        )
                    )
                    omissions.extend(
                        {"side": side, "table": table} for table in omitted
                    )
                    adjustments.extend({"side": side, **bound} for bound in bounds)
                tables = table_names(reg, *(view[reg] for view in catalog_views))
                results, blockers, row_sources, bodies = [], [], [], []
                for i, (side, url) in enumerate(
                    zip(("old", "new"), (args.old_url, args.new_url), strict=True)
                ):
                    write_json(
                        args.out / "projects" / reg / f"{side}.json", projects[i]
                    )
                    if projects[i]["sources"]:
                        status, body = request_json(
                            url.rstrip("/") + "/api/project/order",
                            projects[i],
                            args.out / "responses" / reg / f"{side}.json",
                        )
                    else:
                        status, body = None, {}

                    results.append(
                        {
                            "status": status,
                            "error_code": body.get("error", {}).get("code"),
                        }
                    )
                    blockers.append(blocking_issues(body, projects[i]))
                    row_sources.append(order_rows(body, tables[i], old=i == 0))
                    bodies.append(body)
                rows = [set(side) for side in row_sources]
                placements = placement_explanations(
                    reg, [view[reg] for view in catalog_views], tables
                )

                differences = list(row_differences(*rows, new_db, tables[1]))
                mapped = sorted(
                    {
                        (old[0], old[1], new[1])
                        for old, new, alignment in differences
                        if old
                        and new
                        and old[1] != new[1]
                        and alignment
                        and alignment["supported"]
                    }
                )
                mappings.extend(
                    {"register": reg, "table": t, "old": o, "new": n}
                    for t, o, n in mapped
                )
                classified = partial(
                    classify_difference,
                    rows=rows,
                    bodies=bodies,
                    tables=tables,
                    dbs=(old_db, new_db),
                    curation=curation_evidence(
                        old_db, reg, tables[0], args.old_curation
                    ),
                    unplaced=unplaced_columns(new_db, reg, tables[1]),
                    placements=placements,
                    correspondences=mapped,
                )

                evidence = {
                    "register": reg,
                    "results": dict(zip(("old", "new"), results, strict=True)),
                    "blocking_issues": dict(zip(("old", "new"), blockers, strict=True)),
                }
                if any(r["error_code"] for r in results):
                    emit(classified(evidence | {"kind": "refusal"}))
                for old, new, alignment in differences:
                    table = (old or new)[0]
                    source_names = {}
                    for i, side in enumerate(("old", "new")):
                        # A refused side has no entries; locate its corresponding
                        # source by table. Successful successions name their source.
                        aligned_row = (old, new)[i]
                        sources = row_sources[i].get(
                            aligned_row,
                            {
                                s["name"]
                                for s in projects[i]["sources"]
                                if tables[i][
                                    s["register_variant" if i == 0 else "table"]
                                ]
                                == table
                            },
                        )
                        if alignment is not None and aligned_row is not None:
                            sources = set().union(
                                *(
                                    names
                                    for r, names in row_sources[i].items()
                                    if r[:2] == aligned_row[:2]
                                    and r[4] == aligned_row[4]
                                    and row_days(r)[0] <= row_days(aligned_row)[0]
                                    and row_days(r)[1] >= row_days(aligned_row)[1]
                                )
                            )
                        source_names[side] = sorted(sources)
                    emit(
                        classified(
                            evidence
                            | {
                                "kind": "row",
                                "old": row_value(old),
                                "new": row_value(new),
                                "sources": source_names,
                                **(
                                    {"alignment": alignment}
                                    if alignment is not None
                                    else {}
                                ),
                            },
                            old,
                            new,
                        )
                    )
                blocked_counts = [
                    len(p["sources"]) if None in b else len(b)
                    for p, b in zip(projects, blockers, strict=True)
                ]
                summary.append(
                    f"| {reg} | {len(rows[0])} | {len(rows[1])} | {len(rows[0] & rows[1])} | "
                    f"{len(differences)} | {blocked_counts[0]} | {blocked_counts[1]} | "
                    + "; ".join(
                        f"{side}: {r['status']} {r['error_code'] or 'ok'}"
                        if r["status"] is not None
                        else f"{side}: no orderable columns"
                        for side, r in zip(("old", "new"), results, strict=True)
                    )
                    + " |"
                )
        summary += ["", "## Omitted columnless tables", ""]
        summary.extend(f"- {o['side']} `{o['table']}`" for o in omissions)
        if not omissions:
            summary.append("None.")
        summary += classification_summary(classified_records, mappings)
        write_json(args.out / "variable-map.json", mappings)
        summary += ["", "## Request bound adjustments", ""]
        summary.extend(
            f"- {a['side']} `{a['source']}`: {a['catalog']} → {a['requested']}"
            for a in adjustments
        )
        if not adjustments:
            summary.append("None.")
        write_json(args.out / "request-bounds.json", adjustments)
        (args.out / "summary.md").write_text("\n".join(summary) + "\n")
    print((args.out / "summary.md").read_text(), end="")
    return 0


HOLDINGS_CLASSES = {
    "equal": "",
    "census drift": "input",
    "intended lookup/excluded": "design",
    "defect": "compiler",
    "target changed": "review",
    "placement changed": "review",
    "intended clipping": "design",
    "coverage extended": "review",
    "missing target": "#2072",
    "held": "#2072",
    "curation carryover": "curation",
    "resolved": "curation",
    "withheld carried over": "#2072",
    "owner ambiguity": "#2072",
    "old unknown": "",
    "key census changed": "review",
    "disposition changed": "review",
}


def holdings_span(lo, hi):
    """Old inclusive ISO bounds, including the open upper sentinel."""
    start, end = period_days({"start": lo, "end": hi})
    return start, end + 1 if hi else end


def holdings_union(spans):
    merged = []
    for lo, hi in sorted(set(spans)):
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
        else:
            merged.append((lo, hi))
    return merged


def holdings_side(disposition, locator, **fields):
    return {
        "disposition": disposition,
        "locator": locator,
        "mappings": [],
        "keys": [],
        "key_mappings": [],
        "notices": [],
        **fields,
    }


def old_holdings(db):
    tables = {}
    scopes = {
        "intervals": "dated",
        "year_independent": "timeless",
        "unknown": "unknown",
    }
    for tid, name, scope, locator in db.execute(
        "SELECT table_id, physical_id, scope, source_ref FROM holding_table"
    ):
        tables[tid] = {"name": name, **holdings_side(scopes[scope], locator)}
    periods = defaultdict(list)
    for tid, lo, hi in db.execute("SELECT table_id, lo, hi FROM holding_period"):
        periods[tid].append(holdings_span(lo, hi))
    for tid, table in tables.items():
        if table["disposition"] == "timeless":
            periods[tid] = [(TIMELESS_START, TIMELESS_END)]
    columns, ids = {}, {}
    for cid, tid, name, reason in db.execute(
        "SELECT column_id, table_id, name, unmapped_reason FROM holding_column"
    ):
        table = tables[tid]
        side = holdings_side(table["disposition"], table["locator"], reason=reason)
        side["state"] = "old withheld" if reason else "unmapped"
        columns[table["name"], name] = ids[cid] = side
    target = (
        " JOIN variable v USING (variable_id) JOIN register r ON r.register_id = v.register_id"
        " JOIN provider p ON p.provider_id = r.provider_id"
    )
    for cid, tid, pid, logical, wlo, whi, mlo, mhi in db.execute(
        "SELECT m.column_id, c.table_id, p.slug || '/' || r.slug || '/' || v.slug,"
        " vp.slug || '/' || vr.slug || '/' || rv.slug,"
        " m.window_lo, m.window_hi, m.member_lo, m.member_hi FROM holding_mapping m"
        " JOIN holding_column c USING (column_id)"
        " JOIN register_variant rv ON rv.register_variant_id = m.variant_id"
        " JOIN register vr ON vr.register_id = rv.register_id"
        " JOIN provider vp ON vp.provider_id = vr.provider_id" + target
    ):
        spans = periods[tid]
        if wlo is not None or mlo is not None:
            start, end = holdings_span(wlo or mlo, whi or mhi)
            spans = [
                (max(start, lo), min(end, hi))
                for lo, hi in spans
                if start < hi and lo < end
            ]
        ids[cid]["mappings"].extend((pid, logical, lo, hi) for lo, hi in spans)
        ids[cid]["state"] = "mapped"
    for cid, tid, pid, logical in db.execute(
        "SELECT k.column_id, c.table_id, p.slug || '/' || r.slug || '/' || v.slug,"
        " vp.slug || '/' || vr.slug || '/' || rv.slug FROM holding_key k"
        " JOIN holding_column c USING (column_id)"
        " JOIN register_variant rv ON rv.register_variant_id = k.variant_id"
        " JOIN register vr ON vr.register_id = rv.register_id"
        " JOIN provider vp ON vp.provider_id = vr.provider_id" + target
    ):
        ids[cid]["keys"].append(pid)
        ids[cid]["key_mappings"].extend(
            (pid, logical, lo, hi) for lo, hi in periods[tid]
        )
        if ids[cid]["state"] == "unmapped":
            ids[cid]["state"] = "key only"
    return {t["name"]: t for t in tables.values()}, columns


def new_holdings(db):
    tables, columns, ids = {}, {}, {}
    for tid, name, disposition, locator in db.execute(
        "SELECT id, physical_name, disposition, locator FROM holding_table"
    ):
        tables[tid] = {
            "name": name,
            "routes": [],
            **holdings_side(disposition, locator),
        }
    for tid, pid in db.execute("SELECT holding_id, table_pid FROM holding_route"):
        tables[tid]["routes"].append(pid)
    for cid, tid, name, disposition, reason, follow_up, locator in db.execute(
        "SELECT id, holding_id, name, disposition, reason, follow_up, locator FROM holding_column"
    ):
        table = tables[tid]
        side = holdings_side(
            table["disposition"],
            locator,
            state=disposition,
            reason=reason,
            follow_up=follow_up,
            routes=sorted(table["routes"]),
            column_disposition=disposition,
        )
        columns[table["name"], name] = ids[cid] = side
    for cid, pid, logical, lo, hi, role, carried, locator in db.execute(
        "SELECT column_id, variable_pid, table_pid, start, end, role, carried, locator FROM holding_mapping"
    ):
        side = ids[cid]
        if role == "row_key" or carried:
            side["keys"].append(pid)
            side["key_mappings"].append((pid, logical, lo, hi))
        else:
            side["mappings"].append((pid, logical, lo, hi))
        side.setdefault("mapping_locators", []).append(locator)
    table_notices = defaultdict(list)
    for (
        tid,
        cid,
        code,
        locator,
        message,
        lo,
        hi,
        requested,
        documented,
        admitted,
    ) in db.execute(
        "SELECT holding_id, column_id, code, locator, message, start, end, requested, documented, admitted FROM holding_notice ORDER BY id"
    ):
        notice = {
            "code": code,
            "column_scoped": cid is not None,
            "locator": locator,
            "message": message,
            "period": holdings_iso_span(lo, hi) if lo is not None else None,
            **{
                key: [holdings_iso_span(a, b) for a, b in json.loads(value or "[]")]
                for key, value in (
                    ("requested", requested),
                    ("documented", documented),
                    ("admitted", admitted),
                )
            },
        }
        (ids[cid]["notices"] if cid is not None else table_notices[tid]).append(notice)
    for cid, tid in db.execute("SELECT id, holding_id FROM holding_column"):
        side = ids[cid]
        side["notices"].extend(table_notices[tid])
        if side["mappings"]:
            side["state"] = "mapped"
        elif side["keys"]:
            side["state"] = "key only"
    return {t["name"]: t for t in tables.values()}, columns


def holdings_targets(side):
    return sorted({pid for pid, _, _, _ in side["mappings"]})


def holdings_reach(side, target=None, field="mappings"):
    return holdings_union(
        (lo, hi) for pid, _, lo, hi in side[field] if target is None or pid == target
    )


def holdings_iso_span(lo, hi):
    return {
        "start": None if lo == TIMELESS_START else iso_day(lo),
        "end": None if hi == TIMELESS_END else iso_day(hi),
    }


def holdings_public(side):
    if side is None:
        return None

    return {
        **{
            k: v
            for k, v in side.items()
            if k not in {"mappings", "keys", "key_mappings"}
        },
        "targets": holdings_targets(side),
        "reach": [
            holdings_iso_span(lo, hi)
            for lo, hi in holdings_union(
                (lo, hi) for _, _, lo, hi in side["mappings"] + side["key_mappings"]
            )
        ],
        "orderable_reach": [
            holdings_iso_span(lo, hi) for lo, hi in holdings_reach(side)
        ],
        "key_reach": [
            holdings_iso_span(lo, hi)
            for lo, hi in holdings_union(
                (lo, hi) for _, _, lo, hi in side["key_mappings"]
            )
        ],
        "mappings": [
            {"target": pid, "logical_table": logical, **holdings_iso_span(lo, hi)}
            for pid, logical, lo, hi in sorted(set(side["mappings"]))
        ],
        "key_mappings": [
            {"target": pid, "logical_table": logical, **holdings_iso_span(lo, hi)}
            for pid, logical, lo, hi in sorted(set(side["key_mappings"]))
        ],
        "keys": sorted(set(side["keys"])),
        "notice_codes": sorted({n["code"] for n in side["notices"]}),
    }


def holdings_candidate_status(old, new, base):
    """Target presence and overlapping routed coverage, without slug reconciliation."""
    status = {}
    for pid in holdings_targets(old):
        exists = (
            base.execute("SELECT 1 FROM variable WHERE pid = ?", (pid,)).fetchone()
            is not None
        )
        routed = False
        for table, lo, hi in base.execute(
            "SELECT t.pid, s.start, s.end FROM segment s"
            " JOIN variable v ON v.id = s.variable_id JOIN tbl t ON t.id = s.table_id"
            " WHERE v.pid = ?",
            (pid,),
        ):
            if table in new["routes"] and any(
                lo < end and start < hi for start, end in holdings_reach(old, pid)
            ):
                routed = True
                break
        status[pid] = (
            "routed overlap"
            if routed
            else "present but unrouted"
            if exists
            else "absent"
        )
    return status


def holdings_key_equal(old, new):
    if set(old["keys"]) != set(new["keys"]):
        return False
    return all(
        not uncovered(span, holdings_reach(other, pid, "key_mappings"))
        for side, other in ((old, new), (new, old))
        for pid in side["keys"]
        for span in holdings_reach(side, pid, "key_mappings")
    )


def holdings_placement_changed(old, new):
    # IDs belong to separate catalogs. Compare multiplicity, never coordinate identity.
    def counts(side):
        tables = defaultdict(set)
        for pid, logical, _, _ in side["mappings"] + side["key_mappings"]:
            tables[pid].add(logical)
        return {pid: len(found) for pid, found in tables.items()}

    o, n = counts(old), counts(new)
    return any(o[pid] != n[pid] for pid in o.keys() & n.keys())


def holdings_clip_loss(new):
    removed = []
    for notice in new["notices"]:
        if notice["code"] != "period_clipped" or not notice["column_scoped"]:
            continue
        # The schema has no target on a notice. Requested minus admitted proves
        # physical loss across all of this column's targets; table notices cannot.
        admitted = [period_days(span) for span in notice["admitted"]]
        removed.extend(
            piece
            for span in notice["requested"]
            for piece in uncovered(period_days(span), admitted)
        )
        if notice["period"] is not None:
            removed.append(period_days(notice["period"]))
    return holdings_union(removed)


def classify_holdings(record, old, new, old_tables, new_tables, pins_equal):
    def result(cls, reason, action="none", codes=()):
        locators = [s["locator"] for s in (old, new) if s]
        if old:
            locators += [old_tables[record["table"]]["locator"]]
        if new:
            locators += [n["locator"] for n in new["notices"] if n["code"] in codes]
            locators += new.get("mapping_locators", [])
        return record | {
            "class": cls,
            "owner": HOLDINGS_CLASSES[cls],
            "reason": reason,
            "locator": sorted(set(locators)),
            "next_action": action,
        }

    work = "Add a worklist row in the private steward repo (#2072)."
    defect = "File against reg-build holdings with the retained locators."
    codes = {n["code"] for n in new["notices"]} if new else set()
    if old is None or new is None:
        only_table = (
            record["table"] not in old_tables or record["table"] not in new_tables
        )
        if old is None and only_table and new["disposition"] in {"lookup", "excluded"}:
            return result(
                "intended lookup/excluded",
                "Old compiler did not emit lookup/excluded tables.",
            )
        reason = ("Table" if only_table else "Column") + (
            " only in new." if old is None else " only in old."
        )
        if pins_equal is False:
            return result(
                "census drift",
                reason + " Census pins differ.",
                "Reconcile the census inputs.",
            )
        return result("defect", reason + " Census pins agree.", defect)
    if record["kind"] == "table":
        if old["disposition"] != new["disposition"]:
            return result(
                "disposition changed",
                "Empty-table disposition differs.",
                "Review table disposition.",
            )
        return result("equal", "Same empty-table disposition.")
    if old["disposition"] == "unknown":
        return result(
            "old unknown",
            f"Old scope unknown; new disposition {new['disposition']}.",
            "Review the new disposition.",
        )
    if codes & {
        "ambiguous_delivered_owners",
        "ambiguous_catalog_segment",
    }:
        return result(
            "owner ambiguity", "New ownership has blocked slices.", work, codes
        )
    if new["column_disposition"] == "hold" or codes & {"mapping_held", "row_key_held"}:
        return result(
            "withheld carried over" if old["state"] == "old withheld" else "held",
            "New hold retains reason and follow-up, including partially held mappings.",
            work,
            codes,
        )
    if old["mappings"] and new["mappings"]:
        if holdings_targets(old) != holdings_targets(new):
            return result(
                "target changed",
                "Mapped target pids differ.",
                "Check steward ledgers and slug reconciliation before calling a defect.",
            )
        if holdings_placement_changed(old, new):
            return result(
                "placement changed",
                "Logical table multiplicity differs per target.",
                "Review retained coordinates without joining old and new IDs.",
            )
        if not holdings_key_equal(old, new):
            return result(
                "key census changed",
                "Key targets or per-target reach differ.",
                "Review key requirements separately from ordered columns.",
            )
        removed = []
        added = False
        for pid in holdings_targets(old):
            o, n = holdings_reach(old, pid), holdings_reach(new, pid)
            removed.extend(piece for span in o for piece in uncovered(span, n))
            added |= any(uncovered(span, o) for span in n)
        if removed:
            loss = holdings_clip_loss(new)
            if all(not uncovered(span, loss) for span in removed):
                return result(
                    "intended clipping",
                    "Column clipping notices explain every removed target/day slice."
                    + (" Other slices extend coverage." if added else ""),
                    codes={"period_clipped"},
                )
            return result(
                "defect",
                "New reach is narrower without column clipping evidence covering every lost slice.",
                defect,
            )
        if added:
            return result(
                "coverage extended",
                "D coverage extends the old reach.",
                "List for maintainer review.",
            )
        if old["disposition"] != new["disposition"]:
            return result(
                "disposition changed",
                "Table disposition differs.",
                "Review table disposition.",
            )
        return result("equal", "Same target pids and per-target reach.")
    if old["mappings"] and "mapping_missing" in codes:
        candidates = record["missing_target_status"]
        if "routed overlap" in candidates.values():
            return result(
                "defect",
                "Missing mapping despite an old target with overlapping routed base coverage.",
                defect,
                {"mapping_missing"},
            )
        return result(
            "missing target",
            "Old target is absent or has no overlapping routed base segment.",
            work,
            {"mapping_missing"},
        )
    if old["mappings"] and new["column_disposition"] == "unmap":
        return result(
            "curation carryover",
            "New explicit unmap; reason retained for verification.",
            "Verify the authored unmap reason against old curation.",
        )
    if old["state"] == "old withheld":
        if new["mappings"]:
            return result("resolved", "Old withheld column now maps.")
        return result(
            "withheld carried over",
            "Old hold/unmap were indistinguishable; withholding remains unresolved.",
            work,
            codes,
        )
    if old["keys"] or new["keys"]:
        if not old["mappings"] and not new["mappings"] and holdings_key_equal(old, new):
            if holdings_placement_changed(old, new):
                return result(
                    "placement changed",
                    "Logical key table multiplicity differs per target.",
                    "Review retained coordinates without joining old and new IDs.",
                )
            if old["disposition"] != new["disposition"]:
                return result(
                    "disposition changed",
                    "Table disposition differs.",
                    "Review table disposition.",
                )
            return result("equal", "Same key-only target census and per-target reach.")
        return result(
            "key census changed",
            "Orderable/key-only census differs.",
            "Review key requirements separately from ordered columns.",
        )
    if old["state"] == "unmapped" and new["mappings"]:
        return result(
            "coverage extended",
            "Previously unmapped column now maps.",
            "List for maintainer review.",
        )
    if "mapping_missing" in codes:
        return result(
            "missing target",
            "New column has no accepted target.",
            work,
            {"mapping_missing"},
        )
    if not old["mappings"] and not new["mappings"]:
        if new["column_disposition"] == "unmap":
            return result(
                "curation carryover",
                "New explicit unmap reason retained.",
                "Verify the authored unmap reason.",
            )
        if old["disposition"] != new["disposition"]:
            return result(
                "disposition changed",
                "Table disposition differs.",
                "Review table disposition.",
            )
        return result("equal", "Both columns remain unmapped without withholding.")
    return result(
        "defect",
        "Old orderability is lost without a hold, unmap or missing-target notice.",
        defect,
    )


def holdings(args) -> int:
    if args.out.exists():
        raise ValueError(
            "--out already exists; choose a fresh private report directory"
        )
    with (
        closing(connect_ro(args.old_db)) as old_db,
        closing(connect_ro(args.new_base)) as base,
        closing(connect_ro(args.new_holdings)) as new_db,
    ):
        old_meta = dict(old_db.execute("SELECT key, value FROM import_manifest"))
        new_meta = dict(new_db.execute("SELECT key, value FROM meta"))
        base_meta = dict(base.execute("SELECT key, value FROM meta"))
        with args.new_base.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        if new_meta["catalog_sha256"] != digest:
            raise ValueError(
                "holdings meta.catalog_sha256 does not match --new-base sha256"
            )
        pins = json.loads(new_meta["input_pins"])
        if args.new_census_path not in pins:
            raise ValueError("--new-census-path must name an existing input_pins key")
        new_pin = pins[args.new_census_path]
        old_pin = args.old_census_sha256
        if not re.fullmatch("[0-9a-f]{64}", old_pin):
            raise ValueError("old census sha256 must be 64 lowercase hex digits")
        pins_equal = old_pin == new_pin
        old_tables, old_columns = old_holdings(old_db)
        new_tables, new_columns = new_holdings(new_db)
        shared = old_columns.keys() & new_columns.keys()
        old_only, new_only = old_columns.keys() - shared, new_columns.keys() - shared
        near = len(
            {(fold(t), fold(c)) for t, c in old_only}
            & {(fold(t), fold(c)) for t, c in new_only}
        )
        records = []
        missing_counts = Counter()
        for table, column in sorted(old_columns.keys() | new_columns.keys()):
            old, new = (
                old_columns.get((table, column)),
                new_columns.get((table, column)),
            )
            record = {
                "kind": "column",
                "table": table,
                "column": column,
                "old": holdings_public(old),
                "new": holdings_public(new),
            }
            if new and any(n["code"] == "mapping_missing" for n in new["notices"]):
                status = (
                    holdings_candidate_status(old, new, base)
                    if old and old["mappings"]
                    else {}
                )
                record["missing_target_status"] = status
                category = (
                    "no old mapped target"
                    if not status
                    else "target present, routed overlap"
                    if "routed overlap" in status.values()
                    else "target present, no routed overlap"
                    if "present but unrouted" in status.values()
                    else "target absent"
                )
                missing_counts[category] += 1
            records.append(
                classify_holdings(record, old, new, old_tables, new_tables, pins_equal)
            )
        old_nonempty = {t for t, _ in old_columns}
        new_nonempty = {t for t, _ in new_columns}
        empty_tables = (old_tables.keys() - old_nonempty) | (
            new_tables.keys() - new_nonempty
        )
        for table in sorted(empty_tables):
            old, new = old_tables.get(table), new_tables.get(table)
            record = {
                "kind": "table",
                "table": table,
                "column": None,
                "old": holdings_public(old),
                "new": holdings_public(new),
            }
            records.append(
                classify_holdings(record, old, new, old_tables, new_tables, pins_equal)
            )
        totals = Counter(r["class"] for r in records)
        total_line = f"Totals: old {len(old_tables)} tables / {len(old_columns)} columns; new {len(new_tables)} tables / {len(new_columns)} columns; shared {len(shared)} columns; union {len(old_columns.keys() | new_columns.keys())} columns; empty-table records {len(empty_tables)}; folded near-misses {near}."
        summary = [
            "# Holdings census comparison",
            "",
            f"Old release: reg_meta/v0.64.0; schema: {old_meta.get('schema_version')}; builder: {old_meta.get('builder_commit')}; generation: {old_meta.get('generation_id')}.",
            f"New compiler digest: {new_meta.get('compiler_digest')}; base schema: {base_meta.get('schema_version')}; generation: {base_meta.get('generation_id')}; holdings schema: {new_meta.get('schema_version')}; generation: {new_meta.get('generation_id')}.",
            f"Old census sha256 (supplied from pinned scope): {old_pin}.",
            f"Old input-manifest sha256 (not a census digest): {old_meta.get('holdings_manifest_sha256')}.",
            f"New census sha256: {new_pin}; equal: {pins_equal}.",
            f"catalog_sha256 check: passed ({digest}).",
            "",
            total_line,
            "",
            "Exact physical (table, column) join; folded near-misses are diagnostic only. Old inclusive dates become half-open days by adding one to the end. Null ISO endpoints denote open/timeless reach.",
            "Old hold/unmap reasons are indistinguishable and treated as old withheld. Every census column is retained, including keys and unknown/lookup/excluded dispositions.",
            "Holds and blocked ownership take precedence over clipping, including partially accepted columns. Old-withheld plus new-hold is withheld carried over. Clipping must explain every lost slice using column-scoped notice evidence. Target and key comparisons use per-target reach. Logical table multiplicity is compared without joining old and new IDs.",
            "Empty tables have separate table records; column denominators exclude those records.",
            "",
            "## Artifact totals",
            "",
        ]
        for key in (
            "census_tables",
            "census_columns",
            "mapped_columns",
            "unmatched_columns",
            "storage_extra_columns",
        ):
            summary.append(f"- New meta.{key}: {new_meta.get(key)}")
        summary.append(
            f"- Old accounting counts: {old_meta.get('holdings_accounting_counts')}"
        )
        summary += [
            "",
            "## Table dispositions",
            "",
            "| old | new | tables |",
            "|---|---|---:|",
        ]
        dispositions = Counter(
            (
                old_tables[t]["disposition"] if t in old_tables else "absent",
                new_tables[t]["disposition"] if t in new_tables else "absent",
            )
            for t in old_tables.keys() | new_tables.keys()
        )
        for (old, new), count in sorted(dispositions.items()):
            summary.append(f"| {old} | {new} | {count} |")
        summary += [
            "",
            "## Classes",
            "",
            "| class | columns | tables | owner |",
            "|---|---:|---:|---|",
        ]
        for cls, owner in HOLDINGS_CLASSES.items():
            found = [r for r in records if r["class"] == cls]
            summary.append(
                f"| {cls} | {sum(r['kind'] == 'column' for r in found)} | {len({r['table'] for r in found})} | {owner} |"
            )
        summary += [
            "",
            "## Key census",
            "",
            "| side | key columns | key target pairs | orderable columns | mapping rows |",
            "|---|---:|---:|---:|---:|",
        ]
        for side, columns in (("old", old_columns), ("new", new_columns)):
            mapping_count = (
                old_db.execute("SELECT count(*) FROM holding_mapping").fetchone()[0]
                if side == "old"
                else new_db.execute(
                    "SELECT count(*) FROM holding_mapping WHERE role != 'row_key' AND carried = 0"
                ).fetchone()[0]
            )
            summary.append(
                f"| {side} | {sum(bool(c['keys']) for c in columns.values())} | {sum(len(set(c['keys'])) for c in columns.values())} | {sum(bool(c['mappings']) for c in columns.values())} | {mapping_count} |"
            )
        summary += [
            "",
            "## New disposition × notice",
            "",
            "Columns count once per code; no-notice columns remain visible. Column dispositions distinguish auto-map candidates from explicit hold/unmap.",
            "",
            "| table disposition | column disposition | notice | columns |",
            "|---|---|---|---:|",
        ]
        breakdown = Counter(
            (c["disposition"], c["column_disposition"], code)
            for c in new_columns.values()
            for code in ({n["code"] for n in c["notices"]} or {"none"})
        )
        for (disposition, state, code), count in sorted(breakdown.items()):
            summary.append(f"| {disposition} | {state} | {code} | {count} |")
        summary += [
            "",
            "## mapping_missing target breakdown",
            "",
            "| old target status | columns |",
            "|---|---:|",
        ]
        summary.extend(
            f"| {category} | {count} |"
            for category, count in sorted(missing_counts.items())
        )
        for cls in HOLDINGS_CLASSES:
            found = [r for r in records if r["class"] == cls]
            if found:
                summary += ["", f"## {cls}", ""]
                for r in found[:5]:
                    summary.append(
                        f"- `{r['table']}` / `{r['column']}`; locators: {r['locator']}; {r['reason']} Next: {r['next_action']}"
                    )
        args.out.mkdir(parents=True, exist_ok=False)
        for filename, rows in (
            ("census.jsonl", records),
            ("differences.jsonl", (r for r in records if r["class"] != "equal")),
        ):
            with (args.out / filename).open("w", encoding="utf-8") as output:
                for record in rows:
                    output.write(
                        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                    )
        (args.out / "summary.md").write_text(
            "\n".join(summary) + "\n", encoding="utf-8"
        )
    print("| class | columns | empty-table records |\n|---|---:|---:|")
    for cls in HOLDINGS_CLASSES:
        table_records = sum(r["kind"] == "table" and r["class"] == cls for r in records)
        print(f"| {cls} | {totals[cls] - table_records} | {table_records} |")
    print(total_line)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    reg = commands.add_parser(
        "registers", help="compare catalog metadata (Markdown on stdout)"
    )
    reg.add_argument("old_db")
    reg.add_argument("new_db")
    reg.add_argument("report_json")
    reg.add_argument("providers", nargs="*")
    reg.set_defaults(run=registers)
    order = commands.add_parser("orders", help="compare sampled full-register orders")
    for side in ("old", "new"):
        order.add_argument(f"--{side}-url", required=True)
        order.add_argument(f"--{side}-db", required=True, type=Path)
    order.add_argument(
        "--old-curation",
        type=Path,
        help="released old curation root; required as explicit evidence for header corrections",
    )
    order.add_argument("--seed", type=int, default=0)
    order.add_argument("--n", type=int, default=20)
    order.add_argument("--provider", nargs="+", default=[])
    order.add_argument(
        "--out", type=Path, required=True, help="new directory for evidence"
    )
    order.set_defaults(run=orders)
    holding = commands.add_parser(
        "holdings", help="compare the private physical holdings census"
    )
    for flag in ("old-db", "new-base", "new-holdings", "out"):
        holding.add_argument(f"--{flag}", type=Path, required=True)
    holding.add_argument(
        "--old-census-sha256",
        required=True,
        help="raw census digest from the pinned old scope (0.64.0 stores only the input-manifest digest)",
    )
    holding.add_argument(
        "--new-census-path",
        required=True,
        help="exact census path key in holdings meta.input_pins",
    )
    holding.set_defaults(run=holdings)
    args = parser.parse_args()
    try:
        return args.run(args)
    except (OSError, ValueError, sqlite3.Error) as error:
        parser.exit(1, f"{parser.prog}: {error}\n")


if __name__ == "__main__":
    sys.exit(main())
