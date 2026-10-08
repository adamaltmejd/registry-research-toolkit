"""``schema`` and ``diff`` against the CLI arm's baseline results, reused by case id:

- ``get-schema-summary/<fqid>`` and ``get-schema-year/<fqid>``: ``get schema``
  (whole history, and the case's sampled ``--years``) against every page of
  ``schema``, as one row per column. ``--summary`` only changes the table rendering,
  so its JSON is the full schema.
- ``get-diff/<fqid>``: ``get diff`` against ``diff`` over the case's two years.
- ``get-datacolumns/<stratum>/<id>``: ``get datacolumns`` against the columns of the
  matched variables' ``schema`` rows, per variant. Holdings lists the held
  representation's spelling and ``schema`` the delivered one; one is a case twin of
  the other, so holdings compares them folded.

Rows compare as sorted lists of the fields both arms carry: storage ids are not part of
the API, and the CLI emits the window-only fields (operational definition, source text,
definition, unit) only on expanded rows. A refusal compares as ``error``. The sampled
years and the datacolumns argv come from ``cases.py``'s generator, run again with the
configured seed (the CLI results do not carry their argv).
"""

from __future__ import annotations

import json
import tomllib
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

from conformance.differential import cases as generator
from conformance.differential.served.common import get

if TYPE_CHECKING:
    import sqlite3

SEED = tomllib.loads((Path(__file__).parents[1] / "config.toml").read_text())["seed"]
# Requests in flight per server pair.
PARALLEL = 16
LIMIT = 200
COMMANDS = ("get-schema-summary", "get-schema-year", "get-diff", "get-datacolumns")


def _argv(originals: Path, catalog: str, scopes: list[str]) -> dict[str, list[str]]:
    """The CLI argv of this family's cases, by case id, as `cases.py` generates them."""
    builder = generator._Builder(catalog, originals)
    named = [s for s in scopes if s is not None]
    with closing(generator._connect(originals)) as conn:
        generator._register_cases(builder, conn, named, SEED)
        strata = generator._variable_strata(conn, catalog, "holdings" in named, SEED)
    generator._variable_cases(builder, strata, named)
    return {
        case.id: case.argv
        for case in builder.cases
        if case.id.split("/")[2] in COMMANDS
    }


def _flag(argv: list[str], name: str) -> str:
    return argv[argv.index(name) + 1]


def _pages(cand, path: str, params: dict) -> list[dict] | None:
    """Every row of a paged answer, or None for a refusal."""
    rows, cursor = [], None
    while True:
        answer = get(cand, path, params | {"limit": LIMIT, "cursor": cursor})
        if answer["status"] != 200:
            return None
        rows += answer["body"]["data"]["items"]
        cursor = answer["body"]["data"]["next_cursor"]
        if cursor is None:
            return rows


def _row(variant: str | None, valid_from, valid_to, column: dict) -> list:
    return [
        variant,
        valid_from,
        valid_to,
        column["fqid"],
        column["var_id"],
        column["variable_name"],
        column["source"] or None,
        column["column"] or None,
        column["data_type"],
        column["data_length"],
        column["value_set_version_label"],
        column["group"],
        column["group_label"],
    ]


def _schema_baseline(data: dict, register: str) -> list:
    rows = []
    for variant in data["variants"]:
        for version in variant["versions"]:
            for column in version["columns"]:
                group = column["concept_group"]
                mapped = {
                    **column,
                    "column": column["aliases"],
                    "group": group and f"group/{register}/{group}",
                    "group_label": column["concept_group_label"],
                }
                rows.append(
                    _row(
                        variant["variant"],
                        version["valid_from"],
                        version["valid_to"],
                        mapped,
                    )
                )
    return sorted(rows, key=json.dumps)


def _schema_candidate(rows: list[dict]) -> list:
    return sorted(
        (_row(r["variant"], r["valid_from"], r["valid_to"], r) for r in rows),
        key=json.dumps,
    )


def _diff_column(column: dict, spelling: str) -> list:
    return [
        column["variable_name"],
        column["var_id"],
        column["data_type"],
        column["data_length"],
        column[spelling],
    ]


def _diff_baseline(data: dict) -> list:
    out = []
    for v in data["variants"]:
        out.append(
            {
                "variant_name": v["variant_name"],
                "summary": v["summary"],
                "added": sorted(
                    (_diff_column(c, "aliases") for c in v["added"]), key=json.dumps
                ),
                "removed": sorted(
                    (_diff_column(c, "aliases") for c in v["removed"]), key=json.dumps
                ),
                "changed": sorted(
                    (
                        [
                            c["variable_name"],
                            c["var_id"],
                            [
                                {
                                    "field": "column"
                                    if ch["field"] == "aliases"
                                    else ch["field"],
                                    "from": ch["from"][0]
                                    if ch["field"] == "aliases"
                                    else ch["from"],
                                    "to": ch["to"][0]
                                    if ch["field"] == "aliases"
                                    else ch["to"],
                                }
                                for ch in c["changes"]
                            ],
                        ]
                        for c in v["changed"]
                    ),
                    key=json.dumps,
                ),
            }
        )
    return sorted(out, key=json.dumps)


def _diff_candidate(data: dict) -> list:
    out = []
    for v in data["variants"]:
        out.append(
            {
                "variant_name": v["variant_name"],
                "summary": v["summary"],
                "added": sorted(
                    (_diff_column(c, "column") for c in v["added"]), key=json.dumps
                ),
                "removed": sorted(
                    (_diff_column(c, "column") for c in v["removed"]), key=json.dumps
                ),
                "changed": sorted(
                    (
                        [c["variable_name"], c["var_id"], c["changes"]]
                        for c in v["changed"]
                    ),
                    key=json.dumps,
                ),
            }
        )
    return sorted(out, key=json.dumps)


def _datacolumns_refs(
    conn: sqlite3.Connection, key: str, register: str, scope: str
) -> list[str]:
    """The FQIDs `get datacolumns <key> --register <register>` matches: by provider
    key or name (split siblings share a key), held ones only in holdings."""
    held = "" if scope == "reference" else " AND " + generator._HELD
    return [
        fqid
        for (fqid,) in conn.execute(
            "SELECT p.slug || '/' || r.slug || '/' || v.slug FROM variable v "
            "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
            "WHERE p.slug || '/' || r.slug = ? AND v.slug IS NOT NULL "
            "AND (v.provider_key = ? OR lower(v.name) = lower(?))" + held,
            (register, key, key),
        )
    ]


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, dict, dict]]:
    del base  # The CLI baseline is the oracle.
    argv = _argv(originals, catalog, scopes)
    datacolumns = {
        case_id: (args[args.index("datacolumns") + 1], _flag(args, "--register"))
        for case_id, args in argv.items()
        if case_id.split("/")[2] == "get-datacolumns"
    }
    # Read before the parallel requests: one connection serves one thread.
    with closing(generator._connect(originals)) as conn:
        variant_slugs = dict(
            conn.execute("SELECT register_variant_id, slug FROM register_variant")
        )
        refs = {
            case_id: _datacolumns_refs(conn, key, register, case_id.split("/")[1])
            for case_id, (key, register) in datacolumns.items()
        }
    results = baseline_cli.result()

    def run(case_id: str):
        scope, command, *rest = case_id.split("/")
        base_cli = results.get(f"{catalog}/{case_id}")
        ok = base_cli is not None and base_cli["exit"] == 0
        data = json.loads(base_cli["stdout"]) if ok else None
        args = argv[f"{catalog}/{case_id}"]
        if command == "get-datacolumns":
            with_refs = refs[f"{catalog}/{case_id}"]
            found: set[tuple] = set()
            for ref in with_refs:
                rows = _pages(cand, "/api/schema/" + quote(ref), {"scope": scope})
                found |= {(r["variant"], r["column"]) for r in rows or []}
            fold = str.lower if scope == "holdings" else str
            expected = (
                sorted(
                    {
                        (
                            variant_slugs.get(int(r["register_variant_id"])),
                            fold(r["delivery_column_name"]),
                        )
                        for r in data
                    }
                )
                if ok
                else "error"
            )
            actual = (
                sorted({(v, fold(c)) for v, c in found if c is not None})
                if with_refs
                else "error"
            )
            return case_id, expected, actual
        register = "/".join(rest)
        path = quote(register)
        if command == "get-diff":
            params = {
                "scope": scope,
                "from": _flag(args, "--from"),
                "to": _flag(args, "--to"),
            }
            answer = get(cand, f"/api/diff/{path}", params)
            expected = _diff_baseline(data) if ok else "error"
            actual = (
                _diff_candidate(answer["body"]["data"])
                if answer["status"] == 200
                else "error"
            )
            return case_id, expected, actual
        params = {"scope": scope}
        if command == "get-schema-year":
            params["period"] = _flag(args, "--years")
        rows = _pages(cand, f"/api/schema/{path}", params)
        expected = _schema_baseline(data, register) if ok else "error"
        actual = _schema_candidate(rows) if rows is not None else "error"
        return case_id, expected, actual

    ids = sorted(case_id.split("/", 1)[1] for case_id in argv)
    with ThreadPoolExecutor(PARALLEL) as pool:
        return list(pool.map(run, ids))
