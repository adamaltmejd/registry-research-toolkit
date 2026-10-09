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

A steward catalog's reference ``get-schema-summary`` whose baseline answer equals the
global catalog's case of the same register is not walked again (G1 budget).

Rows compare as sorted lists of the fields both arms carry: storage ids are not part of
the API, and the CLI emits the window-only fields (operational definition, source text,
definition, unit) only on expanded rows. A refusal compares as ``error``. The sampled
years and the datacolumns argv come from ``cases.py``'s generator, run again with the
configured seed (the CLI results do not carry their argv).
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from urllib.parse import quote

from conformance.differential import cases as generator
from conformance.differential.served.common import (
    cli_argv,
    flag,
    get,
    matched_refs,
    pages,
)

# Requests in flight per server pair.
PARALLEL = 16
COMMANDS = ("get-schema-summary", "get-schema-year", "get-diff", "get-datacolumns")


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


def _order(row: list) -> str:
    """A row's sort key with its window last, so a window that differs between the
    arms (holdings clips to the request; the CLI does not) keeps its row's place."""
    return json.dumps([row[0], *row[3:], row[1], row[2]])


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
    return sorted(rows, key=_order)


def _schema_candidate(rows: list[dict]) -> list:
    return sorted(
        (_row(r["variant"], r["valid_from"], r["valid_to"], r) for r in rows),
        key=_order,
    )


def _first(aliases: list[str]) -> str | None:
    """`get diff`'s column: `aliases` is `[column]`, or `[]` without one."""
    return aliases[0] if aliases else None


def _baseline_column(column: dict) -> dict:
    return {**column, "column": _first(column["aliases"])}


def _baseline_change(change: dict) -> dict:
    """A `get diff` change in `diff`'s shape: `aliases` is the column."""
    if change["field"] == "aliases":
        return {
            "field": "column",
            "from": _first(change["from"]),
            "to": _first(change["to"]),
        }
    return {key: change[key] for key in ("field", "from", "to")}


def _diff(data: dict, column=lambda c: c, change=lambda c: c) -> list:
    """A diff's variants as sorted rows, after mapping each added or removed
    `column` and each `change` onto `diff`'s shape."""

    def columns(found: list[dict]) -> list:
        return sorted(
            (
                [
                    c["variable_name"],
                    c["var_id"],
                    c["data_type"],
                    c["data_length"],
                    c["column"],
                ]
                for c in map(column, found)
            ),
            key=json.dumps,
        )

    return sorted(
        (
            {
                "variant_name": v["variant_name"],
                "summary": v["summary"],
                "added": columns(v["added"]),
                "removed": columns(v["removed"]),
                "changed": sorted(
                    (
                        [
                            c["variable_name"],
                            c["var_id"],
                            list(map(change, c["changes"])),
                        ]
                        for c in v["changed"]
                    ),
                    key=json.dumps,
                ),
            }
            for v in data["variants"]
        ),
        key=json.dumps,
    )


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, dict, dict]]:
    # The CLI baseline is the oracle, and its cases fix the scopes.
    del base, scopes
    argv = cli_argv(originals, catalog, COMMANDS)
    datacolumns = {
        case_id: (args[args.index("datacolumns") + 1], flag(args, "--register"))
        for case_id, args in argv.items()
        if case_id.split("/")[2] == "get-datacolumns"
    }
    # Read before the parallel requests: one connection serves one thread.
    with closing(generator.connect(originals)) as conn:
        variant_slugs = dict(
            conn.execute("SELECT register_variant_id, slug FROM register_variant")
        )
        refs = {
            case_id: matched_refs(conn, key, register, case_id.split("/")[1])
            for case_id, (key, register) in datacolumns.items()
        }
        # Each register's state rows: its whole-history walk's length.
        states = dict(
            conn.execute(
                "SELECT p.slug || '/' || r.slug, count(*) FROM variable_state "
                "JOIN register_variant USING(register_variant_id) "
                "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
                "GROUP BY 1"
            )
        )

    def baseline(case_id: str) -> tuple[bool, object]:
        """Whether the CLI baseline case succeeded, and its JSON."""
        base_cli = baseline_cli.result().get(f"{catalog}/{case_id}")
        ok = base_cli is not None and base_cli["exit"] == 0
        return ok, json.loads(base_cli["stdout"]) if ok else None

    # Each case requests the candidate before it waits on `baseline_cli`, so the
    # walks run beside the CLI arms (G1 budget).
    def run(case_id: str):
        scope, command, *rest = case_id.split("/")
        args = argv[f"{catalog}/{case_id}"]
        if command == "get-datacolumns":
            with_refs = refs[f"{catalog}/{case_id}"]
            found: set[tuple] = set()
            for ref in with_refs:
                rows = pages(cand, "/api/schema/" + quote(ref), {"scope": scope})
                found |= {(r["variant"], r["column"]) for r in rows or []}
            fold = str.lower if scope == "holdings" else str
            ok, data = baseline(case_id)
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
                "from": flag(args, "--from"),
                "to": flag(args, "--to"),
            }
            answer = get(cand, f"/api/diff/{path}", params)
            ok, data = baseline(case_id)
            expected = (
                _diff(data, _baseline_column, _baseline_change) if ok else "error"
            )
            actual = (
                _diff(answer["body"]["data"]) if answer["status"] == 200 else "error"
            )
            return case_id, expected, actual
        params = {"scope": scope}
        if command == "get-schema-year":
            params["period"] = flag(args, "--years")
        rows = pages(cand, f"/api/schema/{path}", params)
        ok, data = baseline(case_id)
        expected = _schema_baseline(data, register) if ok else "error"
        actual = _schema_candidate(rows) if rows is not None else "error"
        return case_id, expected, actual

    def twin(case_id: str) -> bool:
        """A steward catalog's reference ``get schema --summary`` whose baseline
        answer equals the global catalog's for the same register (the SCB registers
        both carry): the global walk compares the same rows."""
        scope, command, *_ = case_id.split("/")
        if (scope, command) != ("reference", "get-schema-summary"):
            return False
        results = baseline_cli.result()
        mine = results.get(f"{catalog}/{case_id}")
        other = results.get(f"global/{case_id}")
        return (
            mine is not None
            and other is not None
            and all(mine[k] == other[k] for k in ("exit", "stdout", "stderr"))
        )

    ids = [case_id.split("/", 1)[1] for case_id in argv]
    if catalog != "global":
        # simplify: a twin is not walked, so the server's reference `schema` on the
        # steward file goes uncompared for the registers it shares unchanged with
        # the global file (the steward's own registers and holdings still walk);
        # compare a twin's first page if a steward-only reference defect slips by.
        walked = [case_id for case_id in ids if not twin(case_id)]
        print(
            f"served {catalog} schema: {len(ids) - len(walked)} reference summaries "
            "not walked, equal to the global catalog's",
            flush=True,
        )
        ids = walked

    def length(case_id: str) -> tuple:
        _, command, *rest = case_id.split("/")
        return command != "get-schema-summary", -states.get("/".join(rest), 0)

    # A walk's pages run one after another, so the longest walks start first
    # instead of forming the tail.
    ids.sort(key=lambda case_id: (*length(case_id), case_id))
    with ThreadPoolExecutor(PARALLEL) as pool:
        return sorted(pool.map(run, ids), key=lambda case: case[0])
