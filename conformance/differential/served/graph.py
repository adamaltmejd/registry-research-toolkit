"""``graph-*`` and ``lineage-*``: ``graph`` and ``lineage`` in each named scope.

- ``graph-variable/<fqid>`` for a seeded sample of variables, ``graph-group/<ref>``
  for a seeded sample of concept groups, every classification group and family,
  and ``graph-classification/<fqid>`` per classification, against the baseline
  webapp's three ``/graph`` routes, whose body is ``graph``'s data.
- ``lineage-warnings/<fqid>``: the same variables' ``warnings`` against the
  baseline's ``/lineage_warnings``.
- ``lineage-registers/<stratum>/<id>``: the CLI baseline's ``get lineage <name>
  --register <register>`` case of that id (reused, not run again) against
  ``lineage``'s ``registers`` in reference scope, the CLI's, kept to that register.
  A variable without a name is not compared: the CLI then matches its
  ``provider_key``, which other variables of the register may share, while
  ``lineage`` matches a nameless variable only itself. Both compare as sorted ``(register_name, role, source_register_text,
  instance_count, year_range)`` rows: the CLI lists in table order and names
  registers by storage id.

A retired ref is not compared: graph resolves refs as ``show`` does, and the
``show-retired`` cases compare the baseline's redirects.
"""

from __future__ import annotations

import json
import sqlite3
import tomllib
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from conformance.differential.cases import seeded_sample
from conformance.differential.served.show import FAMILIES, PARALLEL

# Variables and concept groups compared per catalog, drawn with the configured seed.
VARIABLES = 80
GROUPS = 200
SEED = tomllib.loads((Path(__file__).parents[1] / "config.toml").read_text())["seed"]


def _quoted(ref: str) -> str:
    return "/".join(quote(s, safe="") for s in ref.split("/"))


def _answer(client, path: str, scope: str) -> dict:
    response = client.get(path, params={"scope": scope})
    if response.status_code != 200:
        return {"status": response.status_code}
    body = response.json()
    return body.get("data", body)


def _rows(rows: list[dict]) -> list[list]:
    keys = ("register_name", "role", "source_register_text", "instance_count")
    return sorted([*(r[k] for k in keys), r["year_range"]] for r in rows)


def _refs(conn: sqlite3.Connection, catalog: str) -> list[tuple[str, str]]:
    """``(command, ref)`` per compared ref."""
    variables = conn.execute(
        "SELECT p.slug || '/' || r.slug || '/' || v.slug FROM variable v "
        "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL ORDER BY v.variable_id"
    ).fetchall()
    sample = seeded_sample(SEED, catalog, "graph-variables", variables, VARIABLES)
    groups = conn.execute(
        "SELECT 'group/' || p.slug || '/' || r.slug || '/' || g.group_key "
        "FROM concept_group g JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) WHERE g.kind = 'variable' ORDER BY 1"
    ).fetchall()
    groups = seeded_sample(SEED, catalog, "graph-groups", groups, GROUPS)
    groups += conn.execute(
        "SELECT 'group/class/' || group_key FROM concept_group "
        "WHERE kind = 'classification' ORDER BY 1"
    ).fetchall()
    groups += [(f"group/class/{key}",) for key in FAMILIES]
    classifications = conn.execute(
        "SELECT 'class/' || slug FROM classification WHERE slug IS NOT NULL ORDER BY 1"
    ).fetchall()
    return (
        [("graph-variable", f) for (f,) in sample]
        + [("lineage-warnings", f) for (f,) in sample]
        + [("graph-group", g) for (g,) in groups]
        + [("graph-classification", c) for (c,) in classifications]
    )


def _pair(base, cand, scope: str, command: str, ref: str) -> tuple[str, object, object]:
    if command == "lineage-warnings":
        expected = _answer(base, f"/api/catalog/{_quoted(ref)}/lineage_warnings", scope)
        if "lineage_warnings" in expected:
            expected = expected["lineage_warnings"]
        actual = _answer(cand, f"/api/lineage/{_quoted(ref)}", scope)
        if "warnings" in actual:
            actual = actual["warnings"]
    else:
        expected = _answer(base, f"/api/catalog/{_quoted(ref)}/graph", scope)
        actual = _answer(cand, f"/api/graph/{_quoted(ref)}", scope)
    return f"{scope}/{command}/{ref}", expected, actual


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, object, object]]:
    with closing(
        sqlite3.connect(
            f"file:{originals / 'reg_meta.db'}?mode=ro&immutable=1", uri=True
        )
    ) as conn:
        refs = _refs(conn, catalog)
        slugs = {
            str(variable_id): (f"{p}/{r}/{v}" if v else None, f"{p}/{r}")
            for variable_id, p, r, v in conn.execute(
                "SELECT v.variable_id, p.slug, r.slug, v.slug FROM variable v "
                "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
                "WHERE v.name IS NOT NULL"
            )
        }
    named = [s for s in scopes if s is not None]
    work = [(scope, *ref) for scope in named for ref in refs]
    with ThreadPoolExecutor(PARALLEL) as pool:
        out = list(pool.map(lambda job: _pair(base, cand, *job), work))
    # `get lineage` against the CLI arm's baseline results, reused by case id.
    for case_id, base_cli in sorted(baseline_cli.result().items()):
        name, _, command, *key = case_id.split("/")
        if name != catalog or command != "get-lineage":
            continue
        variable, register = slugs.get(key[-1], (None, None))
        if variable is None:
            continue
        expected = (
            _rows(json.loads(base_cli["stdout"])["registers"])
            if base_cli["exit"] == 0
            else {"exit": base_cli["exit"]}
        )
        answer = _answer(cand, f"/api/lineage/{_quoted(variable)}", "reference")
        actual = (
            _rows([r for r in answer["registers"] if r["register"] == register])
            if "registers" in answer
            else answer
        )
        out.append((f"reference/lineage-registers/{'/'.join(key)}", expected, actual))
    return out
