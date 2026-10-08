"""``values`` in each named scope:

- ``values/<fqid>/<state_id>``: each coded state of a seeded sample of variables (the
  states the baseline's binding node embeds, so in holdings only held ones), against
  the baseline webapp's ``/api/value-sets/{id}/codes``. A state carrying a coded
  window's set names the window by ``column`` and ``alias_window_from``.
- ``values/<fqid>/<state_id>/<book>/<partition>``: each partition of each book with a
  stored conformance, against the same route with ``state`` and ``classification``.
- ``values-classification-codes/<name>``: the CLI baseline's ``get classification
  <name> --codes`` case of that id (reused, not run again) against ``values`` on the
  classification. Both compare as ``(code, label, level, is_valid)`` rows sorted by
  code and label: the CLI orders by code alone and leaves out a null ``is_valid``.

The baseline pages by offset (1000 a page) and ``values`` by cursor (200 a page);
each side's pages are concatenated, so the comparison covers paging to the end.
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
from conformance.differential.served.common import get

PARALLEL = 16
VARIABLES = 40
SEED = tomllib.loads((Path(__file__).parents[1] / "config.toml").read_text())["seed"]
PARTITIONS = ("source_extensions", "canonical", "nonstandard", "sentinels")


def _route(ref: str) -> str:
    return "/".join(quote(s, safe="") for s in ref.split("/"))


def _baseline(base, value_set_id: str, params: dict) -> object:
    codes, offset = [], 0
    while True:
        path = f"/api/value-sets/{value_set_id}/codes"
        answer = get(base, path, {**params, "limit": "1000", "offset": str(offset)})
        if answer["status"] != 200:
            return {"status": answer["status"]}
        page = answer["body"]["codes"]
        codes += page
        offset += len(page)
        if not page or offset >= answer["body"]["total"]:
            return codes


def _candidate(cand, ref: str, params: dict) -> object:
    items, cursor = [], None
    while True:
        answer = get(
            cand,
            f"/api/values/{_route(ref)}",
            {**params, "limit": "200", "cursor": cursor},
        )
        if answer["status"] != 200:
            return {"status": answer["status"]}
        items += answer["body"]["data"]["items"]
        cursor = answer["body"]["data"]["next_cursor"]
        if cursor is None:
            return items


def _jobs(base, catalog: str, scope: str, fqid: str) -> list[tuple]:
    """``(key, value set id, baseline params, candidate params)`` per comparison."""
    answer = get(base, f"/api/catalog/{_route(fqid)}", {"scope": scope})
    if answer["status"] != 200:
        return []
    jobs = []
    for state in answer["body"].get("states", []):
        if state["value_set_id"] is None:
            continue
        window = (
            {
                "column": state["delivery_column_name"],
                "alias_window_from": state["coding_window_from"],
            }
            if state["coding_window_from"]
            else {}
        )
        key = f"{scope}/values/{fqid}/{state['state_id']}"
        if window:
            key += f"/{state['delivery_column_name']}/{state['coding_window_from']}"
        jobs.append(
            (key, state["value_set_id"], {}, {"state": state["state_id"], **window})
        )
        for book in state["classifications"]:
            if book["conformance"] is None:
                continue
            for partition in PARTITIONS:
                shared = {"state": state["state_id"], "partition": partition, **window}
                jobs.append(
                    (
                        f"{key}/{book['slug']}/{partition}",
                        state["value_set_id"],
                        {**shared, "classification": book["slug"]},
                        {**shared, "classification": f"class/{book['slug']}"},
                    )
                )
    return [(*job, scope, fqid) for job in jobs]


def _code_rows(rows: list[dict]) -> list[list]:
    return sorted([r["code"], r["label"], r["level"], r.get("is_valid")] for r in rows)


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, object, object]]:
    with closing(
        sqlite3.connect(
            f"file:{originals / 'reg_meta.db'}?mode=ro&immutable=1", uri=True
        )
    ) as conn:
        variables = conn.execute(
            "SELECT p.slug || '/' || r.slug || '/' || v.slug FROM variable v "
            "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM variable_state s WHERE s.variable_id = v.variable_id "
            "AND s.value_set_id IS NOT NULL) ORDER BY v.variable_id"
        ).fetchall()
        short_names = dict(conn.execute("SELECT short_name, slug FROM classification"))
    sample = [
        f for (f,) in seeded_sample(SEED, catalog, "values", variables, VARIABLES)
    ]
    named = [s for s in scopes if s is not None]

    def run(job):
        key, value_set_id, base_params, cand_params, scope, fqid = job
        return (
            key,
            _baseline(base, value_set_id, {"scope": scope, **base_params}),
            _candidate(cand, fqid, {"scope": scope, **cand_params}),
        )

    with ThreadPoolExecutor(PARALLEL) as pool:
        listed = pool.map(
            lambda job: _jobs(base, catalog, *job),
            [(scope, fqid) for scope in named for fqid in sample],
        )
        out = list(pool.map(run, [job for jobs in listed for job in jobs]))
    # A book's codes against the CLI arm's baseline results, reused by case id.
    for case_id, base_cli in sorted(baseline_cli.result().items()):
        name, scope, command, *rest = case_id.split("/")
        if name != catalog or command != "get-classification-codes":
            continue
        short_name = "/".join(rest)
        slug = short_names.get(short_name)
        if slug is None:
            continue
        expected = (
            _code_rows(json.loads(base_cli["stdout"])["codes"])
            if base_cli["exit"] == 0
            else {"exit": base_cli["exit"]}
        )
        params = {} if scope == "-" else {"scope": scope}
        actual = _candidate(cand, f"class/{slug}", params)
        if isinstance(actual, list):
            actual = _code_rows(actual)
        out.append(
            (f"{scope}/values-classification-codes/{short_name}", expected, actual)
        )
    return out
