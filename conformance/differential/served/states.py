"""``states`` and ``warnings`` in each named scope against the baseline webapp's binding
routes, for a seeded sample of variables:

- ``states/<fqid>``: the whole history, against the states the binding node embeds
  (the catalog page's light hydration, which ``states`` serves), every page.
- ``states-period/<fqid>/<year>``: one sampled year of the variable's states, against
  the catch-all's ``?period`` subset.
- ``warnings/<fqid>/<filter>`` and ``warnings/<register>/<filter>``: the warnings of
  the variable and of its register, unfiltered and by the sampled year, the first
  state's variant and column, and ``unassigned_only``, against ``/data_warnings``.

The baseline renders a non-leap February window's token as
``YYYY-02-01..YYYY-02-28``; ``states`` renders ``YYYY-02``, the token whose bounds
those are (a Rust-only fix pinned by ``api/states-window-fallback``, stage 3b–3e
decision 6). The mapping converts the baseline's token, since the difference sits
at no fixed path.
"""

from __future__ import annotations

import calendar
import re
import sqlite3
import tomllib
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from conformance.differential.cases import seeded_sample
from conformance.differential.served.common import get

PARALLEL = 16
VARIABLES = 80
SEED = tomllib.loads((Path(__file__).parents[1] / "config.toml").read_text())["seed"]
FEBRUARY = re.compile(r"^(\d{4})-02-01\.\.\1-02-28$")


def _route(ref: str) -> str:
    return "/".join(quote(s, safe="") for s in ref.split("/"))


def _token(state: dict) -> dict:
    match = FEBRUARY.match(state.get("period_token") or "")
    if match and not calendar.isleap(int(match[1])):
        return {**state, "period_token": f"{match[1]}-02"}
    return state


def _baseline_states(base, fqid: str, scope: str, period: str | None) -> object:
    answer = get(
        base, f"/api/catalog/{_route(fqid)}", {"scope": scope, "period": period}
    )
    if answer["status"] != 200:
        return {"status": answer["status"]}
    return [_token(s) for s in answer["body"]["states"]]


def _candidate_states(cand, fqid: str, scope: str, period: str | None) -> object:
    items, cursor = [], None
    while True:
        params = {"scope": scope, "period": period, "limit": "200", "cursor": cursor}
        answer = get(cand, f"/api/states/{_route(fqid)}", params)
        if answer["status"] != 200:
            return {"status": answer["status"]}
        items += answer["body"]["data"]["items"]
        cursor = answer["body"]["data"]["next_cursor"]
        if cursor is None:
            return items


def _warnings(client, path: str, params: dict, rust: bool) -> object:
    answer = get(client, path, params)
    if answer["status"] != 200:
        return {"status": answer["status"]}
    return answer["body"]["data"] if rust else answer["body"]


def _samples(conn: sqlite3.Connection, catalog: str) -> list[tuple]:
    """``(fqid, register fqid, year, variant, column)`` per sampled variable: a
    year, variant and column of its first dated state."""
    variables = conn.execute(
        "SELECT p.slug || '/' || r.slug || '/' || v.slug, p.slug || '/' || r.slug, "
        "(SELECT substr(s.valid_from, 1, 4) || ' ' || rv.slug || ' ' || "
        "s.delivery_column_name FROM variable_state s JOIN register_variant rv "
        "USING(register_variant_id) WHERE s.variable_id = v.variable_id "
        "AND s.valid_from IS NOT NULL AND s.delivery_column_name IS NOT NULL "
        "ORDER BY s.valid_from, s.state_id LIMIT 1) "
        "FROM variable v JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL ORDER BY v.variable_id"
    ).fetchall()
    out = []
    for fqid, register, first in seeded_sample(
        SEED, catalog, "states", variables, VARIABLES
    ):
        year, variant, column = (first or "  ").split(" ", 2)
        out.append((fqid, register, year or None, variant or None, column or None))
    return out


def cases(base, cand, catalog, scopes, originals) -> list[tuple[str, dict, dict]]:
    with closing(
        sqlite3.connect(
            f"file:{originals / 'reg_meta.db'}?mode=ro&immutable=1", uri=True
        )
    ) as conn:
        samples = _samples(conn, catalog)
    # A register's warning cases repeat for each of its sampled variables; keyed once.
    jobs = {}
    for scope in (s for s in scopes if s is not None):
        for fqid, register, year, variant, column in samples:
            jobs[f"{scope}/states/{fqid}"] = ("states", fqid, scope, None)
            if year:
                key = f"{scope}/states-period/{fqid}/{year}"
                jobs[key] = ("states", fqid, scope, year)
            filters = {
                "all": {},
                "period": {"period": year},
                "variant": {"variant": variant},
                "representation": {"variant": variant, "representation": column},
                "unassigned": {"unassigned_only": "true"},
            }
            for ref in (fqid, register):
                for name, params in filters.items():
                    if all(params.values()):
                        key = f"{scope}/warnings/{ref}/{name}"
                        jobs[key] = ("warnings", ref, scope, params)

    def run(job):
        key, (op, ref, scope, extra) = job
        if op == "states":
            return (
                key,
                _baseline_states(base, ref, scope, extra),
                _candidate_states(cand, ref, scope, extra),
            )
        params = {"scope": scope, **extra}
        return (
            key,
            _warnings(base, f"/api/catalog/{_route(ref)}/data_warnings", params, False),
            _warnings(cand, f"/api/warnings/{_route(ref)}", params, True),
        )

    with ThreadPoolExecutor(PARALLEL) as pool:
        return list(pool.map(run, jobs.items()))
