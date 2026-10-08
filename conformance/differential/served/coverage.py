"""``coverage`` against the CLI arm's ``get availability`` results, reused by case id:

- ``get-availability/<fqid>``: a register's years, gaps and years per variant
  (by slug; storage ids are not part of the API).
- ``get-availability-variable/<stratum>/<id>``: ``get availability <name or key>
  --register <register>`` matches every variable of that key or name (split
  siblings), one ``registers`` entry each; ``coverage`` answers one variable per ref,
  so the matched variables' answers are unioned. Each year's columns compare as a
  sorted list (the CLI lists them in state order).

A refusal compares as ``error``; a variable answer of no ref compares as ``error``,
as the CLI refuses a name none of its variables covers.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from urllib.parse import quote

from conformance.differential import cases as generator
from conformance.differential.served.common import cli_argv, flag, get, matched_refs

PARALLEL = 16
COMMANDS = ("get-availability", "get-availability-variable")


def _span(data: dict) -> dict:
    return {key: data[key] for key in ("min_year", "max_year", "years", "gaps")}


def _entry(entry: dict) -> list:
    aliases = {
        year: sorted(set(cols)) for year, cols in entry["aliases_by_year"].items()
    }
    return [entry["register_name"], entry["var_id"], _span(entry), aliases]


def _register(data: dict, slugs: dict | None = None) -> dict:
    """A register's coverage; `slugs` maps the CLI's variant ids to slugs."""
    variants = sorted(
        (
            [
                slugs.get(v["register_variant_id"]) if slugs else v["variant"],
                v["variant_name"],
                v["years"],
            ]
            for v in data["variants"]
        ),
        key=json.dumps,
    )
    return {"register_name": data["register_name"], **_span(data), "variants": variants}


def _variables(answers: list[dict]) -> dict | str:
    """Variable coverages as one: their entries, and the span of every year."""
    if not answers:
        return "error"
    entries = sorted(
        (_entry(e) for data in answers for e in data["registers"]), key=json.dumps
    )
    years = sorted({y for data in answers for y in data["years"]})
    return {
        "years": years,
        "gaps": [y for y in range(years[0], years[-1] + 1) if y not in years],
        "registers": entries,
    }


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, object, object]]:
    # The CLI baseline is the oracle, and its cases fix the scopes.
    del base, scopes
    argv = cli_argv(originals, catalog, COMMANDS)
    with closing(generator.connect(originals)) as conn:
        slugs = dict(
            conn.execute("SELECT register_variant_id, slug FROM register_variant")
        )
        refs = {
            case_id: matched_refs(
                conn,
                args[args.index("availability") + 1],
                flag(args, "--register"),
                case_id.split("/")[1],
            )
            for case_id, args in argv.items()
            if case_id.split("/")[2] == "get-availability-variable"
        }
    results = baseline_cli.result()

    def run(case_id: str):
        scope, command, *rest = case_id.split("/")
        base_cli = results[f"{catalog}/{case_id}"]
        data = json.loads(base_cli["stdout"]) if base_cli["exit"] == 0 else None
        if command == "get-availability":
            answer = get(
                cand, "/api/coverage/" + quote("/".join(rest)), {"scope": scope}
            )
            expected = _register(data, slugs) if data else "error"
            actual = (
                _register(answer["body"]["data"])
                if answer["status"] == 200
                else "error"
            )
            return case_id, expected, actual
        answers = [
            get(cand, "/api/coverage/" + quote(ref), {"scope": scope})
            for ref in refs[f"{catalog}/{case_id}"]
        ]
        expected = _variables([data]) if data else "error"
        actual = _variables([a["body"]["data"] for a in answers if a["status"] == 200])
        return case_id, expected, actual

    ids = sorted(case_id.split("/", 1)[1] for case_id in argv)
    with ThreadPoolExecutor(PARALLEL) as pool:
        return list(pool.map(run, ids))
