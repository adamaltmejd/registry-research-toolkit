"""``coded_variables`` against the CLI arm's ``get coded-variables --min-registers 5``
(``get-coded-variables`` per named scope), reused by case id.

The CLI ranks by registers, then distinct codes, then name, keeps names in at least
five registers and cuts at its default limit of 100; ``coded_variables`` orders by
distinct codes. Every page is read, filtered and re-ranked the CLI's way, so the two
compare as the same ordered rows.
"""

from __future__ import annotations

import json

from conformance.differential.served.common import pages

MIN_REGISTERS = 5
CLI_LIMIT = 100
FIELDS = ("variable_name", "n_distinct_codes", "n_registers", "n_instances")


def _rows(rows: list[dict]) -> list[list]:
    return [[row[f] for f in FIELDS] for row in rows]


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, object, object]]:
    del base, originals
    results = baseline_cli.result()
    out = []
    for scope in (s for s in scopes if s is not None):
        case_id = f"{scope}/get-coded-variables"
        base_cli = results[f"{catalog}/{case_id}"]
        expected = (
            _rows(json.loads(base_cli["stdout"])) if base_cli["exit"] == 0 else "error"
        )
        rows = pages(cand, "/api/coded-variables", {"scope": scope})
        actual = (
            _rows(
                sorted(
                    (r for r in rows if r["n_registers"] >= MIN_REGISTERS),
                    key=lambda r: (
                        -r["n_registers"],
                        -r["n_distinct_codes"],
                        r["variable_name"],
                    ),
                )[:CLI_LIMIT]
            )
            if rows is not None
            else "error"
        )
        out.append((case_id, expected, actual))
    return out
