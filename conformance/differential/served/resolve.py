"""``resolve`` against the CLI arm's ``resolve --columns <sampled delivery columns>
--register <fqid>`` (``resolve/<fqid>`` per named scope), reused by case id.

``--columns`` is split on commas and stripped, as the CLI reads it, and sent as
repeated ``columns`` keys. Each column compares as its name, status and matches in
order; a match compares by its FQID, ``var_id``, name and matched column (the CLI's
storage ``register_id`` is not part of the API). A refusal compares as ``error``.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

from conformance.differential.served.common import cli_argv, flag, get

PARALLEL = 16
COMMANDS = ("resolve",)


def _columns(data: dict) -> list:
    return [
        [
            c["column_name"],
            c["status"],
            [
                [m["fqid"], m["var_id"], m["variable_name"], m["matched_column"]]
                for m in c["matches"]
            ],
        ]
        for c in data["columns"]
    ]


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, object, object]]:
    # The CLI baseline is the oracle, and its cases fix the scopes.
    del base, scopes
    argv = cli_argv(originals, catalog, COMMANDS)
    results = baseline_cli.result()

    def run(case_id: str):
        scope = case_id.split("/")[0]
        args = argv[f"{catalog}/{case_id}"]
        base_cli = results[f"{catalog}/{case_id}"]
        columns = [c.strip() for c in flag(args, "--columns").split(",") if c.strip()]
        answer = get(
            cand,
            "/api/resolve",
            {"columns": columns, "register": flag(args, "--register"), "scope": scope},
        )
        expected = (
            _columns(json.loads(base_cli["stdout"]))
            if base_cli["exit"] == 0
            else "error"
        )
        actual = (
            _columns(answer["body"]["data"]) if answer["status"] == 200 else "error"
        )
        return case_id, expected, actual

    ids = sorted(case_id.split("/", 1)[1] for case_id in argv)
    with ThreadPoolExecutor(PARALLEL) as pool:
        return list(pool.map(run, ids))
