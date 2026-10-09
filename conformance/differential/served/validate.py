"""``validate-project/<coordinate>``: ``validate`` on each project the CLI arms
validate (``cases.py``'s generated projects, one per sampled register variant)
against the CLI baseline's ``validate`` case of that coordinate, reused by case id
instead of run again. Both compare as the ``{ok, issues}`` document: the CLI prints
it, ``validate`` answers it as ``data``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from concurrent.futures import Future
    from pathlib import Path

# `reg-meta validate`'s exits: 0 when `ok`, 17 (`EXIT_NO_MATCH`) when not.
PRINTED = (0, 17)


def cases(
    base,
    cand,
    catalog,
    scopes,
    originals,
    baseline_cli: Future[dict[str, dict]],
    projects: Path,
) -> list[tuple[str, dict, dict]]:
    """``projects`` is the directory ``cases.generate`` wrote the projects to."""
    out = []
    prefix = f"{catalog}/-/validate/"
    for case_id, result in sorted(baseline_cli.result().items()):
        if not case_id.startswith(prefix):
            continue
        coordinate = case_id.removeprefix(prefix)
        expected = (
            json.loads(result["stdout"])
            if result["exit"] in PRINTED
            else {"exit": result["exit"], "stderr": result["stderr"]}
        )
        # The file name `cases.py` gives a coordinate: slugs never hold `--`.
        project = projects / catalog / (coordinate.replace("/", "--") + ".json")
        response = cand.post(
            "/api/project/validate",
            content=project.read_bytes(),
            headers={"content-type": "application/json"},
        )
        body = response.json()
        actual = (
            body["data"]
            if response.status_code == 200
            else {"status": response.status_code, "body": body}
        )
        out.append((f"-/validate-project/{coordinate}", expected, actual))
    return out
