"""G1 for the operations ``reg-meta serve`` implements.

The Rust server serves each candidate copy; the baseline commit's ``reg_webapp``, from
the baseline environment, serves the reference copy and is the oracle. Each operation
family is a module here, listed in ``FAMILIES``: its ``cases`` maps the baseline's
responses onto its operations' shapes, so the two arms compare as JSON in the CLI
cases' result form (``exit``, ``stdout``, ``stderr``).

- ``context``: the catalog's identity and counts per scope.
- ``search``: each ``type``'s pages per scope and term.
- ``docs``: ``docs_get`` per document, ``docs_related`` per register with documents,
  and each related document's download.
"""

from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

from conformance.differential.cache import REPO_ROOT
from conformance.differential.served import context, docs, search
from conformance.http_cases import ServerPool

if TYPE_CHECKING:
    from pathlib import Path

FAMILIES = (context, search, docs)
# The production rate limit (30 writes per minute) does not bind GETs. Eight worker
# processes, since one Python process serves one search at a time (G1 budget).
BASELINE_APP = (
    "import sys, uvicorn; uvicorn.run('reg_webapp.app:create_app', factory=True, "
    "workers=8, port=int(sys.argv[1]), log_level='warning')"
)


def _result(value) -> dict:
    return {
        "exit": 0,
        "stdout": json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True),
        "stderr": "",
        "seconds": 0.0,
    }


def served_cases(
    baseline_python: Path,
    baseline_stewards: Path,
    server: Path,
    reference: dict[str, Path],
    derived: dict[str, Path],
    log_dir: Path,
) -> list[tuple[str, dict, dict]]:
    """``(case id, baseline result, checkout result)`` for every served case."""
    for arm in ("baseline", "rust"):
        (log_dir / arm).mkdir(parents=True, exist_ok=True)
    baseline = ServerPool(
        shlex.join([str(baseline_python), "-I", "-c", BASELINE_APP, "{port}"]),
        log_dir / "baseline",
    )
    rust = ServerPool(
        shlex.join(
            [
                str(server),
                "serve",
                "--db",
                "{db}",
                "--catalog",
                "{catalog}",
                "--stewards",
                str(REPO_ROOT / "reg_webapp/stewards"),
                "--port",
                "{port}",
            ]
        ),
        log_dir / "rust",
    )
    cases = []
    try:
        for catalog in sorted(reference):
            base = baseline.client(
                {
                    "REG_META_DB": str(reference[catalog]),
                    "REG_WEBAPP_STEWARD": catalog,
                    "REG_WEBAPP_STEWARDS_DIR": str(baseline_stewards),
                }
            )
            cand = rust.client(
                {"REG_META_DB": str(derived[catalog]), "REG_WEBAPP_STEWARD": catalog}
            )
            scopes = [None, "reference"] + (["holdings"] if catalog != "global" else [])
            for family in FAMILIES:
                for key, expected, actual in family.cases(
                    base, cand, catalog, scopes, reference[catalog]
                ):
                    cases.append(
                        (f"{catalog}/{key}", _result(expected), _result(actual))
                    )
    finally:
        baseline.close()
        rust.close()
    return cases
