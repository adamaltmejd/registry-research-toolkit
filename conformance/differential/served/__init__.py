"""G1 for the operations ``reg-meta serve`` implements.

The Rust server serves each candidate copy; the baseline commit's ``reg_webapp``, from
the baseline environment, serves the release original and is the oracle. Each operation
family is a module here, listed in ``FAMILIES``: its ``cases`` maps the baseline's
responses onto its operations' shapes, so the two arms compare as JSON in the CLI
cases' result form (``exit``, ``stdout``, ``stderr``).

- ``context``: the catalog's identity and counts per scope.
- ``search``: each ``type``'s pages per scope and term.
- ``docs``: ``docs_get`` per document, ``docs_related`` per register with documents,
  and each related document's download.
- ``show``: every catalog node kind per named scope, retired refs, and owning
  variables against the CLI baseline.
- ``states``: ``states`` and ``warnings`` for sampled variables and their registers.
- ``values``: sampled variables' coded states and their books' partitions, and each
  classification's codes against the CLI baseline's ``get classification --codes``.
- ``graph``: ``graph`` per sampled variable, group and every classification,
  ``lineage``'s warnings per sampled variable, and its provenance rows against the
  CLI baseline's ``get lineage``.
- ``schema``: ``schema`` and ``diff`` against the CLI baseline's ``get schema``,
  ``get datacolumns`` and ``get diff``.

``show``'s, ``values``', ``graph``'s and ``schema``'s ``cases`` also take
``baseline_cli``, a
future of the CLI arm's baseline results by case id, set once the CLI arms finish, so
they compare with a CLI baseline case instead of running it again.
"""

from __future__ import annotations

import json
import shlex
import time
from typing import TYPE_CHECKING

from conformance.differential.cache import REPO_ROOT
from conformance.differential.served import (
    context,
    docs,
    graph,
    schema,
    search,
    show,
    states,
    values,
)
from conformance.http_cases import ServerPool

if TYPE_CHECKING:
    from concurrent.futures import Future
    from pathlib import Path

FAMILIES = (context, search, docs, show, states, values, graph, schema)
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
    originals: dict[str, Path],
    derived: dict[str, Path],
    log_dir: Path,
    baseline_cli: Future[dict[str, dict]],
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
        for catalog in sorted(originals):
            base = baseline.client(
                {
                    "REG_META_DB": str(originals[catalog]),
                    "REG_WEBAPP_STEWARD": catalog,
                    "REG_WEBAPP_STEWARDS_DIR": str(baseline_stewards),
                }
            )
            cand = rust.client(
                {"REG_META_DB": str(derived[catalog]), "REG_WEBAPP_STEWARD": catalog}
            )
            scopes = [None, "reference"] + (["holdings"] if catalog != "global" else [])
            for family in FAMILIES:
                started = time.monotonic()
                args = (base, cand, catalog, scopes, originals[catalog])
                found = (
                    family.cases(*args, baseline_cli)
                    if family in {show, values, graph, schema}
                    else family.cases(*args)
                )
                cases += [
                    (f"{catalog}/{key}", _result(expected), _result(actual))
                    for key, expected, actual in found
                ]
                # Wall time per family (G1 budget); a family that waits on
                # `baseline_cli` includes the wait.
                name = family.__name__.rpartition(".")[2]
                print(
                    f"served {catalog} {name}: {len(found)} cases in "
                    f"{time.monotonic() - started:.1f} s",
                    flush=True,
                )
    finally:
        baseline.close()
        rust.close()
    return cases
