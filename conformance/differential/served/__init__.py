"""G1 for the operations ``reg-meta serve`` implements.

The Rust server serves each candidate copy; the baseline commit's ``reg_webapp``, from
the baseline environment, serves the release original and is the oracle. Each operation
family is a module here, listed in ``FAMILIES``: its ``cases`` maps the baseline's
responses onto its operations' shapes, so the two arms compare as JSON in the CLI
cases' result form (``exit``, ``stdout``, ``stderr``).

- ``context``: the catalog's identity and counts per scope.
- ``search``: each ``type``'s pages per scope and term.
- ``docs``: ``docs_get`` per document, ``docs_search`` per document's variable and
  against the CLI baseline's ``docs search``, ``docs list`` and ``docs get``,
  ``docs_related`` per register with documents, and each related document's download.
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
- ``coverage``, ``coded`` and ``resolve``: ``coverage``, ``coded_variables`` and
  ``resolve`` against the CLI baseline's ``get availability``, ``get
  coded-variables`` and ``resolve``.
- ``validate``: ``validate`` on each generated project against the CLI baseline's
  ``validate``.
- ``order``: the manifest download on each generated project against the CLI
  baseline's ``order``.

The families in ``CLI_BASELINE`` take ``baseline_cli`` besides, a future of the CLI
arm's baseline results by case id, set once the CLI arms finish, so they compare with
a CLI baseline case instead of running it again; ``validate`` and ``order`` also take
the directory of the generated projects.
"""

from __future__ import annotations

import json
import shlex
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from conformance.differential.cache import REPO_ROOT
from conformance.differential.served import (
    coded,
    context,
    coverage,
    docs,
    graph,
    order,
    resolve,
    schema,
    search,
    show,
    states,
    validate,
    values,
)
from conformance.http_cases import ServerPool

if TYPE_CHECKING:
    from concurrent.futures import Future
    from pathlib import Path

# Run order within a catalog's lane: the families that never wait on `baseline_cli`
# or wait only at their end come first, so the CLI arms' wall overlaps them. `schema`
# runs in a lane of its own (`LANES`).
FAMILIES = (
    context,
    search,
    states,
    show,
    values,
    graph,
    docs,
    schema,
    coverage,
    coded,
    resolve,
    validate,
    order,
)
# `schema` requests the candidate before it waits on `baseline_cli`; in a lane of
# its own, its walks run beside the CLI arms.
LANES = (tuple(f for f in FAMILIES if f is not schema), (schema,))
CLI_BASELINE = {docs, show, values, graph, schema, coverage, coded, resolve}
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
                # The validate and order families replay every project from one
                # address, past the production write limit.
                "--write-limit",
                "100000",
            ]
        ),
        log_dir / "rust",
    )
    started = time.monotonic()

    def run(catalog: str, families: tuple) -> list[tuple[str, dict, dict]]:
        base, cand = pairs[catalog]
        cases = []
        scopes = [None, "reference"] + (["holdings"] if catalog != "global" else [])
        for family in families:
            family_started = time.monotonic()
            args = (base, cand, catalog, scopes, originals[catalog])
            if family in (validate, order):
                # `__main__` writes the projects beside the servers' logs.
                found = family.cases(*args, baseline_cli, log_dir.parent / "projects")
            elif family in CLI_BASELINE:
                found = family.cases(*args, baseline_cli)
            else:
                found = family.cases(*args)
            cases += [
                (f"{catalog}/{key}", _result(expected), _result(actual))
                for key, expected, actual in found
            ]
            # Wall time per family and when it ended (G1 budget); a family that
            # waits on `baseline_cli` includes the wait.
            name = family.__name__.rpartition(".")[2]
            now = time.monotonic()
            print(
                f"served {catalog} {name}: {len(found)} cases in "
                f"{now - family_started:.1f} s (done at {now - started:.0f} s)",
                flush=True,
            )
        return cases

    try:
        # One server pair per catalog, started here: the pools are not thread-safe.
        pairs = {
            catalog: (
                baseline.client(
                    {
                        "REG_META_DB": str(originals[catalog]),
                        "REG_WEBAPP_STEWARD": catalog,
                        "REG_WEBAPP_STEWARDS_DIR": str(baseline_stewards),
                    }
                ),
                rust.client(
                    {
                        "REG_META_DB": str(derived[catalog]),
                        "REG_WEBAPP_STEWARD": catalog,
                    }
                ),
            )
            for catalog in sorted(originals)
        }
        # Every catalog's lanes run side by side: walked one after the other, the
        # served arm was G1's critical path.
        jobs = [(catalog, lane) for catalog in pairs for lane in LANES]
        with ThreadPoolExecutor(len(jobs)) as pool:
            found = pool.map(lambda job: run(*job), jobs)
            return [case for cases in found for case in cases]
    finally:
        baseline.close()
        rust.close()
