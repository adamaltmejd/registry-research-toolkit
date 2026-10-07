"""G1 for the operations ``reg-meta serve`` implements.

The Rust server serves each derived copy; the baseline commit's ``reg_webapp``, from
the baseline environment, serves the original artifact and is the oracle. Each case
maps the baseline's responses onto the operation's shape, so the two arms compare as
JSON in the CLI cases' result form (``exit``, ``stdout``, ``stderr``).

- ``context``: ``data`` and ``meta.scope`` against the baseline's ``/api/context``
  plus ``/api/stats`` in the same scope, for the default scope and each available
  scope.
"""

from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

from conformance.differential.cache import REPO_ROOT
from conformance.http_cases import ServerPool

if TYPE_CHECKING:
    from pathlib import Path

# The production rate limit (30 writes per minute) does not bind GETs; this matches
# the conformance runner's FastAPI template.
BASELINE_APP = (
    "import sys, uvicorn; from reg_webapp.app import create_app; "
    "uvicorn.run(create_app(rate_limit_per_minute=1000), port=int(sys.argv[1]), "
    "log_level='warning')"
)


def _result(value) -> dict:
    return {
        "exit": 0,
        "stdout": json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True),
        "stderr": "",
        "seconds": 0.0,
    }


def _get(client, path: str, scope: str | None) -> dict:
    response = client.get(path, params={"scope": scope} if scope else {})
    return {"status": response.status_code, "body": response.json()}


def context_cases(
    baseline_python: Path,
    baseline_stewards: Path,
    server: Path,
    dirs: dict[str, Path],
    derived: dict[str, Path],
    log_dir: Path,
) -> list[tuple[str, dict, dict]]:
    """``(case id, baseline result, checkout result)`` for every context case."""
    log_dir.mkdir(parents=True, exist_ok=True)
    baseline = ServerPool(
        shlex.join([str(baseline_python), "-I", "-c", BASELINE_APP, "{port}"]),
        log_dir,
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
        log_dir,
    )
    cases = []
    try:
        for catalog in sorted(dirs):
            base = baseline.client(
                {
                    "REG_META_DB": str(dirs[catalog]),
                    "REG_WEBAPP_STEWARD": catalog,
                    "REG_WEBAPP_STEWARDS_DIR": str(baseline_stewards),
                }
            )
            cand = rust.client(
                {"REG_META_DB": str(derived[catalog]), "REG_WEBAPP_STEWARD": catalog}
            )
            scopes = [None, "reference"] + (["holdings"] if catalog != "global" else [])
            for scope in scopes:
                context = _get(base, "/api/context", scope)
                stats = _get(base, "/api/stats", scope)
                if context["status"] == stats["status"] == 200:
                    ctx = context["body"]
                    expected = {
                        "data": {
                            "steward": {
                                k: ctx["steward"][k]
                                for k in ("id", "name", "long_name")
                            },
                            "schema_version": ctx["reg_meta"]["schema_version"],
                            "import_date": ctx["reg_meta"]["import_date"],
                            "period_span": ctx["steward"]["catalog_period_span"],
                            "reg_meta_version": ctx["webapp"]["reg_meta_version"],
                            "sizes": stats["body"],
                        },
                        "scope": scope or ctx["reg_meta"]["default_scope"],
                    }
                else:
                    expected = {"status": [context["status"], stats["status"]]}
                answer = _get(cand, "/api/context", scope)
                actual = (
                    {
                        "data": answer["body"]["data"],
                        "scope": answer["body"]["meta"]["scope"],
                    }
                    if answer["status"] == 200
                    else {"status": answer["status"], "body": answer["body"]}
                )
                cases.append(
                    (
                        f"{catalog}/{scope or 'default'}/context",
                        _result(expected),
                        _result(actual),
                    )
                )
    finally:
        baseline.close()
        rust.close()
    return cases
