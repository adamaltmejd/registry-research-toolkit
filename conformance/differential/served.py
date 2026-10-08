"""G1 for the operations ``reg-meta serve`` implements.

The Rust server serves each derived copy; the baseline commit's ``reg_webapp``, from
the baseline environment, serves the original artifact and is the oracle. Each case
maps the baseline's responses onto the operation's shape, so the two arms compare as
JSON in the CLI cases' result form (``exit``, ``stdout``, ``stderr``).

- ``context``: ``data`` and ``meta.scope`` against the baseline's ``/api/context``
  plus ``/api/stats`` in the same scope, for the default scope and each available
  scope.
- ``variable-page``: ``search`` with ``type=variable`` against the baseline's
  variables group, for the search-eval corpus terms and the edge terms, page by page
  to the same depth, each arm following its own cursor. Items map to
  ``shape.SearchHit``; ``more`` is whether the page continues.
"""

from __future__ import annotations

import json
import shlex
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING

from conformance.differential.cache import REPO_ROOT
from conformance.differential.cases import EDGE_TERMS, eval_terms
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
# The baseline caps a group at 50; two pages of ten cover a continuation.
PAGE_LIMIT = 10
PAGES = 2
# Requests in flight per server pair; each search takes ~0.4 s on the baseline.
PARALLEL = 8


def _result(value) -> dict:
    return {
        "exit": 0,
        "stdout": json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True),
        "stderr": "",
        "seconds": 0.0,
    }


def _get(client, path: str, params: dict) -> dict:
    response = client.get(path, params={k: v for k, v in params.items() if v})
    return {"status": response.status_code, "body": response.json()}


def _hit(item: dict) -> dict:
    """A baseline variables-group row as ``shape.SearchHit``."""
    if item["type"] == "group":
        return {
            "type": "group",
            "kind": item["kind"],
            "key": item["group_key"],
            "label": item["group_label"],
            "register_name": item["register"],
            "matched_count": item["matched_count"],
            "members": [
                {k: m[k] for k in ("fqid", "name", "delivery_column")}
                for m in item["members"]
            ],
        }
    return {
        "type": item["type"],
        "fqid": item["fqid"],
        "name": item["name"],
        "register_name": item["register"],
        "definition": item["definition"],
        "operational_definition": item["operational_definition"],
        "delivery_column_names": item["delivery_column_names"],
    }


def _pages(client, term: str, scope: str | None, *, baseline: bool) -> list:
    params = {"q": term, "type": "variable", "limit": PAGE_LIMIT, "scope": scope}
    pages = []
    for _ in range(PAGES):
        answer = _get(client, "/api/search", params)
        if answer["status"] != 200:
            pages.append({"status": answer["status"]})
            break
        body = answer["body"]
        if baseline:
            (group,) = body["groups"]
            items, cursor = [_hit(i) for i in group["results"]], group["next_cursor"]
        else:
            items, cursor = body["data"]["items"], body["data"]["next_cursor"]
        pages.append({"items": items, "more": cursor is not None})
        if cursor is None:
            break
        params["cursor"] = cursor
    return pages


def _page_pair(base, cand, job: tuple) -> tuple:
    scope, _, term = job
    return (
        job,
        _pages(base, term, scope, baseline=True),
        _pages(cand, term, scope, baseline=False),
    )


def _context(base, cand, scope: str | None) -> tuple[dict, dict]:
    context = _get(base, "/api/context", {"scope": scope})
    stats = _get(base, "/api/stats", {"scope": scope})
    if context["status"] == stats["status"] == 200:
        ctx = context["body"]
        expected = {
            "data": {
                "steward": {k: ctx["steward"][k] for k in ("id", "name", "long_name")},
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
    answer = _get(cand, "/api/context", {"scope": scope})
    actual = (
        {"data": answer["body"]["data"], "scope": answer["body"]["meta"]["scope"]}
        if answer["status"] == 200
        else {"status": answer["status"], "body": answer["body"]}
    )
    return expected, actual


def served_cases(
    baseline_python: Path,
    baseline_stewards: Path,
    server: Path,
    dirs: dict[str, Path],
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
    terms = list(dict.fromkeys([*eval_terms(), *EDGE_TERMS]))
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
                prefix = f"{catalog}/{scope or 'default'}"
                expected, actual = _context(base, cand, scope)
                cases.append((f"{prefix}/context", _result(expected), _result(actual)))
            # The default scope is one of the named ones, so pages run per name.
            named = [s for s in scopes if s is not None]
            work = [(scope, i, term) for scope in named for i, term in enumerate(terms)]
            with ThreadPoolExecutor(PARALLEL) as pool:
                pages = pool.map(partial(_page_pair, base, cand), work)
                for (scope, i, _), expected, actual in pages:
                    cases.append(
                        (
                            f"{catalog}/{scope}/variable-page/{i}",
                            _result(expected),
                            _result(actual),
                        )
                    )
    finally:
        baseline.close()
        rust.close()
    return cases
