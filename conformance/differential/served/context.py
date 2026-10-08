"""``context``: ``data`` and ``meta.scope`` against the baseline's ``/api/context`` plus
``/api/stats`` in the same scope, for the default scope and each available scope."""

from __future__ import annotations

from conformance.differential.served.common import get


def _context(base, cand, scope: str | None) -> tuple[dict, dict]:
    context = get(base, "/api/context", {"scope": scope})
    stats = get(base, "/api/stats", {"scope": scope})
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
    answer = get(cand, "/api/context", {"scope": scope})
    actual = (
        {"data": answer["body"]["data"], "scope": answer["body"]["meta"]["scope"]}
        if answer["status"] == 200
        else {"status": answer["status"], "body": answer["body"]}
    )
    return expected, actual


def cases(base, cand, catalog, scopes, reference) -> list[tuple[str, dict, dict]]:
    return [
        (f"{scope or 'default'}/context", *_context(base, cand, scope))
        for scope in scopes
    ]
