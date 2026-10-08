"""``<type>-page``: ``search`` with each ``type`` against the baseline's group of that
type, for the search-eval corpus terms and the edge terms, page by page to the same
depth, each arm following its own cursor. Items map to ``shape.SearchHit``; ``more``
is whether the page continues."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from conformance.differential.cases import EDGE_TERMS, eval_terms
from conformance.differential.served.common import get

# The baseline caps a group at 50; two pages of ten cover a continuation.
PAGE_LIMIT = 10
PAGES = 2
TYPES = (
    "register",
    "variable",
    "classification",
    "classification_code",
    "register_value",
)
# The `shape.SearchHit` fields a baseline row of these types keeps as they are.
SHAPES = {
    "register": ("fqid", "name", "purpose"),
    "classification": ("fqid", "short_name", "name", "terminal_fqid"),
    "classification_succession": (
        "fqid",
        "short_name",
        "name",
        "matched_count",
        "editions",
    ),
}
# Requests in flight per server pair; each search takes ~0.4 s on the baseline.
PARALLEL = 16


def _hit(item: dict) -> dict:
    """A baseline search row as ``shape.SearchHit``."""
    kind = item["type"]
    if kind == "code":
        return {
            **{k: item[k] for k in ("type", "code", "label", "code_system")},
            **{k: item[k] for k in ("variable_count", "classification_count")},
            "variables": [
                {"fqid": v["fqid"], "name": v["name"], "register_name": v["register"]}
                for v in item["variables"]
            ],
            "classifications": item["classifications"],
        }
    if kind in SHAPES:
        return {"type": kind, **{k: item[k] for k in SHAPES[kind]}}
    if kind == "group":
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


def _pages(client, job: tuple, baseline: bool) -> list:
    scope, kind, term = job
    params = {"q": term, "type": kind, "limit": PAGE_LIMIT, "scope": scope}
    pages = []
    for _ in range(PAGES):
        answer = get(client, "/api/search", params)
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


def cases(base, cand, catalog, scopes, originals) -> list[tuple[str, dict, dict]]:
    terms = list(dict.fromkeys([*eval_terms(), *EDGE_TERMS]))
    # The default scope is one of the named ones, so pages run per name. Untyped
    # pages are not compared: the baseline has no single ranked list (decision 17);
    # the `api` corpus pins them.
    work = [
        (scope, kind, term)
        for scope in scopes
        if scope is not None
        for kind in TYPES
        for term in terms
    ]
    with ThreadPoolExecutor(PARALLEL) as pool:
        # Each job's two arms side by side, so both servers stay busy.
        runs = [(c, job, c is base) for job in work for c in (base, cand)]
        pages = list(pool.map(_pages, *zip(*runs, strict=True)))
    return [
        (f"{scope}/{kind}-page/{terms.index(term)}", base_pages, cand_pages)
        for (scope, kind, term), base_pages, cand_pages in zip(
            work, pages[::2], pages[1::2], strict=True
        )
    ]
