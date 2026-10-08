"""The docs operations, in the reference scope (the baseline's docs routes and
commands take no scope). Global only for the per-document cases: the catalogs share
one docs database.

Against the baseline webapp's docs routes:

- ``docs_get`` per document of the docs database, by the identifier the CLI arm's
  ``docs-get`` cases use, against ``/api/docs/doc/{identifier}``. ``data`` without
  ``body`` (the webapp serves an excerpt only) against the baseline's ``DocDetail``
  without its ``kind`` tag.
- ``docs_search`` with ``q`` and ``register`` against ``/api/docs/for-variable``:
  each document's variable in its register's FQID, and one register without
  documents. ``total`` and ``register_ingested`` against ``total_count`` and
  ``register_ingested``, the items against ``results`` without ``fuzzy``.
- ``docs_related`` per FQID of a register with documents, against
  ``/api/docs/related/{register}``'s ``documents``.
- Each related document's download, by its FQID, against
  ``/api/docs/file/{register}/{filename}``: status, the headers both set and the
  SHA-256 of the bytes.

Against the CLI arm's baseline results, reused by case id:

- ``docs-search/<i>``: ``docs_search?q=<term>`` (the CLI's 20 results) against
  ``docs search``'s results without ``type`` and ``fts_rank``, and its total.
- ``docs-list/<register>``: every page of ``docs_search?register=<fqid>`` against
  ``docs list --register``'s rows and total.
- ``docs-get/<filename>``: ``docs_get``'s ``data`` without ``excerpt`` against ``docs
  get`` without ``body_clean``.

The bare ``docs list`` (a summary) has no ``docs_search`` equivalent.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from conformance.differential.cases import EDGE_TERMS, eval_terms
from conformance.differential.served.common import get

REFERENCE = {"scope": "reference"}
HEADERS = (
    "content-type",
    "content-length",
    "content-disposition",
    "x-content-type-options",
    "cache-control",
)
# The CLI's and the webapp's default page size.
LIMIT = 20
LIST_KEYS = ("filename", "variable", "display_name", "tags")


def _connect(directory: Path, name: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{directory / name}?mode=ro&immutable=1", uri=True)


def _data(answer: dict, mapped) -> dict:
    """A 200's mapped body, or the status alone (errors have no common shape)."""
    if answer["status"] != 200:
        return {"status": answer["status"]}
    return {"status": 200, "data": mapped(answer["body"])}


def _without(row: dict, *keys: str) -> dict:
    return {k: v for k, v in row.items() if k not in keys}


def _download(client, path: str, params: dict) -> dict:
    response = client.get(path, params=params)
    return {
        "status": response.status_code,
        "headers": {name: response.headers.get(name) for name in HEADERS},
        "sha256": hashlib.sha256(response.content).hexdigest(),
    }


def _all_pages(client, params: dict) -> dict:
    """Every page of a ``docs_search``: its items and total, or the status."""
    items: list[dict] = []
    cursor = None
    while True:
        page = {**params, **REFERENCE, "limit": 200, "cursor": cursor}
        answer = get(client, "/api/docs/search", page)
        if answer["status"] != 200:
            return {"status": answer["status"]}
        data = answer["body"]["data"]
        items += data["items"]
        if (cursor := data["next_cursor"]) is None:
            return {"items": items, "total": data["total"]}


def _cli(result: dict, mapped) -> object:
    """A CLI baseline result's mapped JSON, or its exit code."""
    if result["exit"] != 0:
        return {"exit": result["exit"]}
    return mapped(json.loads(result["stdout"]))


def _search_page(answer: dict) -> dict:
    """``docs_search``'s page in the CLI's ``docs search`` form."""
    if answer["status"] != 200:
        return {"status": answer["status"]}
    data = answer["body"]["data"]
    return {"total": data["total"], "results": data["items"]}


def _cli_cases(
    cand, providers, identifiers, baseline_cli
) -> list[tuple[str, dict, dict]]:
    terms = [*eval_terms(), *EDGE_TERMS]
    out = []
    for case_id, base_cli in sorted(baseline_cli.result().items()):
        catalog, _, command, *key = case_id.split("/")
        if catalog != "global" or command not in {
            "docs-search",
            "docs-list",
            "docs-get",
        }:
            continue
        name = "/".join(key)
        if command == "docs-search" and key:
            expected = _cli(
                base_cli,
                lambda body: {
                    "total": body["total_count"],
                    "results": [
                        _without(r, "type", "fts_rank") for r in body["results"]
                    ],
                },
            )
            actual = _search_page(
                get(
                    cand,
                    "/api/docs/search",
                    {"q": terms[int(name)], "limit": LIMIT, **REFERENCE},
                )
            )
            out.append((f"reference/docs_search-cli/{name}", expected, actual))
        elif command == "docs-list" and key:
            expected = _cli(
                base_cli,
                lambda body: {
                    "total": body["total_count"],
                    "items": body["results"],
                },
            )
            for provider in providers.get(name, []):
                found = _all_pages(cand, {"register": f"{provider}/{name}"})
                actual = (
                    {
                        "total": found["total"],
                        "items": [{k: r[k] for k in LIST_KEYS} for r in found["items"]],
                    }
                    if "items" in found
                    else found
                )
                out.append(
                    (f"reference/docs_list-cli/{provider}/{name}", expected, actual)
                )
        elif command == "docs-get":
            expected = _cli(base_cli, lambda body: _without(body, "body_clean"))
            identifier = quote(identifiers[name], safe="")
            answer = get(cand, f"/api/docs/doc/{identifier}", REFERENCE)
            actual = (
                _without(answer["body"]["data"], "excerpt")
                if answer["status"] == 200
                else {"status": answer["status"]}
            )
            out.append((f"reference/docs_get-cli/{name}", expected, actual))
    return out


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, dict, dict]]:
    with closing(_connect(originals, "reg_meta_docs.db")) as conn:
        documents = conn.execute(
            "SELECT filename, variable, register FROM doc ORDER BY doc_id"
        ).fetchall()
        registers = [
            row[0]
            for row in conn.execute(
                "SELECT register FROM doc UNION SELECT register FROM related_document "
                "ORDER BY 1"
            )
        ]
        files = conn.execute(
            "SELECT register, filename FROM related_document ORDER BY id"
        ).fetchall()
    with closing(_connect(originals, "reg_meta.db")) as conn:
        providers = {
            register: [
                row[0]
                for row in conn.execute(
                    "SELECT p.slug FROM register r JOIN provider p USING(provider_id) "
                    "WHERE r.slug = ? ORDER BY 1",
                    (register,),
                )
            ]
            for register in registers
        }
        # The first register without documents: register_ingested false.
        undocumented = conn.execute(
            "SELECT p.slug, r.slug FROM register r JOIN provider p USING(provider_id) "
            f"WHERE r.slug NOT IN ({','.join('?' * len(registers))}) "
            "ORDER BY p.slug, r.slug LIMIT 1",
            registers,
        ).fetchone()
    out = []
    if catalog == "global":
        for filename, variable, _ in documents:
            identifier = quote(variable or Path(filename).stem, safe="")
            path = f"/api/docs/doc/{identifier}"
            expected = _data(
                get(base, path),
                lambda body: _without(body, "kind"),
            )
            actual = _data(
                get(cand, path, REFERENCE),
                lambda body: _without(body["data"], "body"),
            )
            out.append((f"reference/docs_get/{filename}", expected, actual))
        mentions = [
            (variable, register, provider)
            for _, variable, register in documents
            if variable
            for provider in providers.get(register, [])
        ]
        if undocumented:
            provider, register = undocumented
            mentions.append(("Kon", register, provider))
        for q, register, provider in mentions:
            expected = _data(
                get(
                    base,
                    "/api/docs/for-variable",
                    {"q": q, "register": register, "limit": LIMIT},
                ),
                lambda body: {
                    "total": body["total_count"],
                    "register_ingested": body["register_ingested"],
                    "items": [_without(r, "fuzzy") for r in body["results"]],
                },
            )
            actual = _data(
                get(
                    cand,
                    "/api/docs/search",
                    {
                        "q": q,
                        "register": f"{provider}/{register}",
                        "limit": LIMIT,
                        **REFERENCE,
                    },
                ),
                lambda body: {
                    "total": body["data"]["total"],
                    "register_ingested": body["data"]["register_ingested"],
                    "items": body["data"]["items"],
                },
            )
            out.append(
                (
                    f"reference/docs_search-for-variable/{provider}/{register}/{q}",
                    expected,
                    actual,
                )
            )
        identifiers = {
            filename: variable or Path(filename).stem
            for filename, variable, _ in documents
        }
        out += _cli_cases(cand, providers, identifiers, baseline_cli)
    for register in registers:
        for provider in providers[register]:
            expected = _data(
                get(base, f"/api/docs/related/{register}"),
                lambda body: body["documents"],
            )
            actual = _data(
                get(cand, f"/api/docs/related/{provider}/{register}", REFERENCE),
                lambda body: body["data"],
            )
            out.append(
                (f"reference/docs_related/{provider}/{register}", expected, actual)
            )
    for register, filename in files:
        name = quote(filename, safe="")
        for provider in providers[register]:
            expected = _download(base, f"/api/docs/file/{register}/{name}", {})
            actual = _download(
                cand, f"/api/docs/file/{provider}/{register}/{name}", REFERENCE
            )
            out.append(
                (
                    f"reference/docs_file/{provider}/{register}/{filename}",
                    expected,
                    actual,
                )
            )
    return out
