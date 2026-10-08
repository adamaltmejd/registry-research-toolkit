"""The docs operations, in the reference scope (the baseline's docs routes take no
scope), against the baseline webapp's docs routes:

- ``docs_get`` per document of the docs database, by the identifier the CLI arm's
  ``docs-get`` cases use, against ``/api/docs/doc/{identifier}``. ``data`` without
  ``body`` (the webapp serves an excerpt only) against the baseline's ``DocDetail``
  without its ``kind`` tag. Global only: the catalogs share one docs database. The
  ``body`` is pinned by the ``api`` corpus until 3b.6 compares it with the CLI
  baseline's ``docs get``.
- ``docs_related`` per FQID of a register with documents, against
  ``/api/docs/related/{register}``'s ``documents``.
- Each related document's download, by its FQID, against
  ``/api/docs/file/{register}/{filename}``: status, the headers both set and the
  SHA-256 of the bytes.
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from conformance.differential.served.common import get

REFERENCE = {"scope": "reference"}
HEADERS = (
    "content-type",
    "content-length",
    "content-disposition",
    "x-content-type-options",
    "cache-control",
)


def _connect(directory: Path, name: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{directory / name}?mode=ro&immutable=1", uri=True)


def _data(answer: dict, mapped) -> dict:
    """A 200's mapped body, or the status alone (errors have no common shape)."""
    if answer["status"] != 200:
        return {"status": answer["status"]}
    return {"status": 200, "data": mapped(answer["body"])}


def _download(client, path: str, params: dict) -> dict:
    response = client.get(path, params=params)
    return {
        "status": response.status_code,
        "headers": {name: response.headers.get(name) for name in HEADERS},
        "sha256": hashlib.sha256(response.content).hexdigest(),
    }


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, dict, dict]]:
    with closing(_connect(originals, "reg_meta_docs.db")) as conn:
        documents = conn.execute(
            "SELECT filename, variable FROM doc ORDER BY doc_id"
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
    out = []
    if catalog == "global":
        for filename, variable in documents:
            identifier = quote(variable or Path(filename).stem, safe="")
            path = f"/api/docs/doc/{identifier}"
            expected = _data(
                get(base, path),
                lambda body: {k: v for k, v in body.items() if k != "kind"},
            )
            actual = _data(
                get(cand, path, REFERENCE),
                lambda body: {k: v for k, v in body["data"].items() if k != "body"},
            )
            out.append((f"reference/docs_get/{filename}", expected, actual))
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
