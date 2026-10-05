"""`GET /api/docs/*` — the documentation library endpoints (#354).

`docs_db` fixture = catalog DB + a 2-doc LISA `reg_meta_docs.db` (present index).
`catalog_db` alone = no docs index (the degradation case). Covers search +
register scope, doc get (excerpt, never full body), the "mentioned in docs"
variable hook (fuzzy + coverage distinction), input gates, the docs-absent
degradation, and the ETag round-trip.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app

import reg_meta_build

DOCS_SOURCE = Path(__file__).parent / "docs_source"
_BUILD_DOCS = (
    "import sys; from pathlib import Path; "
    "from reg_meta_build.doc_db import build_doc_db; "
    "build_doc_db(Path(sys.argv[1]), Path(sys.argv[2]))"
)


@pytest.fixture
def client(docs_db):
    """App with the docs index present."""
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def client_built_docs(catalog_db, tmp_path):
    """App with a docs index built from the readable `docs_source/` markdown only.

    `build_doc_db` also reads the checkout's doc-source and related-document
    curation and its untracked PDF seed (paths beside the package). Building with
    a copy of the installed package under `<tmp>/runtime/src` leaves those absent,
    so the index holds exactly the docs in `docs_source/` in every checkout."""
    src = tmp_path / "runtime" / "src"
    shutil.copytree(
        Path(reg_meta_build.__file__).parent,
        src / "reg_meta_build",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    subprocess.run(
        [sys.executable, "-c", _BUILD_DOCS, str(DOCS_SOURCE), str(catalog_db.parent)],
        check=True,
        env={"PYTHONPATH": str(src)},
    )
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def client_no_docs(catalog_db):
    """App booted with a catalog DB but NO docs index (degradation case)."""
    with TestClient(create_app()) as c:
        yield c


# ── search ───────────────────────────────────────────────────────────────────


def test_docs_search_finds_doc(client):
    body = client.get("/api/docs/search", params={"q": "Könstillhörighet"}).json()
    assert body["ingested"] is True
    assert body["total_count"] >= 1
    hit = body["results"][0]
    assert hit["filename"] == "Kon.md"
    assert hit["register"] == "lisa"  # wire key is `register`, not `register_name`
    assert hit["source"] == "lisa-bakgrundsfakta-1990-2017"
    assert hit["snippet"]  # an excerpt, not full text
    # Kon carries the curated source→PDF link + title (#372).
    assert hit["source_url"].endswith("lisa-bakgrundsfakta-1990-2017.pdf")
    assert hit["source_title"] == "LISA bakgrundsfakta 1990-2017"


def test_docs_search_uncurated_source_has_null_url(client):
    # SyssStat's source is uncurated → source_url/source_title stay None (#372).
    body = client.get("/api/docs/search", params={"q": "ställning"}).json()
    hit = next(r for r in body["results"] if r["filename"] == "Sysselsattning.md")
    assert hit["source_url"] is None
    assert hit["source_title"] is None


def test_docs_search_register_scope(client):
    inside = client.get(
        "/api/docs/search", params={"q": "individ", "register": "lisa"}
    ).json()
    assert inside["total_count"] >= 1
    # A register with no ingested docs → empty, but still ingested=True.
    outside = client.get(
        "/api/docs/search", params={"q": "individ", "register": "rams"}
    ).json()
    assert outside["ingested"] is True
    assert outside["total_count"] == 0


def test_docs_search_fts_operators_safe(client):
    for q in ['foo"bar', "AND OR", "kon*", "(a b)", ":", "-kon"]:
        r = client.get("/api/docs/search", params={"q": q})
        assert r.status_code == 200, f"{q!r} -> {r.status_code}"


def test_docs_search_too_long_422(client):
    assert client.get("/api/docs/search", params={"q": "x" * 201}).status_code == 422


def test_docs_search_nul_422(client):
    assert client.get("/api/docs/search", params={"q": "a\x00b"}).status_code == 422


# ── doc get ──────────────────────────────────────────────────────────────────


def test_doc_get_by_variable(client):
    body = client.get("/api/docs/doc/Kon").json()
    assert body["kind"] == "doc"
    assert body["variable"] == "Kon"
    assert body["register"] == "lisa"
    assert body["source"] == "lisa-bakgrundsfakta-1990-2017"
    # The resolved SCB-PDF link + human title (#372 curated map).
    assert body["source_url"].endswith("lisa-bakgrundsfakta-1990-2017.pdf")
    assert body["source_title"] == "LISA bakgrundsfakta 1990-2017"
    assert body["excerpt"]
    # Never the full converted body — only metadata/excerpt fields are exposed.
    assert "body" not in body


def test_doc_get_by_filename(client):
    assert client.get("/api/docs/doc/Kon.md").json()["variable"] == "Kon"


def test_doc_get_not_found_404(client):
    assert client.get("/api/docs/doc/DoesNotExist").status_code == 404


# ── "mentioned in documentation" variable hook ───────────────────────────────


def test_for_variable_fuzzy_and_coverage(client):
    body = client.get(
        "/api/docs/for-variable", params={"q": "Kon", "register": "lisa"}
    ).json()
    assert body["kind"] == "doc-mentions"
    assert body["ingested"] is True
    assert body["register_ingested"] is True
    assert body["total_count"] >= 1
    assert all(r["fuzzy"] is True for r in body["results"])


def test_for_variable_register_not_ingested(client):
    # A register with no ingested docs reads as "no docs ingested for this
    # register" (register_ingested False), NOT "variable undocumented".
    body = client.get(
        "/api/docs/for-variable", params={"q": "syss", "register": "rams"}
    ).json()
    assert body["ingested"] is True
    assert body["register_ingested"] is False


# ── docs-absent degradation ──────────────────────────────────────────────────


def test_search_degrades_when_docs_absent(client_no_docs):
    body = client_no_docs.get("/api/docs/search", params={"q": "kon"}).json()
    assert body["ingested"] is False
    assert body["total_count"] == 0
    assert body["results"] == []


def test_doc_get_404_when_docs_absent(client_no_docs):
    r = client_no_docs.get("/api/docs/doc/Kon")
    assert r.status_code == 404
    assert "not ingested" in r.json()["detail"]


def test_for_variable_degrades_when_docs_absent(client_no_docs):
    body = client_no_docs.get(
        "/api/docs/for-variable", params={"q": "kon", "register": "lisa"}
    ).json()
    assert body["ingested"] is False
    assert body["register_ingested"] is False


# ── related-document PDFs ───────────────────────────────────────────────────


def test_related_documents_list_returns_metadata_without_blob(client):
    body = client.get("/api/docs/related/lisa").json()
    assert body == {
        "kind": "related-documents",
        "ingested": True,
        "register": "lisa",
        "documents": [
            {
                "title": "LISA register documentation",
                "filename": "lisa_related.pdf",
                "source_url": "https://www.scb.se/lisa-related",
                "license": "CC BY 4.0",
                "fetched": "2026-06-01",
                "sha256": body["documents"][0]["sha256"],
                "byte_size": len(b"%PDF-1.4\n% related document fixture\n%%EOF\n"),
            }
        ],
    }
    assert "content" not in body["documents"][0]


def test_related_documents_list_degrades_when_docs_absent(client_no_docs):
    body = client_no_docs.get("/api/docs/related/lisa").json()
    assert body == {
        "kind": "related-documents",
        "ingested": False,
        "register": "lisa",
        "documents": [],
    }


def test_related_document_file_serves_pdf_bytes(client):
    r = client.get("/api/docs/file/lisa/lisa_related.pdf")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["content-disposition"].startswith(
        'inline; filename="lisa_related.pdf"'
    )
    assert r.content == b"%PDF-1.4\n% related document fixture\n%%EOF\n"


def test_related_document_file_404_when_docs_absent(client_no_docs):
    r = client_no_docs.get("/api/docs/file/lisa/lisa_related.pdf")
    assert r.status_code == 404
    assert "not ingested" in r.json()["detail"]


def test_related_document_file_404_for_unknown_filename(client):
    r = client.get("/api/docs/file/lisa/missing.pdf")
    assert r.status_code == 404
    assert "no related document" in r.json()["detail"]


def test_related_document_register_slug_gate(client):
    assert client.get("/api/docs/related/not-a-register!").status_code == 422


# ── ETag + excerpt unit ──────────────────────────────────────────────────────


def test_docs_search_etag_roundtrip(client):
    first = client.get("/api/docs/search", params={"q": "kon"})
    assert first.status_code == 200
    etag = first.headers["etag"]
    second = client.get(
        "/api/docs/search", params={"q": "kon"}, headers={"If-None-Match": etag}
    )
    assert second.status_code == 304


def _source_prose(name: str) -> str:
    """The body of a plain-prose source doc: the text after its frontmatter."""
    text = (DOCS_SOURCE / "lisa" / name).read_text(encoding="utf-8")
    return text.split("\n---\n", 1)[1].strip()


def test_doc_excerpt_short_body_verbatim(client_built_docs):
    """A short doc body is served whole as its excerpt."""
    body = client_built_docs.get("/api/docs/doc/Kort").json()
    assert body["excerpt"] == "Kort beskrivning av variabeln."


def test_doc_excerpt_long_body_truncated_at_500(client_built_docs):
    """A body over 500 chars is cut to its first 500 chars, right-stripped, plus an
    ellipsis — never the full text."""
    prose = _source_prose("LoneInk.md")
    assert len(prose) > 500
    body = client_built_docs.get("/api/docs/doc/LoneInk").json()
    assert body["excerpt"] == prose[:500].rstrip() + "…"


def test_doc_excerpt_null_when_clean_body_empty(client_built_docs):
    """A doc whose body is only a markdown table (stripped from the searchable text)
    has no excerpt."""
    body = client_built_docs.get("/api/docs/doc/Tabell").json()
    assert body["filename"] == "Tabell.md"
    assert body["excerpt"] is None
