"""Shared pytest fixtures used by both `reg_meta` and `reg_meta_build` test
suites. Both conftests import these via the on-`sys.path` bare-name path
(see each conftest's `sys.path.insert`)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_scb_input
from reg_meta_build.db import build_db

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a small SQLite DB from synthetic CSV fixtures.

    Builds *with* slugs — `link_consumer_side_bindings` is strict
    slug-only post-γ, so `skip_slugs=True` would leave
    `register_version.slug` NULL and zero out lineage edges. Also seeds
    a minimal doc DB so query commands (search/get/resolve) pass the
    "docs not installed" guard.
    """
    input_dir = tmp_path_factory.mktemp("input")
    db_dir = tmp_path_factory.mktemp("db")
    slug_dir = tmp_path_factory.mktemp("slugs")

    write_scb_input(input_dir)
    _write_fixture_slug_dir(slug_dir)

    build_db(
        input_dir=input_dir,
        db_dir=db_dir,
        skip_classifications=True,
        slug_dir=slug_dir,
    )
    _build_stub_doc_db(db_dir, tmp_path_factory)

    return db_dir / "reg_meta.db"


def _write_fixture_slug_dir(slug_dir: Path) -> None:
    """Minimal slug TOML for the synthetic fixture: register + variant
    slugs for the two test registers. Version slugs auto-derive at build
    time from the `YYYY` registerversionnamn values, so no
    `[register_version]` entries are needed.

    `skip_classifications=True` in the fixture means the classification
    table stays empty, so the empty `classifications.toml` clears
    `populate_slugs`'s strict coverage check (no rows = no NULL slugs).
    """
    # §5.6 lineage default: OTHERREG's Kön (sourced from TESTREG) pins to
    # TESTREG's `individer` variant, so the e2e build materializes a
    # variable_state_lineage edge (asserted in test_build_db.py). Without the
    # pin the consumer would hit the single-variant fallback (TESTREG has only
    # `individer`), which is silent — the explicit pin exercises the curated path.
    (slug_dir / "scb.toml").write_text(
        '[lineage_defaults]\ntestreg = "individer"\n'
        '[register."1"]\nslug = "testreg"\n'
        '[register."2"]\nslug = "otherreg"\n'
        '[register_variant."1.10"]\nslug = "individer"\n'
        '[register_variant."2.20"]\nslug = "foretag"\n',
        encoding="utf-8",
    )
    (slug_dir / "classifications.toml").write_text("", encoding="utf-8")


def _build_stub_doc_db(db_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Write a minimally valid doc DB alongside the main DB.

    Query-command tests don't exercise doc-search behaviour — they just
    need *a* schema-compatible doc DB present so the presence guard lets
    them through. Doc-specific behaviour is tested in test_doc_commands.py.
    """
    from reg_meta_build.doc_db import build_doc_db

    docs_src = tmp_path_factory.mktemp("stub_docs")
    reg_dir = docs_src / "stub"
    reg_dir.mkdir()
    (reg_dir / "Stub.md").write_text(
        "---\nvariable: Stub\ndisplay_name: Stub\ntags:\n  - type/variable\n---\n\nStub body.\n",
        encoding="utf-8",
    )
    build_doc_db(docs_src, db_dir)


@pytest.fixture()
def db_conn(fixture_db: Path) -> Iterator[sqlite3.Connection]:
    """Read-only connection to the fixture database."""
    from reg_meta.db import open_db

    conn = open_db(fixture_db)
    yield conn
    conn.close()


@pytest.fixture()
def db_path(fixture_db: Path) -> str:
    """`--db` arg pointing to the fixture database directory."""
    return str(fixture_db.parent)
