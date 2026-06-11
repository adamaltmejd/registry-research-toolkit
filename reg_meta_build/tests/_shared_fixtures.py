"""Shared pytest fixtures used by both `reg_meta` and `reg_meta_build` test
suites. Both conftests import these via the on-`sys.path` bare-name path
(see each conftest's `sys.path.insert`)."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_scb_input
from reg_meta_build.db import build_db

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(scope="session", autouse=True)
def _no_repo_curation() -> Iterator[None]:
    """Synthetic test builds run with EMPTY curation maps — the documented
    contract for the maintainer TOMLs (codelivery / fold_overrides /
    column_merges). A checkout-run `build_db` would otherwise load the REPO
    TOMLs, which are keyed on real SCB source ids that can collide with the
    fixture register ids (RTB IS register 2 — a real `column_merges.toml`
    entry for it binds the fixture's OTHERREG and fails every build). Session-
    scoped + autouse so it lands before the session-scoped `fixture_db` build;
    tests that exercise a curation surface monkeypatch their own file path on
    top (function-scoped, applied after, undone per test)."""
    import reg_meta_build.codelivery as _cd
    import reg_meta_build.column_merges as _cm
    import reg_meta_build.concept_groups as _cg
    import reg_meta_build.db as _db
    import reg_meta_build.fold_overrides as _fo

    mp = pytest.MonkeyPatch()
    mp.setattr(_cd, "repo_codelivery_path", lambda: None)
    mp.setattr(_cm, "repo_column_merges_path", lambda: None)
    mp.setattr(_fo, "repo_fold_overrides_path", lambda: None)
    # concept_groups.toml references real registers (scb/lisa) by SLUG, and the
    # materializer fails fast on a dangling reference — which every synthetic
    # fixture build would be. `db.materialize` imported the symbol directly, so
    # patch it there too.
    mp.setattr(_cg, "repo_concept_groups_path", lambda: None)
    mp.setattr(_db, "repo_concept_groups_path", lambda: None)
    yield
    mp.undo()


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a small SQLite DB from synthetic CSV fixtures.

    Builds *with* slugs — `link_variable_state_lineage` (A2.4) keys on the
    curated variant/variable slugs, so `skip_slugs=True` would leave them
    NULL and zero out lineage edges. Also seeds a minimal doc DB so query
    commands (search/get/resolve) pass the "docs not installed" guard.
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
    # Lineage default: OTHERREG's Kön (sourced from TESTREG) pins to
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


# ── build-driven test helpers (test_codelivery_build / test_fold_overrides /
#    test_column_merges) — one definition, three suites ─────────────────────

# Clearly-distinct codings for one column: pairwise-disjoint codes (symmetric
# diff 6 > _COSMETIC_MAX_SYM=2 → not cosmetic) and DIFFERENT version labels
# (→ no same-label-drift, and arbitrary labels rank equal under
# _label_resolution_rank → no freshness tiebreak). Plain "YYYY" register
# versions → equal authority/recency. So nothing in the co-delivery cascade
# resolves two of these on one column except SUPERSESSION (distinct intro year).
CODING_A = [("11", "Alpha ett"), ("12", "Alpha två"), ("13", "Alpha tre")]
CODING_B = [("21", "Beta ett"), ("22", "Beta två"), ("23", "Beta tre")]
CODING_C = [("31", "Gamma ett"), ("32", "Gamma två"), ("33", "Gamma tre")]


def vm_rows(cvid: int, version: str, codes: list[tuple[str, str]]) -> list[str]:
    """Vardemangder rows for one cvid: [version, niva, kod, benämning, CVID, ItemId].
    `niva="1"` is a non-historical grain (matches the default fixture); ItemId is
    left empty (the importer accepts it, and no ValidDates row means always-valid).
    The value_set_id is derived from the (kod, benämning) set, so two cvids sharing
    identical codes fold into ONE value set; the `version` becomes the state's
    `value_set_version_label`."""
    from _csv_fixtures import PIPE

    return [PIPE.join([version, "1", kod, ben, str(cvid), ""]) for kod, ben in codes]


def build_with_rows(
    tmp_path: Path, ri_extra: list[str], vm_extra: list[str]
) -> sqlite3.Connection:
    """Run a real SCB build with the standard fixture plus the extra rows; return
    a connection to the built DB. Never touches the live DB (tmp only)."""
    from _csv_fixtures import REGISTERINFORMATION_ROWS, VARDEMANGDER_ROWS

    input_dir = tmp_path / "input"
    db_dir = tmp_path / "db"
    slug_dir = tmp_path / "slugs"
    for d in (input_dir, db_dir, slug_dir):
        d.mkdir()
    write_scb_input(
        input_dir,
        registerinformation_rows=REGISTERINFORMATION_ROWS + ri_extra,
        vardemangder_rows=VARDEMANGDER_ROWS + vm_extra,
    )
    _write_fixture_slug_dir(slug_dir)
    build_db(
        input_dir=input_dir, db_dir=db_dir, skip_classifications=True, slug_dir=slug_dir
    )
    return sqlite3.connect(db_dir / "reg_meta.db")
