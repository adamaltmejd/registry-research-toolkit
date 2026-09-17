"""Shared pytest fixtures used by both `reg_meta` and `reg_meta_build` test
suites. Both conftests import these via the on-`sys.path` bare-name path
(see each conftest's `sys.path.insert`)."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from reg_meta.db import register_py_lower

if TYPE_CHECKING:
    from collections.abc import Iterator


def connect_built_db(db: Path | str) -> sqlite3.Connection:
    """Open a writable connection to a built fixture DB the way production
    validation does: a raw connect plus the `py_lower` UDF that
    `reg_meta.db.open_db` (and thus `validate_built_db`) registers. Tests that
    drive `_check_*` validators or `queries.*` against a built DB and then mutate
    it can't use the read-only `open_db` path, so they go through here to get the
    same UDF surface (refs #853)."""
    conn = sqlite3.connect(str(db))
    register_py_lower(conn)
    return conn


def fail_replace_onto(monkeypatch: pytest.MonkeyPatch, live: Path) -> None:
    """Make the final publication replacement onto `live` fail (Y-52).

    `db.publish_db` installs a staged DB with one `Path.replace`, which
    delegates to `os.replace` — so patching that, keyed on the destination,
    injects the replacement failure and leaves every other replace alone.
    The raised message is `injected replacement failure`.
    """
    real_replace = os.replace
    live_target = str(live.resolve())

    def _fail_on_live(src, dst, **kwargs):
        if os.fspath(dst) == live_target:
            raise OSError("injected replacement failure")
        return real_replace(src, dst, **kwargs)

    monkeypatch.setattr(os, "replace", _fail_on_live)


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Seed a small explicit catalog, independent of source-resolution rules.

    Reader and structural-validator tests need stable graph identities, not a
    source build. Pipeline tests exercise preparation and the resolved writer.
    """
    from contextlib import closing

    from reg_meta.db import SCHEMA_VERSION
    from reg_meta_build.db import DDL, _populate_fts, seed_providers

    db_dir = tmp_path_factory.mktemp("db")
    output = db_dir / "reg_meta.db"
    with closing(sqlite3.connect(output)) as conn:
        conn.executescript(DDL)
        seed_providers(conn)
        conn.executescript(
            Path(__file__).with_name("_catalog_fixture.sql").read_text(encoding="utf-8")
        )
        conn.executemany(
            "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
            (
                ("schema_version", SCHEMA_VERSION),
                ("import_date", "2020-01-01T00:00:00Z"),
                ("row_counts", json.dumps({"variables": 8, "states": 9})),
            ),
        )
        _populate_fts(conn)
        conn.commit()
        conn.execute("VACUUM")
    _build_stub_doc_db(db_dir, tmp_path_factory)

    return output


def _write_fixture_slug_dir(slug_dir: Path) -> None:
    """Minimal slug TOML for the synthetic fixture: register + variant
    slugs for the two test registers. Version slugs auto-derive at build
    time from the `YYYY` registerversionnamn values, so no
    `[register_version]` entries are needed.

    `skip_classifications=True` in the fixture means the classification
    table stays empty, so the empty `classifications.toml` clears
    `populate_slugs`'s strict coverage check (no rows = no NULL slugs).
    """
    (slug_dir / "scb.toml").write_text(
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


# ── build-driven test helpers (test_codelivery_build /
#    test_coalesce_connectivity) — one definition, shared across suites ───────

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


def errata_version(name: str) -> str:
    """A `[[version]]` entry for `scb_errata.toml` (Y-114) naming an edition of
    the fixture's TESTREG/individer variant."""
    return (
        "[[version]]\n"
        'register = "scb/testreg"\n'
        'variant = "individer"\n'
        f'name = "{name}"\n'
        f'evidence = "the steward holds the {name} delivery"\n'
        'noted = "2026-09-11"\n'
    )


def errata_column(column: str, *versions: str, **fields: object) -> str:
    """A `[[column]]` entry for `scb_errata.toml` (Y-116): `column` is delivered
    on TESTREG/individer but SCB's export documents it NOWHERE, so the entry
    mints the variable. With no `versions`, it claims every edition
    (`all_versions = true`); `fields` overrides or adds any key."""
    entry: dict[str, object] = {
        "register": "scb/testreg",
        "variant": "individer",
        "column": column,
        "name": f"{column} name",
        "definition": f"{column} definition",
        **({"versions": list(versions)} if versions else {"all_versions": True}),
        "source": "steward-holdings",
        "evidence": f"the steward holds {column}",
        "noted": "2026-09-12",
        **fields,
    }
    return "[[column]]\n" + "".join(
        f"{k} = {json.dumps(v, ensure_ascii=False)}\n" for k, v in entry.items()
    )


def errata_delivered(column: str, *versions: str) -> str:
    """A `[[delivered]]` entry for `scb_errata.toml` (Y-114): `column` was
    delivered in `versions` of TESTREG/individer but SCB's export omits the row."""
    listed = ", ".join(f'"{v}"' for v in versions)
    return (
        "[[delivered]]\n"
        'register = "scb/testreg"\n'
        'variant = "individer"\n'
        f'column = "{column}"\n'
        f"versions = [{listed}]\n"
        f'evidence = "the steward holds {column} for those years"\n'
        'noted = "2026-09-11"\n'
    )
