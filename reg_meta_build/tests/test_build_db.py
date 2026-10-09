"""Artifact schema seeds and the edition-year grammar."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.edition_bounds import extract_year

if TYPE_CHECKING:
    from pathlib import Path


class TestBuildDb:
    def test_provider_seed(self, db_conn: sqlite3.Connection):
        # provider_id values are stable across releases — downstream pins
        # against them (PROVIDER_ID_SCB = 1, PROVIDER_ID_SOS = 2,
        # PROVIDER_ID_FOHM = 3, PROVIDER_ID_FK = 4). Every seeded provider is
        # present regardless of
        # which adapters this build ran.
        rows = db_conn.execute(
            "SELECT provider_id, slug, name FROM provider ORDER BY provider_id"
        ).fetchall()
        assert [(r["provider_id"], r["slug"]) for r in rows] == [
            (1, "scb"),
            (2, "sos"),
            (3, "fohm"),
            (4, "fk"),
            (5, "lakemedelsverket"),
            (6, "pliktverket"),
            (7, "riksarkivet"),
            (8, "umu"),
        ]

    def test_seed_providers_idempotent(self, tmp_path: Path):
        # `seed_providers` is a public helper that may run more than once
        # against the same DB; a plain INSERT would raise IntegrityError on the
        # fixed PKs the second time.
        import sqlite3 as _sqlite3

        from reg_meta_build.db import DDL, seed_providers

        conn = _sqlite3.connect(str(tmp_path / "idem.db"))
        conn.row_factory = _sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        seed_providers(conn)  # must not raise
        rows = conn.execute(
            "SELECT provider_id, slug FROM provider ORDER BY provider_id"
        ).fetchall()
        assert [(r["provider_id"], r["slug"]) for r in rows] == [
            (1, "scb"),
            (2, "sos"),
            (3, "fohm"),
            (4, "fk"),
            (5, "lakemedelsverket"),
            (6, "pliktverket"),
            (7, "riksarkivet"),
            (8, "umu"),
        ]
        conn.close()

    def test_seed_providers_rejects_mismatch(self, tmp_path: Path):
        # A pre-existing row with the wrong slug means the DB came from
        # somewhere else (corruption, partial migration). Failing fast is
        # safer than silently overwriting via UPSERT.
        import sqlite3 as _sqlite3

        from reg_meta_build.db import DDL, seed_providers

        conn = _sqlite3.connect(str(tmp_path / "mismatch.db"))
        conn.row_factory = _sqlite3.Row
        conn.executescript(DDL)
        conn.execute(
            "INSERT INTO provider (provider_id, slug, name) VALUES (1, 'wrong', 'X')"
        )
        with pytest.raises(RuntimeError, match="already present"):
            seed_providers(conn)
        conn.close()


# ---------------------------------------------------------------------------
# Year projection
# ---------------------------------------------------------------------------


class TestExtractYear:
    """``extract_year`` matches a 1900-2099 year as a standalone 4-digit
    token; values outside that range or embedded in longer digit runs return
    None (yearless fallback for projection)."""

    def test_extracts_year_from_lisa_2018(self):
        assert extract_year("LISA 2018") == 2018

    def test_returns_none_for_out_of_range(self):
        assert extract_year("Komvux 1234-poäng") is None
        assert extract_year("1234") is None

    def test_returns_none_for_yearless_names(self):
        assert extract_year("Person-År") is None
        assert extract_year("Födelseland") is None

    def test_returns_none_for_empty(self):
        assert extract_year("") is None

    def test_rejects_year_inside_longer_digit_run(self):
        # 19999 is not a year; the regex must not match the prefix 1999.
        assert extract_year("v19999") is None


class TestMemberHashCheckConstraint:
    """The DDL declares ``CHECK (length(member_hash) = 32)`` — non-32-byte
    blobs must be rejected."""

    def test_short_blob_rejected(self, fixture_db: Path):
        conn = sqlite3.connect(fixture_db)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO value_set (member_hash) VALUES (?)", (b"\x00" * 31,)
            )
        conn.close()
