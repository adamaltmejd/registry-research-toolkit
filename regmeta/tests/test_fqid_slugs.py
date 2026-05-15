"""Tests for slug TOML loading, validation, population, and seed/precheck."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from regmeta.errors import RegmetaError
from regmeta.fqid_slugs import (
    SNAPSHOT_FILENAME,
    SlugEntry,
    diff_snapshot,
    load_classifications_toml,
    load_provider_toml,
    load_slug_dir,
    populate_slugs,
    precheck_slugs,
    read_snapshot,
    seed_all,
    snapshot_payload,
    write_snapshot,
)

from _slugged_db import build_slugged_db


# ---------------------------------------------------------------------------
# Loader / validation
# ---------------------------------------------------------------------------


def _write(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


class TestProviderToml:
    def test_minimal_register(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\n',
        )
        entries = load_provider_toml(path)
        assert entries == [
            SlugEntry(
                kind="register",
                source_id="34",
                slug="lisa",
                provider="scb",
            )
        ]

    def test_variant_with_display_group(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\n'
            'slug = "individer-15plus"\n'
            'display_group = "Individer"\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].kind == "register_variant"
        assert entries[0].slug == "individer-15plus"
        assert entries[0].display_group == "Individer"

    def test_variable_override(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].kind == "variable"
        assert entries[0].slug == "kon"

    def test_variable_without_slug_keeps_metadata(self, tmp_path: Path):
        # A variable entry may omit slug if it only carries deprecation / same_as
        # metadata; the auto-derived slug from kolumnnamn is authoritative.
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.99"]\ndeprecated = true\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].slug is None
        assert entries[0].deprecated is True

    def test_invalid_slug_grammar(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "Bad_Slug"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_reserved_slug_class_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "class"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_default_slug_rejected_outside_variant(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "_default"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_default_slug_allowed_for_variant(self, tmp_path: Path):
        # synthetic variant emission lives at the build layer; the TOML allows
        # the slug as a curated override too.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.0"]\nslug = "_default"\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].slug == "_default"

    def test_period_shaped_slug_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "2020"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_unknown_field_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\nbogus = "x"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "bogus" in exc.value.message

    def test_duplicate_slug_within_kind(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\n[register."35"]\nslug = "lisa"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_replaced_by_chain_acyclic(self, tmp_path: Path):
        # b -> a (legal): editing a typo on `40` produces `40b`; both rows
        # remain in the TOML and a one-hop chain is followed at resolve time.
        path = _write(
            tmp_path / "scb.toml",
            '[register."40"]\nslug = "rams-typo"\nreplaced_by = "40b"\n'
            '[register."40b"]\nslug = "rams"\n',
        )
        entries = load_provider_toml(path)
        assert {e.source_id for e in entries} == {"40", "40b"}

    def test_replaced_by_dangling_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."40"]\nslug = "rams"\nreplaced_by = "40b"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert "not declared" in exc.value.message

    def test_replaced_by_cycle_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."40"]\nslug = "a"\nreplaced_by = "41"\n'
            '[register."41"]\nslug = "b"\nreplaced_by = "40"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert "cycle" in exc.value.message


class TestClassificationsToml:
    def test_minimal(self, tmp_path: Path):
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        entries = load_classifications_toml(path)
        assert entries == [
            SlugEntry(
                kind="classification",
                source_id="SUN2020",
                slug="sun",
                version="2020",
            )
        ]

    def test_version_required(self, tmp_path: Path):
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_classifications_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_classification_version_allows_period_shaped(self, tmp_path: Path):
        # `2020` is period-shaped; slug grammar would reject it elsewhere, but
        # the classification *version* explicitly allows it.
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        entries = load_classifications_toml(path)
        assert entries[0].version == "2020"

    def test_duplicate_slug_version_pair_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020A"]\nslug = "sun"\nversion = "2020"\n'
            '[classification."SUN2020B"]\nslug = "sun"\nversion = "2020"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_classifications_toml(path)
        assert exc.value.code == "slug_toml_invalid"


class TestLoadSlugDir:
    def test_empty_dir(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        assert load_slug_dir(d) == []

    def test_missing_dir(self, tmp_path: Path):
        with pytest.raises(RegmetaError) as exc:
            load_slug_dir(tmp_path / "missing")
        assert exc.value.code == "slug_dir_not_found"

    def test_loads_provider_and_classifications(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."34"]\nslug = "lisa"\n')
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        entries = load_slug_dir(d)
        kinds = sorted({e.kind for e in entries})
        assert kinds == ["classification", "register"]


# ---------------------------------------------------------------------------
# populate_slugs against a hand-curated DB
# ---------------------------------------------------------------------------


class TestPopulateSlugs:
    def _make_db(self) -> sqlite3.Connection:
        # The slugged-db fixture pre-populates slugs; we want the empty-slug
        # state to test population. Build the DB then clear the slug columns.
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        conn.commit()
        return conn

    def test_populates_register_and_variant(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n'
            'display_group = "Individer"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {"register": 1, "register_variant": 1, "classification": 1}
        assert (
            conn.execute("SELECT slug FROM register WHERE register_id = 1").fetchone()[
                0
            ]
            == "lisa"
        )
        row = conn.execute(
            "SELECT slug, display_group FROM register_variant WHERE regvar_id = 10"
        ).fetchone()
        assert (row["slug"], row["display_group"]) == ("individer-15plus", "Individer")
        assert (
            conn.execute(
                "SELECT slug FROM classification WHERE short_name = 'SUN2020'"
            ).fetchone()[0]
            == "sun"
        )

    def test_strict_fails_when_register_missing_slug(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_db()
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_missing_for_source_id"

    def test_non_strict_skips_coverage_check(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."1"]\nslug = "lisa"\n')
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_db()
        # Variant `1.10` has no slug entry; strict would fail, non-strict
        # populates whatever is available.
        counts = populate_slugs(conn, d, strict=False)
        assert counts["register"] == 1
        assert counts["register_variant"] == 0

    def test_deprecated_entry_with_no_live_row_is_silent(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register."999"]\nslug = "retired"\ndeprecated = true\n'
            '[register_variant."1.10"]\nslug = "v"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts["register"] == 1  # 999 is deprecated, skipped

    def test_unknown_source_id_fails(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register."999"]\nslug = "ghost"\n'
            '[register_variant."1.10"]\nslug = "v"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_db()
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_unknown_source_id"


# ---------------------------------------------------------------------------
# seed-slugs
# ---------------------------------------------------------------------------


class TestSeedSlugs:
    def test_writes_provider_and_classifications(self, tmp_path: Path):
        conn = build_slugged_db()
        out = tmp_path / "out"
        written = seed_all(conn, out)
        assert "scb.toml" in written
        assert "classifications.toml" in written
        scb_body = (out / "scb.toml").read_text()
        assert '[register."1"]' in scb_body
        assert 'slug = "lisa"' in scb_body
        assert '[register_variant."1.10"]' in scb_body
        cls_body = (out / "classifications.toml").read_text()
        assert '[classification."SUN2020"]' in cls_body
        assert 'version = "2020"' in cls_body


# ---------------------------------------------------------------------------
# precheck-slugs
# ---------------------------------------------------------------------------


class TestPrecheckSlugs:
    def test_clean_state(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert result.ok

    def test_missing_register_reported(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        _write(d / "classifications.toml", "")
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert not result.ok
        assert any(r[1] == "1" for r in result.missing_registers)
        assert any(c == "SUN2020" for c in result.missing_classifications)

    def test_parse_error_surfaces(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."1"]\nslug = "Bad_Slug"\n')
        _write(d / "classifications.toml", "")
        conn = build_slugged_db()
        result = precheck_slugs(conn, d)
        assert result.parse_errors


# ---------------------------------------------------------------------------
# Snapshot / immutability
# ---------------------------------------------------------------------------


class TestSnapshot:
    def test_diff_detects_add(self):
        prev = {
            "register": {},
            "register_variant": {},
            "variable": {},
            "classification": {},
        }
        cur = {
            "register": {"scb/1": "lisa"},
            "register_variant": {},
            "variable": {},
            "classification": {},
        }
        diff = diff_snapshot(prev, cur)
        assert diff["added"] == ["register/scb/1 = 'lisa'"]
        assert diff["removed"] == []
        assert diff["renamed"] == []

    def test_diff_detects_removal(self):
        prev = {
            "register": {"scb/1": "lisa"},
            "register_variant": {},
            "variable": {},
            "classification": {},
        }
        cur = {
            "register": {},
            "register_variant": {},
            "variable": {},
            "classification": {},
        }
        diff = diff_snapshot(prev, cur)
        assert "register/scb/1" in diff["removed"][0]

    def test_diff_detects_rename(self):
        prev = {
            "register": {"scb/1": "lisa"},
            "register_variant": {},
            "variable": {},
            "classification": {},
        }
        cur = {
            "register": {"scb/1": "lisa-renamed"},
            "register_variant": {},
            "variable": {},
            "classification": {},
        }
        diff = diff_snapshot(prev, cur)
        assert diff["renamed"]
        assert "'lisa'" in diff["renamed"][0]
        assert "'lisa-renamed'" in diff["renamed"][0]

    def test_snapshot_round_trip(self, tmp_path: Path):
        path = tmp_path / SNAPSHOT_FILENAME
        payload = {
            "classification": {"SUN2020|2020": "sun"},
            "register": {"scb/1": "lisa"},
            "register_variant": {"scb/1.10": "individer-15plus"},
            "variable": {},
        }
        write_snapshot(path, payload)
        loaded = read_snapshot(path)
        assert loaded == payload

    def test_snapshot_missing_file_returns_empty(self, tmp_path: Path):
        loaded = read_snapshot(tmp_path / "nope.json")
        assert loaded == {
            "classification": {},
            "register": {},
            "register_variant": {},
            "variable": {},
        }

    def test_snapshot_payload_skips_slugless_entries(self):
        entries = [
            SlugEntry(
                kind="variable",
                source_id="34.99",
                slug=None,
                provider="scb",
                deprecated=True,
            ),
            SlugEntry(
                kind="variable",
                source_id="34.4",
                slug="kon",
                provider="scb",
            ),
        ]
        payload = snapshot_payload(entries)
        assert payload["variable"] == {"scb/34.4": "kon"}
