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
    seed_classifications_toml,
    seed_provider_toml,
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


# ---------------------------------------------------------------------------
# Gap coverage from PR review
# ---------------------------------------------------------------------------


class TestVariableOverridesNotYetSupported:
    """`[variable]` slug overrides are forward-compat metadata. `populate_slugs`
    must raise rather than silently drop them so a maintainer's commit takes
    effect — or fails loudly until step 1e wires up consumer-side bindings."""

    def _make_db(self):
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        conn.commit()
        return conn

    def test_variable_slug_override_raises(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "v"\n'
            '[variable."1.44"]\nslug = "kon"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_db()
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_variable_override_unsupported"

    def test_variable_metadata_only_is_accepted(self, tmp_path: Path):
        # No slug — just deprecation / replaced_by / same_as metadata. Parsed
        # and round-tripped but never applied; should not raise.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "v"\n'
            '[variable."1.44"]\ndeprecated = true\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {"register": 1, "register_variant": 1, "classification": 1}


class TestSeedEmitsValidToml:
    """seed_*_toml must produce TOML that round-trips through tomllib even
    when DB strings contain quote, backslash, or Unicode oddities."""

    def test_round_trip_with_quote_and_backslash(self, tmp_path: Path):
        conn = build_slugged_db(
            register=('Foo "Bar" \\Baz', "foo-bar", 1, 1),
            variant=('name with "quotes"', "v", 10),
        )
        body = seed_provider_toml(conn, "scb")
        # tomllib.loads will raise on malformed escapes.
        import tomllib

        parsed = tomllib.loads(body)
        assert "register" in parsed
        assert "1" in parsed["register"]

    def test_classifications_round_trip_with_unicode(self, tmp_path: Path):
        conn = build_slugged_db(
            classification=(
                'KÅL "2020"',
                "Svensk utbildning",
                "2020",
                "sun",
            ),
        )
        body = seed_classifications_toml(conn)
        import tomllib

        parsed = tomllib.loads(body)
        assert 'KÅL "2020"' in parsed["classification"]


class TestEntryParseIds:
    """`_parse_register_id` / `_parse_variant_id` are hit by populate when the
    TOML key isn't the expected integer / integer-pair shape."""

    def _db(self):
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        return conn

    def test_register_key_not_integer(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."abc"]\nslug = "lisa"\n')
        _write(d / "classifications.toml", "")
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(self._db(), d, strict=False)
        assert exc.value.code == "slug_toml_invalid"
        assert "RegisterId" in exc.value.message

    def test_variant_key_wrong_shape(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n[register_variant."1"]\nslug = "v"\n',
        )
        _write(d / "classifications.toml", "")
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(self._db(), d, strict=False)
        assert exc.value.code == "slug_toml_invalid"
        assert "RegisterId" in exc.value.message

    def test_variant_key_non_integer_halves(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n[register_variant."1.x"]\nslug = "v"\n',
        )
        _write(d / "classifications.toml", "")
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(self._db(), d, strict=False)
        assert exc.value.code == "slug_toml_invalid"


class TestDisplayGroupTyped:
    """`display_group` must be a string when set."""

    def test_non_string_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."1.10"]\nslug = "v"\ndisplay_group = 42\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "display_group" in exc.value.message


class TestSeedPopulateRoundTrip:
    """Seed a TOML from a fully-slugged DB, replay it via populate_slugs into
    a freshly-cleared DB, and assert the slug columns match. Locks in the
    promise that seed output is directly consumable by populate_slugs."""

    def test_round_trip(self, tmp_path: Path):
        # 1) Seed from a fully slugged DB.
        seeded = build_slugged_db()
        out_dir = tmp_path / "slugs"
        seed_all(seeded, out_dir)
        seeded.close()

        # 2) Drop NULL placeholder for classification version since the seed
        # writes "TODO" when version is missing — our fixture sets version.
        # Replay into a cleared DB.
        target = build_slugged_db()
        target.execute("UPDATE register SET slug = NULL")
        target.execute("UPDATE register_variant SET slug = NULL")
        target.execute("UPDATE classification SET slug = NULL")
        target.commit()

        counts = populate_slugs(target, out_dir, strict=True)
        assert counts == {"register": 1, "register_variant": 1, "classification": 1}
        assert (
            target.execute(
                "SELECT slug FROM register WHERE register_id = 1"
            ).fetchone()[0]
            == "lisa"
        )
        # Auto-derived slugs differ from the curated "sun" — the seed is a
        # starter, not a faithful round-trip of existing slugs. Just assert
        # populate_slugs filled the column with whatever the seed proposed.
        cls_slug = target.execute(
            "SELECT slug FROM classification WHERE short_name = 'SUN2020'"
        ).fetchone()[0]
        assert cls_slug and cls_slug != "TODO"


class TestRepoSlugDir:
    """`repo_slug_dir()` returns the live directory in a repo checkout."""

    def test_repo_layout_resolves(self):
        from regmeta.fqid_slugs import repo_slug_dir

        result = repo_slug_dir()
        assert result is not None
        assert result.is_dir()
        assert (result / "scb.toml").is_file()


class TestPrecheckCli:
    """CLI exit codes mirror the per-test contract: added rows are not fatal
    in the maintainer's interactive view, but they must fail CI so the
    snapshot stays current."""

    def _seed_layout(self, tmp_path: Path) -> tuple[Path, Path]:
        """Build a DB with one provider + register + variant, slug TOMLs that
        cover both, and a `db` arg compatible with `regmeta --db`."""
        from regmeta.db import DDL, seed_providers

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        db_path = db_dir / "regmeta.db"
        conn = sqlite3.connect(db_path)
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, registernamn, slug) "
            "VALUES (1, 1, 'LISA', 'lisa')"
        )
        conn.execute(
            "INSERT INTO register_variant (regvar_id, register_id, slug, "
            "registervariantnamn) VALUES (10, 1, 'individer-15plus', 'Individer 15+')"
        )
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug) "
            "VALUES ('SUN2020', 'Svensk utbildning', '2020', 'sun')"
        )
        conn.execute("INSERT INTO import_manifest VALUES ('schema_version', '3.1.0')")
        conn.commit()
        conn.close()

        slug_dir = tmp_path / "slugs"
        slug_dir.mkdir()
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        _write(
            slug_dir / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        return db_dir, slug_dir

    def test_added_entries_exit_non_zero(self, tmp_path: Path):
        from regmeta.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # Snapshot is empty — the three entries above show as `added`.
        write_snapshot(
            slug_dir / SNAPSHOT_FILENAME,
            {
                kind: {}
                for kind in (
                    "register",
                    "register_variant",
                    "variable",
                    "classification",
                )
            },
        )
        exit_code = run(
            [
                "--db",
                str(db_dir),
                "maintain",
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
            ]
        )
        assert exit_code != 0

    def test_update_snapshot_clears_added(self, tmp_path: Path):
        from regmeta.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        write_snapshot(
            slug_dir / SNAPSHOT_FILENAME,
            {
                kind: {}
                for kind in (
                    "register",
                    "register_variant",
                    "variable",
                    "classification",
                )
            },
        )
        exit_code = run(
            [
                "--db",
                str(db_dir),
                "maintain",
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        # All TOMLs valid, all live rows covered — `--update-snapshot` skips
        # the diff so the run exits clean.
        assert exit_code == 0
        # Snapshot file was rewritten.
        contents = (slug_dir / SNAPSHOT_FILENAME).read_text()
        assert "lisa" in contents

    def test_update_snapshot_still_exits_on_missing(self, tmp_path: Path):
        """`--update-snapshot` is review-only for snapshot drift but still
        exits non-zero on real problems (parse errors / missing slugs) so a
        broken state can't be snapshot-frozen."""
        from regmeta.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # Drop the register entry — DB still has register_id=1, so precheck
        # surfaces a missing slug.
        _write(
            slug_dir / "scb.toml",
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        exit_code = run(
            [
                "--db",
                str(db_dir),
                "maintain",
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0

    def test_update_snapshot_refuses_on_parse_error(self, tmp_path: Path):
        """A corrupted TOML must not silently blow away the baseline snapshot
        — `precheck_slugs` truncates `entries` at the first parse error and
        the partial set would otherwise wipe genuine prior entries."""
        from regmeta.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        snapshot_before = (
            '{"classification":{"SUN2020|2020":"sun"},'
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        # Corrupt the scb.toml so load_slug_dir raises.
        _write(slug_dir / "scb.toml", '[register."1"]\nslug = "Bad_Slug"\n')

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "maintain",
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0
        # Snapshot file is unchanged — the prior baseline survives.
        assert (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8") == (
            snapshot_before
        )


class TestSameAs:
    """`same_as` is parsed and round-tripped but deferred to step 1e. The
    parser still enforces shape and kind constraints."""

    def test_accepts_valid_inline_tables(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.137"]\n'
            'slug = "civilstand"\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "civilstand-legacy" }]\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].same_as == (
            {
                "provider": "scb",
                "register": "lisa",
                "variable_slug": "civilstand-legacy",
            },
        )

    def test_rejected_on_register(self, tmp_path: Path):
        # §5.3 same_as is only valid on variable / classification.
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\n'
            'same_as = [{ provider = "scb", register = "rtb" }]\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "same_as" in exc.value.message

    def test_empty_value_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\nsame_as = [{ provider = "" }]\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_non_array_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\nsame_as = "not a list"\n',
        )
        with pytest.raises(RegmetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"


class TestCanonicalIntegerKeys:
    """`"1.10"` and `"1.010"` must not alias the same DB row. Reject the
    leading-zero form at populate-time so the maintainer gets a loud error
    instead of silent collisions."""

    def _db_with_register_1_10(self):
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        conn.commit()
        return conn

    def test_leading_zero_register_key_rejected(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."01"]\nslug = "lisa"\n')
        _write(d / "classifications.toml", "")
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(self._db_with_register_1_10(), d, strict=False)
        assert exc.value.code == "slug_toml_invalid"
        assert "canonical" in exc.value.message

    def test_leading_zero_variant_half_rejected(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n[register_variant."1.010"]\nslug = "v"\n',
        )
        _write(d / "classifications.toml", "")
        with pytest.raises(RegmetaError) as exc:
            populate_slugs(self._db_with_register_1_10(), d, strict=False)
        assert exc.value.code == "slug_toml_invalid"


class TestSeedEmptyDb:
    """Empty seed output is well-formed and self-explanatory."""

    def test_provider_with_no_registers(self, tmp_path: Path):
        # Build a DB whose `scb` provider has no register rows.
        from regmeta.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        # Only sos has a register; scb is empty.
        conn.execute(
            "INSERT INTO register (register_id, provider_id, registernamn) "
            "VALUES (1, 2, 'PAR')"
        )
        body = seed_provider_toml(conn, "scb")
        assert "no registers found" in body

    def test_classifications_table_empty(self, tmp_path: Path):
        from regmeta.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        body = seed_classifications_toml(conn)
        assert "no classifications" in body


class TestSnapshotCorrupt:
    """A snapshot file that fails to parse raises `slug_snapshot_unreadable`
    so the maintainer can't silently progress against garbage."""

    def test_corrupt_json_raises(self, tmp_path: Path):
        path = tmp_path / "snap.json"
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(RegmetaError) as exc:
            read_snapshot(path)
        assert exc.value.code == "slug_snapshot_unreadable"
