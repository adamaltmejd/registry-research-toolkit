"""Tests for slug TOML loading, validation, population, and seed/precheck."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_version, build_slugged_db
from reg_meta.errors import RegMetaError

from reg_meta_build.fqid_slugs import (
    SNAPSHOT_FILENAME,
    SlugEntry,
    classify_default_candidate,
    diff_snapshot,
    format_default_slug_hints,
    iter_default_slug_candidates,
    load_classifications_toml,
    load_provider_toml,
    load_slug_dir,
    materialize_same_as_edges,
    populate_slugs,
    precheck_slugs,
    read_snapshot,
    seed_all,
    seed_classifications_toml,
    seed_provider_toml,
    snapshot_payload,
    write_snapshot,
)

if TYPE_CHECKING:
    from pathlib import Path

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
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_reserved_slug_class_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "class"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_default_slug_rejected_outside_variant(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "_default"\n',
        )
        with pytest.raises(RegMetaError) as exc:
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
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_unknown_field_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\nbogus = "x"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "bogus" in exc.value.message

    def test_duplicate_slug_within_kind(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\n[register."35"]\nslug = "lisa"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_variant_slug_repeats_across_registers(self, tmp_path: Path):
        # Variant slugs are scoped per parent register (FQID grammar
        # `<provider>/<register>/<variant>` already disambiguates them).
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.151"]\nslug = "individer"\n'
            '[register_variant."26.157"]\nslug = "individer"\n',
        )
        entries = load_provider_toml(path)
        assert {e.source_id for e in entries} == {"34.151", "26.157"}

    def test_variant_slug_collision_within_register_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.151"]\nslug = "individer"\n'
            '[register_variant."34.152"]\nslug = "individer"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "within register '34'" in exc.value.message

    def test_variable_slug_repeats_across_registers(self, tmp_path: Path):
        # Variable slugs are scoped per parent register for the same reason
        # variant slugs are.
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\n[variable."26.4"]\nslug = "kon"\n',
        )
        entries = load_provider_toml(path)
        assert {e.source_id for e in entries} == {"34.4", "26.4"}

    def test_variable_slug_collision_within_register_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\n[variable."34.5"]\nslug = "kon"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "within register '34'" in exc.value.message

    def test_replaced_by_chain_acyclic(self, tmp_path: Path):
        # Slug typo gets a `replaced_by` link to the new row; both rows stay
        # in the TOML and a one-hop chain resolves cleanly.
        path = _write(
            tmp_path / "scb.toml",
            '[register."40"]\nslug = "rams-typo"\nreplaced_by = "41"\n'
            '[register."41"]\nslug = "rams"\n',
        )
        entries = load_provider_toml(path)
        assert {e.source_id for e in entries} == {"40", "41"}

    def test_replaced_by_dangling_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."40"]\nslug = "rams"\nreplaced_by = "40b"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert "not declared" in exc.value.message

    def test_replaced_by_cycle_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."40"]\nslug = "a"\nreplaced_by = "41"\n'
            '[register."41"]\nslug = "b"\nreplaced_by = "40"\n',
        )
        with pytest.raises(RegMetaError) as exc:
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
        with pytest.raises(RegMetaError) as exc:
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

    @pytest.mark.parametrize(
        "bad_version",
        ["With Space", "slash/here", "UPPER", "_default", "class"],
    )
    def test_version_must_round_trip_through_fqid_grammar(
        self, tmp_path: Path, bad_version: str
    ):
        # `version` becomes the third segment of `class/<slug>/<version>`.
        # Anything that fails `validate_slug(..., allow_period=True)` must
        # fail at TOML load so a malformed value can't be frozen into the
        # snapshot and only blow up later at FQID emission.
        path = _write(
            tmp_path / "classifications.toml",
            f'[classification."SUN2020"]\nslug = "sun"\nversion = "{bad_version}"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_classifications_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "version" in exc.value.message

    def test_duplicate_slug_version_pair_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020A"]\nslug = "sun"\nversion = "2020"\n'
            '[classification."SUN2020B"]\nslug = "sun"\nversion = "2020"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_classifications_toml(path)
        assert exc.value.code == "slug_toml_invalid"


class TestLoadSlugDir:
    def test_empty_dir(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        assert load_slug_dir(d) == []

    def test_missing_dir(self, tmp_path: Path):
        with pytest.raises(RegMetaError) as exc:
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
        assert counts == {
            "register": 1,
            "register_variant": 1,
            "register_version": 0,
            "register_version_auto": 0,
            "classification": 1,
        }
        assert (
            conn.execute("SELECT slug FROM register WHERE register_id = 1").fetchone()[
                0
            ]
            == "lisa"
        )
        row = conn.execute(
            "SELECT slug, display_group FROM register_variant WHERE register_variant_id = 10"
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
        with pytest.raises(RegMetaError) as exc:
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
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_unknown_source_id"

    def _make_unperiodized_db(self) -> sqlite3.Connection:
        # Variant + unperiodized version (`Gymnasieintyg, ackumulerat` has no
        # parseable period). All slug columns cleared so populate_slugs can
        # run a clean auto-derive + curated pass.
        conn = build_slugged_db(
            version=("Gymnasieintyg, ackumulerat", None, 200),
        )
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        conn.commit()
        return conn

    def test_populates_register_version_from_toml(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n'
            '[register_version."1.10.200"]\nslug = "ackumulerat-register"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_unperiodized_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts["register_version"] == 1
        assert counts["register_version_auto"] == 0
        assert (
            conn.execute(
                "SELECT slug FROM register_version WHERE regver_id = 200"
            ).fetchone()[0]
            == "ackumulerat-register"
        )

    def test_strict_fails_when_unperiodized_version_missing_slug(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_unperiodized_db()
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_missing_for_source_id"

    def test_unknown_register_version_source_id_fails(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n'
            # Version 999 does not exist; sibling 200 has its own TOML so
            # the strict-mode coverage check doesn't fire first.
            '[register_version."1.10.200"]\nslug = "ackumulerat-register"\n'
            '[register_version."1.10.999"]\nslug = "ghost"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = self._make_unperiodized_db()
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_unknown_source_id"

    def test_curated_slug_overrides_auto_derived(self, tmp_path: Path):
        # Escape hatch documented at populate_slugs:722 — TOML override beats
        # the period regex when a maintainer needs to correct a wrong-year
        # extraction.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n'
            '[register_version."1.10.100"]\nslug = "2017"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = build_slugged_db()  # default version is ("LISA 2018", "2018", 100)
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE register_version SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        conn.commit()
        populate_slugs(conn, d, strict=True)
        assert (
            conn.execute(
                "SELECT slug FROM register_version WHERE regver_id = 100"
            ).fetchone()[0]
            == "2017"
        )

    def test_unique_constraint_blocks_sibling_slug_collision(self):
        # SQL-level UNIQUE(register_variant_id, slug) catches collisions that the
        # TOML-load `seen_slugs` check can't see — two auto-derived siblings
        # mapping to the same period, or a curated override clashing with
        # an auto-derived sibling.
        conn = build_slugged_db()  # regver 100 already carries slug "2018"
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO register_version "
                "(regver_id, register_variant_id, slug, registerversionnamn) "
                "VALUES (?, ?, ?, ?)",
                (101, 10, "2018", "LISA 2018 (dup)"),
            )

    def test_autoderive_collision_raises_reg_meta_error(self, tmp_path: Path):
        # If precheck is bypassed and two siblings auto-derive to the same
        # period, the raw sqlite IntegrityError from the second UPDATE is
        # wrapped as a RegMetaError pointing at `precheck-slugs`. Keeps
        # diagnostics consistent with the rest of the slug pipeline.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = build_slugged_db(version=("LISA 2018 huvudfil", None, 100))
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (?, ?, ?, ?)",
            (101, 10, None, "LISA 2018 tilläggsfil"),
        )
        with pytest.raises(RegMetaError) as excinfo:
            populate_slugs(conn, d, strict=False)
        assert excinfo.value.code == "slug_periodized_collision"
        assert "precheck-slugs" in excinfo.value.remediation


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

    def test_seeds_unperiodized_version_against_skip_slugs_build(self):
        # Regression for PR #94 Codex P2: seed_provider_toml used to filter
        # `WHERE rver.slug IS NOT NULL`, which omitted every unperiodized
        # version when bootstrapping from a `build-db --skip-slugs` DB
        # (slug column is still NULL across the board). The filter now runs
        # on the version name via derive_period, so the stub appears
        # regardless of slug-column state.
        conn = build_slugged_db(
            version=("Födelseland", None, 200),  # name has no period, slug NULL
        )
        body = seed_provider_toml(conn, "scb")
        assert '[register_version."1.10.200"]' in body
        assert 'slug = "TODO"' in body

    def test_omits_periodized_version_from_seed(self):
        # Periodized versions round-trip without any TOML curation — they
        # must not appear as stubs in the seed output. Source name is a bare
        # period token: any prefix would trigger the §5.3 residual flag and
        # legitimately force an emission.
        conn = build_slugged_db(version=("2018", "2018", 200))
        body = seed_provider_toml(conn, "scb")
        assert "[register_version." not in body

    def test_emits_curated_override_on_periodized_version(self):
        # A maintainer-pinned slug that doesn't match derive_period (e.g.
        # collision-resolution within a variant) must round-trip through
        # seed → TOML → populate, not get silently dropped on regen.
        conn = build_slugged_db(
            version=("Ankor och anklingar 1968-1997", "ankor-1968-1997", 200),
        )
        body = seed_provider_toml(conn, "scb")
        assert '[register_version."1.10.200"]' in body
        assert 'slug = "ankor-1968-1997"' in body
        # Audit comment carries the source registerversionnamn so the next
        # curator can verify any typo/abbreviation normalization (§5.3).
        assert "# 'Ankor och anklingar 1968-1997'" in body

    def test_emits_collision_annotation_against_sibling(self):
        # §5.3 rule 5: when the curated row's derive_period(name) matches a
        # sibling's effective slug, the comment names the sibling so a future
        # curator sees *why* the curated slug isn't the bare period. Mirrors
        # the real `104.840.5510 / 'Vårterminen 2013 - betyg' (vs 5177:VT2013)`
        # case from scb.toml.
        conn = build_slugged_db(
            # 5177 holds the canonical VT2013 (auto-derived from name).
            version=("Vårterminen 2013", None, 5177),
        )
        add_version(
            conn,
            regver_id=5510,
            register_variant_id=10,
            slug="betyg-vt2013",
            name="Vårterminen 2013 - betyg",
        )
        conn.commit()
        body = seed_provider_toml(conn, "scb")
        assert '[register_version."1.10.5510"]' in body
        assert 'slug = "betyg-vt2013"' in body
        assert "# 'Vårterminen 2013 - betyg' (vs 5177:VT2013)" in body
        # The claimant (5177) auto-derives to its slug — it's NOT a curated
        # override, so it should not itself appear as a seed stub.
        assert '[register_version."1.10.5177"]' not in body

    def test_collision_annotation_omitted_when_no_sibling_claims_period(self):
        # No sibling under the variant claims derive_period(name), so no
        # annotation. Guards against false-positive annotations on lone
        # descriptor rows (e.g. unperiodized aux tables).
        # Source name `1968-1997` (year range) leaves a non-alpha residual
        # `-1997` so the §5.3 rule 6 residual flag also stays quiet — keeps
        # this test focused on the collision-omitted axis.
        conn = build_slugged_db(
            version=("1968-1997", "ankor-1968-1997", 200),
        )
        body = seed_provider_toml(conn, "scb")
        assert "# '1968-1997'\n" in body
        assert "(vs " not in body
        assert "(residual:" not in body

    def test_residual_emits_stub_with_auto_derived_slug(self):
        # §5.3 rule 6: source name carries scope info beyond the period
        # (`Strandlinje, 2019` → derive_period extracts `2019`, drops
        # `Strandlinje`). seed-slugs refuses the round-trip skip so the
        # curator sees a stub. Default slug is the auto-derive value —
        # accepting as-is is the "acknowledge auto-derive" path; renaming
        # is the curate path.
        conn = build_slugged_db(version=("Strandlinje, 2019", None, 200))
        body = seed_provider_toml(conn, "scb")
        assert '[register_version."1.10.200"]' in body
        assert 'slug = "2019"' in body
        assert "# 'Strandlinje, 2019' (residual:" in body
        assert '(residual: "Strandlinje,")' in body

    def test_residual_skipped_when_only_connector_token_remains(self):
        # §5.3 rule 6 connector denylist: a residual of ONLY a 3-char Swedish
        # function word (`och`, `för`, `med`, `men`) is not scope-bearing — it
        # is a conjunction/preposition stranded by the period match. The row
        # round-trips through auto-derive without curation, so no stub is
        # emitted. Guards against false-positive curation work that the bare
        # `[^\W\d_]{3,}` length test would create.
        conn = build_slugged_db(version=("2019 och", "2019", 200))
        body = seed_provider_toml(conn, "scb")
        assert "[register_version." not in body

    def test_residual_normalizes_whitespace_around_period_match(self):
        # `Gifta 1996-1997` → derive_period matches `1996`, slicing leaves a
        # double-space artifact (`"Gifta "` + `" "` + `"-1997"`). Comment
        # readability matters since ~330 production rows are affected; the
        # residual collapses whitespace runs to a single space.
        conn = build_slugged_db(
            version=("Gifta 1996-1997", "gifta-1996-1997", 200),
        )
        body = seed_provider_toml(conn, "scb")
        assert '(residual: "Gifta -1997")' in body
        assert '(residual: "Gifta  -1997")' not in body

    def test_collision_and_residual_co_occur_in_fixed_order(self):
        # §5.3 rules 5 + 6 can both fire on the same row (a curated slug
        # whose name *also* leaks scope info). The spec promises a fixed
        # output order: `(vs ...)` first, then `(residual: ...)`. Production
        # data leans on this ordering, so pin it.
        conn = build_slugged_db(
            version=("Vårterminen 2013", None, 5177),
        )
        add_version(
            conn,
            regver_id=5510,
            register_variant_id=10,
            slug="betyg-vt2013",
            name="Vårterminen 2013 - betyg",
        )
        conn.commit()
        body = seed_provider_toml(conn, "scb")
        assert (
            "# 'Vårterminen 2013 - betyg' (vs 5177:VT2013) (residual: \"- betyg\")"
            in body
        )


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

    def test_stale_register_id_reported(self, tmp_path: Path):
        # TOML entry for register 999 has no live row; `populate_slugs` would
        # raise `slug_unknown_source_id` at build time. Precheck surfaces it
        # earlier so maintainers don't ship a TOML that breaks the next build.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n[register."999"]\nslug = "ghost"\n',
        )
        _write(d / "classifications.toml", "")
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert not result.ok
        assert ("scb", "999") in result.stale_registers
        assert ("scb", "1") not in result.stale_registers

    def test_stale_variant_id_reported(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer"\n'
            '[register_variant."1.999"]\nslug = "ghost"\n',
        )
        _write(d / "classifications.toml", "")
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert ("scb", "1.999") in result.stale_variants
        assert ("scb", "1.10") not in result.stale_variants

    def test_stale_classification_reported(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        _write(
            d / "classifications.toml",
            '[classification."GHOST"]\nslug = "ghost"\nversion = "2020"\n',
        )
        conn = build_slugged_db()
        result = precheck_slugs(conn, d)
        assert "GHOST" in result.stale_classifications

    def test_deprecated_entries_excluded_from_stale(self, tmp_path: Path):
        # Deprecated rows are allowed to outlive their DB row — that's the
        # whole point of `deprecated=true`. Stale-check must not flag them.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register."999"]\nslug = "old-lisa"\ndeprecated = true\n',
        )
        _write(d / "classifications.toml", "")
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert ("scb", "999") not in result.stale_registers

    def test_periodized_sibling_collision_flagged(self, tmp_path: Path):
        # Regression for Codex P1 on PR #94 (commit ffd07fe): two sibling
        # periodized rows both deriving to `2018` with no override would
        # pass precheck but fail mid-build on UNIQUE(register_variant_id, slug).
        # Precheck must catch this so the maintainer sees a clean diagnostic
        # instead of a raw SQLite IntegrityError.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = build_slugged_db(version=("LISA 2018 huvudfil", None, 100))
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (?, ?, ?, ?)",
            (101, 10, None, "LISA 2018 tilläggsfil"),
        )
        result = precheck_slugs(conn, d)
        assert not result.ok
        slugs = {(p, sid, slug) for (p, sid, _name, slug) in result.colliding_versions}
        assert ("scb", "1.10.100", "2018") in slugs
        assert ("scb", "1.10.101", "2018") in slugs

    def test_collision_resolved_by_curated_override(self, tmp_path: Path):
        # Disambiguating override on one sibling clears the collision —
        # precheck must mirror populate_slugs's override-then-derive order,
        # not naive auto-derive-only.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n'
            '[register_version."1.10.101"]\nslug = "tillagg-2018"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = build_slugged_db(version=("LISA 2018 huvudfil", None, 100))
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (?, ?, ?, ?)",
            (101, 10, None, "LISA 2018 tilläggsfil"),
        )
        result = precheck_slugs(conn, d)
        assert not result.colliding_versions

    def test_collision_when_override_clashes_with_auto_derived_sibling(
        self, tmp_path: Path
    ):
        # Override on regver 101 explicitly pins slug "2018", which then
        # collides with regver 100's auto-derived "2018". A naive precheck
        # that only checks for "both rows would auto-derive the same period"
        # would miss this; the would-be-slug grouping catches it.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n'
            '[register_version."1.10.101"]\nslug = "2018"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        conn = build_slugged_db(version=("LISA 2018", None, 100))
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, slug, registerversionnamn) "
            "VALUES (?, ?, ?, ?)",
            (101, 10, None, "Other 2019 file"),  # would auto-derive "2019"
        )
        result = precheck_slugs(conn, d)
        slugs = {(p, sid, slug) for (p, sid, _name, slug) in result.colliding_versions}
        assert ("scb", "1.10.100", "2018") in slugs
        assert ("scb", "1.10.101", "2018") in slugs


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
            "register_version": {"scb/1.10.100": "ackumulerat-register"},
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
            "register_version": {},
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
        with pytest.raises(RegMetaError) as exc:
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
        assert counts == {
            "register": 1,
            "register_variant": 1,
            "register_version": 0,
            "register_version_auto": 0,
            "classification": 1,
        }


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
        with pytest.raises(RegMetaError) as exc:
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
        with pytest.raises(RegMetaError) as exc:
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
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(self._db(), d, strict=False)
        assert exc.value.code == "slug_toml_invalid"


class TestDisplayGroupTyped:
    """`display_group` must be a string when set."""

    def test_non_string_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."1.10"]\nslug = "v"\ndisplay_group = 42\n',
        )
        with pytest.raises(RegMetaError) as exc:
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
        assert counts == {
            "register": 1,
            "register_variant": 1,
            "register_version": 0,
            "register_version_auto": 0,
            "classification": 1,
        }
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


class TestCuratedDefaultVariantRoundTrip:
    """A curated `slug = "_default"` on a real register_variant row must
    survive TOML → populate_slugs → seed_provider_toml → TOML cycles.

    Before §5.1 synthesis moved to FQID-resolve time, the seed emitter
    treated every `_default` row as build-synthesized and silently dropped
    it from the regenerated TOML, so a curator who committed the new TOML
    would discard their own entry."""

    def test_populate_then_seed_preserves_default(self, tmp_path: Path):
        # 1) Curate `_default` in TOML, populate into a fresh DB.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "sos.toml",
            '[register."5"]\nslug = "lss"\n'
            '[register_variant."5.50"]\nslug = "_default"\n',
        )
        _write(d / "classifications.toml", "")
        conn = build_slugged_db(
            register=("LSS", None, 5, 2),
            variant=("LSS", None, 50),
            version=None,
            variable=None,
            classification=None,
        )
        counts = populate_slugs(conn, d, strict=True)
        assert counts["register_variant"] == 1
        slug = conn.execute(
            "SELECT slug FROM register_variant WHERE register_variant_id = 50"
        ).fetchone()[0]
        assert slug == "_default"

        # 2) Re-seed from the now-populated DB. The curated `_default` must
        # appear in the regenerated TOML (regression guard for the trapdoor
        # that silently dropped it before).
        body = seed_provider_toml(conn, "sos")
        assert 'slug = "_default"' in body
        assert '[register_variant."5.50"]' in body


class TestRepoSlugDir:
    """`repo_slug_dir()` returns the live directory in a repo checkout."""

    def test_repo_layout_resolves(self):
        from reg_meta_build.fqid_slugs import repo_slug_dir

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
        cover both, and a `db` arg compatible with `reg_meta --db`."""
        from reg_meta_build.db import DDL, seed_providers

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        db_path = db_dir / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'LISA', 'lisa')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug, "
            "name) VALUES (10, 1, 'individer-15plus', 'Individer 15+')"
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
        from reg_meta_build.cli import run

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
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
            ]
        )
        assert exit_code != 0

    def test_update_snapshot_clears_added(self, tmp_path: Path):
        from reg_meta_build.cli import run

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
        from reg_meta_build.cli import run

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
        from reg_meta_build.cli import run

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
    """`same_as` parser shape and kind constraints. Edge materialization
    is covered separately under `TestMaterializeSameAsEdges`."""

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
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "same_as" in exc.value.message

    def test_empty_value_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\nsame_as = [{ provider = "" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_non_array_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\nsame_as = "not a list"\n',
        )
        with pytest.raises(RegMetaError) as exc:
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
        with pytest.raises(RegMetaError) as exc:
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
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(self._db_with_register_1_10(), d, strict=False)
        assert exc.value.code == "slug_toml_invalid"

    def test_malformed_id_rejected_at_load_not_populate(self, tmp_path: Path):
        # Source-ID shape is enforced at TOML load, so `precheck_slugs` and
        # other read-only commands surface the same error without needing a
        # `populate_slugs` call. Without this the precheck path would silently
        # skip the row.
        path = _write(tmp_path / "scb.toml", '[register."abc"]\nslug = "lisa"\n')
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "RegisterId" in exc.value.message


class TestSeedEmptyDb:
    """Empty seed output is well-formed and self-explanatory."""

    def test_provider_with_no_registers(self, tmp_path: Path):
        # Build a DB whose `scb` provider has no register rows.
        from reg_meta_build.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        # Only sos has a register; scb is empty.
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name) VALUES (1, 2, 'PAR')"
        )
        body = seed_provider_toml(conn, "scb")
        assert "no registers found" in body

    def test_classifications_table_empty(self, tmp_path: Path):
        from reg_meta_build.db import DDL, seed_providers

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
        with pytest.raises(RegMetaError) as exc:
            read_snapshot(path)
        assert exc.value.code == "slug_snapshot_unreadable"


class TestDeprecatedStrictType:
    """`deprecated` must be a TOML boolean. Truthy strings like
    `"false"` previously coerced to True via `bool(value)`, masking real
    source-ID drift because deprecated rows skip the missing-row check."""

    def test_string_false_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\ndeprecated = "false"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "deprecated" in exc.value.message

    def test_integer_zero_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\ndeprecated = 0\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_bare_true_accepted(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\ndeprecated = true\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].deprecated is True


class TestUnknownTopLevelTables:
    """Reject typo'd top-level tables (e.g. `[registers."34"]`) so a slug
    entry doesn't silently no-op."""

    def test_provider_unknown_top_level(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[registers."34"]\nslug = "lisa"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "registers" in exc.value.message

    def test_classifications_unknown_top_level(self, tmp_path: Path):
        path = _write(
            tmp_path / "classifications.toml",
            '[classifications."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_classifications_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "classifications" in exc.value.message


class TestSameAsKeyValidation:
    """`same_as` inline-table keys must come from a known set. A typo like
    `classifcation_slug` shouldn't silently round-trip as forward-compat
    metadata."""

    def test_variable_unknown_key_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "kon"\n'
            'same_as = [{ provider = "scb", typo_key = "x" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert "typo_key" in exc.value.message

    def test_classification_unknown_key_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n'
            'same_as = [{ provider = "scb", classifcation_slug = "sun-v1" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_classifications_toml(path)
        assert "classifcation_slug" in exc.value.message


class TestClassificationVersionMismatch:
    """A TOML version that disagrees with the DB row's version would
    snapshot a different FQID than the catalog emits at query time."""

    def test_mismatch_raises(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n[register_variant."1.10"]\nslug = "v"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "1996"\n',
        )
        # build_slugged_db's classification carries version = "2020".
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        conn.commit()
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_classification_version_mismatch"


class TestPrecheckCliGrowOnly:
    """--update-snapshot must refuse to bless a removal or rename — otherwise
    the §5.4 grow-only contract is bypassable in one command."""

    def _seed_layout(self, tmp_path: Path) -> tuple[Path, Path]:
        """Reuse the layout from TestPrecheckCli but local to keep dependencies
        explicit."""
        from reg_meta_build.db import DDL, seed_providers

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        db_path = db_dir / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'LISA', 'lisa')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug, "
            "name) VALUES (10, 1, 'individer-15plus', 'Individer 15+')"
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
        return db_dir, slug_dir

    def test_rename_refused_even_with_update(self, tmp_path: Path):
        """A maintainer renames a previously-published slug, then tries to
        bless it via --update-snapshot. The CLI must refuse and leave the
        snapshot unchanged."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # Baseline has `lisa`; the new TOML renames it to `lisa-individuals`.
        snapshot_before = (
            '{"classification":{"SUN2020|2020":"sun"},'
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        # But the register row in the DB also needs the matching slug, since
        # populate runs first. Rewrite the DB row to match the TOML so the
        # only divergence is between TOML and snapshot.
        import sqlite3 as _sql

        conn = _sql.connect(db_dir / "reg_meta.db")
        conn.execute(
            "UPDATE register SET slug = 'lisa-individuals' WHERE register_id = 1"
        )
        conn.commit()
        conn.close()
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa-individuals"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        _write(
            slug_dir / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0
        assert (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8") == (
            snapshot_before
        )

    def test_removal_refused_even_with_update(self, tmp_path: Path):
        """Maintainer drops a previously-published row from the TOML."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        snapshot_before = (
            '{"classification":{"SUN2020|2020":"sun"},'
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa"\n',
        )
        _write(
            slug_dir / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0
        assert (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8") == (
            snapshot_before
        )

    def test_rename_accepted_under_update_when_unfrozen(self, tmp_path: Path):
        """§5.4 pre-v1 escape hatch: an ``UNFROZEN`` sentinel in the slug dir
        flips `--update-snapshot` from refuse-and-fail to write-through. The
        rename is still reported in the envelope so drift stays visible.
        """
        from reg_meta_build.cli import run

        from reg_meta_build.fqid_slugs import UNFROZEN_MARKER

        db_dir, slug_dir = self._seed_layout(tmp_path)
        snapshot_before = (
            '{"classification":{"SUN2020|2020":"sun"},'
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        (slug_dir / UNFROZEN_MARKER).write_text("pre-v1\n", encoding="utf-8")

        import sqlite3 as _sql

        conn = _sql.connect(db_dir / "reg_meta.db")
        conn.execute(
            "UPDATE register SET slug = 'lisa-individuals' WHERE register_id = 1"
        )
        conn.commit()
        conn.close()
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa-individuals"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        _write(
            slug_dir / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code == 0
        contents = (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8")
        assert "lisa-individuals" in contents
        assert '"lisa"' not in contents

    def test_pure_addition_accepted_under_update(self, tmp_path: Path):
        """The legitimate use case still works — adding a new slug refreshes
        the snapshot."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # Empty baseline; the three live entries are pure additions.
        (slug_dir / SNAPSHOT_FILENAME).write_text(
            '{"classification":{},"register":{},"register_variant":{},"variable":{}}\n',
            encoding="utf-8",
        )
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        _write(
            slug_dir / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun"\nversion = "2020"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code == 0
        # Snapshot now contains the live entries.
        contents = (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8")
        assert "lisa" in contents
        assert "individer-15plus" in contents


# ---------------------------------------------------------------------------
# `_default` candidate heuristic (issue #95)
# ---------------------------------------------------------------------------


class TestClassifyDefaultCandidate:
    def test_exact_mirror(self):
        assert (
            classify_default_candidate("Nybörjare i Komvux", "Nybörjare i Komvux")[0]
            == "exact"
        )

    def test_case_and_whitespace_insensitive(self):
        cls, _ = classify_default_candidate(
            "  Arbetskraftsbarometern  ", "ARBETSKRAFTSBAROMETERN"
        )
        assert cls == "exact"

    def test_paren_abbrev_stripped(self):
        cls, reason = classify_default_candidate(
            "Konjunkturstatistik, löner för statlig sektor (KLS)",
            "Konjunkturstatistik, löner för statlig sektor",
        )
        assert cls == "near"
        assert "paren" in reason

    def test_variant_matches_register_parenthetical(self):
        cls, reason = classify_default_candidate(
            "Registret för integrationsstudier (STATIV)", "STATIV"
        )
        assert cls == "near"
        assert "STATIV" in reason

    def test_survey_statistics_sibling(self):
        cls, reason = classify_default_candidate(
            "Continuing Vocational Training Statistics",
            "Continuing Vocational Training Survey",
        )
        assert cls == "near"
        assert "Survey/Statistics" in reason

    def test_canonical_substring(self):
        cls, _ = classify_default_candidate("Leveranser av fordonsgas", "Fordonsgas")
        assert cls == "near"

    def test_kept_when_genuinely_different(self):
        cls, _ = classify_default_candidate(
            "Mervärdesskatteregistret (Moms)", "Momsdeklarationsregistret"
        )
        assert cls == "kept"

    def test_kept_with_short_canonical_collision(self):
        # Two-letter overlap shouldn't qualify as a substring match.
        assert classify_default_candidate("Foo", "Bar")[0] == "kept"

    def test_missing_name(self):
        assert classify_default_candidate("", "Variant")[0] == "kept"


def _add_single_variant_register(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    register_variant_id: int,
    name: str,
    variant_name: str,
    variant_slug: str | None,
    provider_id: int = 1,
) -> None:
    """Insert a register with exactly one variant for iter-candidate tests."""
    conn.execute(
        "INSERT INTO register (register_id, provider_id, slug, name) "
        "VALUES (?, ?, ?, ?)",
        (register_id, provider_id, "todo", name),
    )
    conn.execute(
        "INSERT INTO register_variant "
        "(register_variant_id, register_id, slug, name) "
        "VALUES (?, ?, ?, ?)",
        (register_variant_id, register_id, variant_slug, variant_name),
    )


class TestIterDefaultSlugCandidates:
    def test_skips_multi_variant_register(self):
        # The default fixture has one variant; only that register-variant
        # pair should show up (and it's `kept` since names diverge).
        conn = build_slugged_db()
        # Add a second variant under the same register → no longer single-variant.
        conn.execute(
            "INSERT INTO register_variant "
            "(register_variant_id, register_id, slug, name) "
            "VALUES (?, ?, ?, ?)",
            (11, 1, "second", "Företag"),
        )
        candidates = list(iter_default_slug_candidates(conn))
        assert candidates == []

    def test_yields_three_classes(self):
        conn = build_slugged_db(
            register=None,
            variant=None,
            version=None,
            variable=None,
            classification=None,
        )
        _add_single_variant_register(
            conn,
            register_id=42,
            register_variant_id=124,
            name="Nybörjare i Komvux",
            variant_name="Nybörjare i Komvux",
            variant_slug="nyborjare-i-komvux",
        )
        _add_single_variant_register(
            conn,
            register_id=60,
            register_variant_id=168,
            name="Konjunkturstatistik, löner för statlig sektor (KLS)",
            variant_name="Konjunkturstatistik, löner för statlig sektor",
            variant_slug=None,
        )
        _add_single_variant_register(
            conn,
            register_id=346,
            register_variant_id=1158,
            name="Hushållens boende",
            variant_name="Individer",
            variant_slug="individer",
        )
        cands = list(iter_default_slug_candidates(conn))
        classes = {c.source_id: c.classification for c in cands}
        assert classes == {
            "42.124": "exact",
            "60.168": "near",
            "346.1158": "kept",
        }
        # Carries current slug so the hint can suppress already-applied rows.
        by_id = {c.source_id: c for c in cands}
        assert by_id["42.124"].current_slug == "nyborjare-i-komvux"
        assert by_id["60.168"].current_slug is None


class TestFormatDefaultSlugHints:
    def _make(
        self,
        provider: str,
        source_id: str,
        register_name: str,
        variant_name: str,
        classification: str,
        current_slug: str | None,
    ):
        from reg_meta_build.fqid_slugs import DefaultSlugCandidate

        return DefaultSlugCandidate(
            provider=provider,
            source_id=source_id,
            register_name=register_name,
            variant_name=variant_name,
            classification=classification,  # type: ignore[arg-type]
            reason="test",
            current_slug=current_slug,
        )

    def test_returns_none_when_no_actionable_candidates(self):
        # Both candidates already carry `_default` → nothing to suggest.
        cands = [
            self._make("scb", "42.124", "X", "X", "exact", "_default"),
            self._make("scb", "50.171", "Y", "Y", "exact", "_default"),
        ]
        assert format_default_slug_hints(cands, all_hints=False) is None

    def test_skips_kept_candidates(self):
        cands = [self._make("scb", "13.20", "A", "B", "kept", None)]
        assert format_default_slug_hints(cands, all_hints=False) is None

    def test_truncated_preview_by_default(self):
        cands = [
            self._make("scb", f"{i}.{i + 100}", f"Reg{i}", f"Reg{i}", "exact", None)
            for i in range(1, 11)
        ]
        out = format_default_slug_hints(cands, all_hints=False)
        assert out is not None
        assert "10 single-variant register(s)" in out
        assert "scb/1.101" in out
        assert "scb/5.105" in out
        # Tail is omitted; sentinel mentions `--all-hints`.
        assert "scb/10.110" not in out
        assert "--all-hints" in out
        assert "5 more" in out

    def test_all_hints_shows_full_list(self):
        cands = [
            self._make("scb", f"{i}.{i + 100}", f"Reg{i}", f"Reg{i}", "exact", None)
            for i in range(1, 11)
        ]
        out = format_default_slug_hints(cands, all_hints=True)
        assert out is not None
        assert "scb/10.110" in out
        assert "--all-hints" not in out

    def test_excludes_candidates_already_default(self):
        cands = [
            self._make("scb", "42.124", "X", "X", "exact", "_default"),
            self._make("scb", "50.171", "Y", "Y", "exact", None),
        ]
        out = format_default_slug_hints(cands, all_hints=True)
        assert out is not None
        assert "1 single-variant register(s)" in out
        assert "scb/50.171" in out
        assert "scb/42.124" not in out


class TestSeedSlugsCli:
    # The CLI's open_db enforces a schema-version manifest the in-memory
    # fixture doesn't seed, so these tests stub both the path resolver and
    # the connection open. Manifest-check coverage stays in integration tests.

    def _patch_cli(
        self,
        monkeypatch: pytest.MonkeyPatch,
        conn: sqlite3.Connection,
        tmp_path: Path,
    ) -> None:
        from reg_meta_build import cli

        monkeypatch.setattr(
            cli, "db_path_from_args", lambda _x: tmp_path / "reg_meta.db"
        )
        monkeypatch.setattr(cli, "open_db", lambda _path: conn)

    def test_quiet_suppresses_hints(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture,
    ):
        from reg_meta_build import cli

        conn = _make_seedable_db()
        self._patch_cli(monkeypatch, conn, tmp_path)
        out_dir = tmp_path / "out"
        args = _ns(
            out_dir=str(out_dir), force=True, all_hints=False, quiet=True, db=None
        )
        _env, rc = cli._cmd_seed_slugs(args)
        assert rc == 0
        assert capsys.readouterr().err == ""

    def test_emits_hints_to_stderr(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture,
    ):
        from reg_meta_build import cli

        conn = _make_seedable_db()
        self._patch_cli(monkeypatch, conn, tmp_path)
        out_dir = tmp_path / "out"
        args = _ns(
            out_dir=str(out_dir), force=True, all_hints=False, quiet=False, db=None
        )
        _env, rc = cli._cmd_seed_slugs(args)
        assert rc == 0
        captured = capsys.readouterr()
        assert "single-variant register(s)" in captured.err
        assert "_default" in captured.err
        assert "scb/42.124" in captured.err
        # The hint never leaks into the curated TOML files.
        scb_body = (out_dir / "scb.toml").read_text(encoding="utf-8")
        assert "Hint:" not in scb_body
        assert "_default" not in scb_body  # heuristic isn't auto-applied

    def test_reg_meta_quiet_env_suppresses_hints(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture,
    ):
        from reg_meta_build import cli

        conn = _make_seedable_db()
        self._patch_cli(monkeypatch, conn, tmp_path)
        monkeypatch.setenv("REG_META_QUIET", "1")
        args = _ns(
            out_dir=str(tmp_path / "out"),
            force=True,
            all_hints=False,
            quiet=False,
            db=None,
        )
        _env, rc = cli._cmd_seed_slugs(args)
        assert rc == 0
        assert capsys.readouterr().err == ""

    def test_toml_output_byte_identical_with_or_without_hint(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        # Acceptance criterion: TOML output unchanged byte-for-byte vs. before
        # this issue. Running with --quiet vs. without must produce the same
        # files on disk — the only difference is stderr.
        from reg_meta_build import cli

        # CLI closes the connection on exit; rebuild for each invocation.
        monkeypatch.setattr(
            cli, "db_path_from_args", lambda _x: tmp_path / "reg_meta.db"
        )
        monkeypatch.setattr(cli, "open_db", lambda _path: _make_seedable_db())
        out_quiet = tmp_path / "quiet"
        out_loud = tmp_path / "loud"
        cli._cmd_seed_slugs(
            _ns(
                out_dir=str(out_quiet), force=True, all_hints=False, quiet=True, db=None
            )
        )
        cli._cmd_seed_slugs(
            _ns(out_dir=str(out_loud), force=True, all_hints=True, quiet=False, db=None)
        )
        for name in ("scb.toml", "classifications.toml"):
            assert (out_quiet / name).read_bytes() == (out_loud / name).read_bytes()


def _make_seedable_db() -> sqlite3.Connection:
    """In-memory DB with one single-variant name-mirror register + LISA."""
    conn = build_slugged_db()
    _add_single_variant_register(
        conn,
        register_id=42,
        register_variant_id=124,
        name="Nybörjare i Komvux",
        variant_name="Nybörjare i Komvux",
        variant_slug=None,
    )
    conn.commit()
    return conn


def _ns(**kw):
    import argparse

    return argparse.Namespace(**kw)


# ---------------------------------------------------------------------------
# §5.5 same_as edge materialization
# ---------------------------------------------------------------------------


class TestMaterializeSameAsEdges:
    """Build-time `same_as` edge materialization (§5.5)."""

    @staticmethod
    def _slug_dir_with_same_as(tmp_path: Path, body: str) -> Path:
        """Write a scb.toml under tmp_path; classifications.toml gets an empty
        stub since load_slug_dir scans the whole directory."""

        (tmp_path / "scb.toml").write_text(body, encoding="utf-8")
        (tmp_path / "classifications.toml").write_text("", encoding="utf-8")
        return tmp_path

    @staticmethod
    def _db_with_two_variables() -> sqlite3.Connection:
        # Build a DB with two register/variable pairs under LISA so we can
        # exercise variable_same_as without involving cross-register links.
        conn = build_slugged_db()
        # Add second variable + binding under the same register/variant/version.
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '88', 'Civilstånd')"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id, data_type) "
            "VALUES (1002, 1, 10, 100, 88, 'int')"
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (1002, 'Civilstand')"
        )
        conn.commit()
        return conn

    def test_inserts_both_directions(self, tmp_path: Path) -> None:
        conn = self._db_with_two_variables()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "civilstand" }]\n',
        )
        counts = materialize_same_as_edges(conn, slug_dir)
        assert counts == {"variable": 1, "classification": 0}
        rows = conn.execute(
            "SELECT a_variable, b_variable FROM variable_same_as ORDER BY a_variable"
        ).fetchall()
        # One TOML edge → two DB rows (A→B + B→A).
        assert [(r[0], r[1]) for r in rows] == [
            ("civilstand", "kon"),
            ("kon", "civilstand"),
        ]

    def test_self_loop_rejected(self, tmp_path: Path) -> None:
        conn = self._db_with_two_variables()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "kon" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_self_loop"

    def test_reciprocal_pair_is_a_cycle(self, tmp_path: Path) -> None:
        conn = self._db_with_two_variables()
        # Both sides declare same_as → directed 2-cycle (kon → civilstand
        # → kon). The build stores both directions automatically; the
        # maintainer should only declare from one side.
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "civilstand" }]\n'
            '[variable."1.88"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "kon" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_cycle"

    def test_unknown_target_register_rejected(self, tmp_path: Path) -> None:
        conn = build_slugged_db()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "nonexistent", '
            'variable_slug = "kon" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_unknown_register"

    def test_unknown_target_variant_rejected(self, tmp_path: Path) -> None:
        conn = build_slugged_db()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'register_variant = "nonexistent", '
            'variable_slug = "kon" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_unknown_variant"

    def test_ambiguous_variable_aliases_rejected(self, tmp_path: Path) -> None:
        # Source (register_id, var_id) has two aliases that derive to
        # different slugs — the canonical form is ambiguous, so same_as
        # materialization must refuse.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (1001, 'CivStat')"
        )
        conn.commit()
        # Add a second variable so the target slot exists.
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '88', 'Civilstånd')"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id, data_type) "
            "VALUES (1002, 1, 10, 100, 88, 'int')"
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (1002, 'Civilstand')"
        )
        conn.commit()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "civilstand" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_ambiguous_source"

    def test_classification_edge_inserted(self, tmp_path: Path) -> None:
        conn = build_slugged_db()
        # Insert a second classification so the target slot resolves.
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug, publisher) "
            "VALUES ('SUN_OLD', 'Legacy SUN', '1996', 'sun-old', 'SCB')"
        )
        # Tag the seed classification's publisher too so the source resolves
        # to a stable provider key (default in the fixture is NULL).
        conn.execute("UPDATE classification SET publisher = 'SCB' WHERE slug = 'sun'")
        conn.commit()
        slug_dir = tmp_path
        (slug_dir / "scb.toml").write_text("", encoding="utf-8")
        (slug_dir / "classifications.toml").write_text(
            '[classification."SUN2020"]\n'
            'slug = "sun"\n'
            'version = "2020"\n'
            'same_as = [{ provider = "scb", classification_slug = "sun-old" }]\n',
            encoding="utf-8",
        )
        counts = materialize_same_as_edges(conn, slug_dir)
        assert counts == {"variable": 0, "classification": 1}
        rows = conn.execute(
            "SELECT a_classification_slug, b_classification_slug "
            "FROM classification_same_as ORDER BY a_classification_slug"
        ).fetchall()
        assert [(r[0], r[1]) for r in rows] == [
            ("sun", "sun-old"),
            ("sun-old", "sun"),
        ]

    def test_classification_ambiguous_target_rejected(self, tmp_path: Path) -> None:
        conn = build_slugged_db()
        # Two classifications share the same slug across versions; same_as
        # on (provider, slug) is ambiguous and must fail.
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug, publisher) "
            "VALUES ('SUN_OLD', 'Old SUN', '1996', 'sun-old', 'SCB')"
        )
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug, publisher) "
            "VALUES ('SUN_REVIVED', 'Revived SUN', '2024', 'sun-old', 'SCB')"
        )
        conn.execute("UPDATE classification SET publisher = 'SCB' WHERE slug = 'sun'")
        conn.commit()
        slug_dir = tmp_path
        (slug_dir / "scb.toml").write_text("", encoding="utf-8")
        (slug_dir / "classifications.toml").write_text(
            '[classification."SUN2020"]\n'
            'slug = "sun"\n'
            'version = "2020"\n'
            'same_as = [{ provider = "scb", classification_slug = "sun-old" }]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_ambiguous_classification"

    def test_multiple_targets_share_source(self, tmp_path: Path) -> None:
        # A single [variable] entry with two same_as targets produces two
        # edges sharing the same source endpoint. Both directions stored
        # for each → 4 DB rows total.
        conn = self._db_with_two_variables()
        # Add a third variable so we have a distinct second target.
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '99', 'Födelseår')"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id, data_type) "
            "VALUES (1003, 1, 10, 100, 99, 'int')"
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (1003, 'FodAr')"
        )
        conn.commit()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            "same_as = [\n"
            '  { provider = "scb", register = "lisa", variable_slug = "civilstand" },\n'
            '  { provider = "scb", register = "lisa", variable_slug = "fodar" },\n'
            "]\n",
        )
        counts = materialize_same_as_edges(conn, slug_dir)
        assert counts == {"variable": 2, "classification": 0}
        rows = conn.execute(
            "SELECT a_variable, b_variable FROM variable_same_as "
            "ORDER BY a_variable, b_variable"
        ).fetchall()
        # 2 TOML edges × 2 directions = 4 rows.
        assert [(r[0], r[1]) for r in rows] == [
            ("civilstand", "kon"),
            ("fodar", "kon"),
            ("kon", "civilstand"),
            ("kon", "fodar"),
        ]

    def test_classification_self_loop_rejected(self, tmp_path: Path) -> None:
        # Classification entry refers to itself — the shared cycle detector
        # must catch it under the classification label.
        conn = build_slugged_db()
        conn.execute("UPDATE classification SET publisher = 'SCB' WHERE slug = 'sun'")
        conn.commit()
        slug_dir = tmp_path
        (slug_dir / "scb.toml").write_text("", encoding="utf-8")
        (slug_dir / "classifications.toml").write_text(
            '[classification."SUN2020"]\n'
            'slug = "sun"\n'
            'version = "2020"\n'
            'same_as = [{ provider = "scb", classification_slug = "sun" }]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_self_loop"

    def test_period_without_variant_rejected(self, tmp_path: Path) -> None:
        # Codex P2: the resolver inherits the query's variant when
        # traversing an edge, so a period-only narrowing can never resolve
        # outside the variant that happens to carry the period. Reject at
        # build time so the curator's intent is unambiguous.
        conn = self._db_with_two_variables()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'period = "2018", variable_slug = "civilstand" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_period_without_variant"
