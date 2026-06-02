"""Tests for slug TOML loading, validation, population, and seed/precheck."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from reg_meta.errors import RegMetaError

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    SNAPSHOT_FILENAME,
    SlugEntry,
    _parse_variable_id,
    classify_default_candidate,
    diff_snapshot,
    format_default_slug_hints,
    iter_default_slug_candidates,
    load_classifications_toml,
    load_provider_toml,
    load_slug_dir,
    materialize_same_as_edges,
    populate_slugs,
    populate_variable_slugs,
    precheck_slugs,
    propose_panel_entity_key,
    read_auto_derivations,
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


class TestParseVariableId:
    """A4.4b: the variable source-ID `<RegisterId>.<VarId>[.<disc>]` accepts a
    TEXT VarId for a non-SCB provider (SOS `provider_key` is a variable name),
    while keeping RegisterId a canonical int and guarding a numeric VarId."""

    def test_scb_integer_varid(self) -> None:
        assert _parse_variable_id("34.10") == (34, "10")

    def test_split_sibling_three_part(self) -> None:
        assert _parse_variable_id("34.10.kon") == (34, "10")

    def test_sos_text_varid(self) -> None:
        # SOS: register_id is a big minted int, VarId is the variable name.
        assert _parse_variable_id("5028920659479690770.ALDER") == (
            5028920659479690770,
            "ALDER",
        )
        # text VarId in a split-sibling key is fine too.
        assert _parse_variable_id("123.FOD_DATUMN.heltal") == (123, "FOD_DATUMN")

    def test_leading_zero_register_rejected(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            _parse_variable_id("034.10")
        assert "RegisterId must be an integer" in exc.value.message

    def test_numeric_varid_must_be_canonical(self) -> None:
        # A purely-numeric VarId is an SCB id → still guarded against leading zeros
        # so `1.10` and `1.010` can't alias one row.
        with pytest.raises(RegMetaError) as exc:
            _parse_variable_id("1.010")
        assert "numeric VarId must be in canonical" in exc.value.message

    def test_empty_varid_rejected(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            _parse_variable_id("1.")
        assert "VarId segment is empty" in exc.value.message

    def test_wrong_arity_rejected(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            _parse_variable_id("1")
        assert "expected" in exc.value.message


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

    def test_variant_with_panel_simple_entity_key(self, tmp_path: Path):
        # A4.4c: bare-string panel_entity_key + literal "period" time key.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\n'
            'slug = "individer-15plus"\n'
            'panel_entity_key = "personnummer"\n'
            'panel_time_key = "period"\n'
            'panel_time_grain = "delivery"\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].panel_entity_key == "personnummer"
        assert entries[0].panel_time_key == "period"
        assert entries[0].panel_time_grain == "delivery"

    def test_variant_with_panel_composite_entity_key(self, tmp_path: Path):
        # A4.4c: list panel_entity_key → stored as a tuple on the entry; a
        # variable-slug row-level time key.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\n'
            'slug = "individer-15plus"\n'
            'panel_entity_key = ["foretag", "arbetsstalle"]\n'
            'panel_time_key = "manad"\n'
            'panel_time_grain = "row"\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].panel_entity_key == ("foretag", "arbetsstalle")
        assert entries[0].panel_time_key == "manad"
        assert entries[0].panel_time_grain == "row"

    def test_variant_invalid_panel_time_grain_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\n'
            'slug = "individer-15plus"\n'
            'panel_time_grain = "yearly"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_variant_empty_panel_entity_key_rejected(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\nslug = "x"\npanel_entity_key = ""\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_panel_field_rejected_on_non_variant_kind(self, tmp_path: Path):
        # Panel fields are register_variant-only: the unknown-field guard rejects
        # them on a register entry (they're not in `_allowed_fields("register")`).
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "lisa"\npanel_entity_key = "personnummer"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_variant_malformed_panel_entity_key_rejected(self, tmp_path: Path):
        # A4.4c-i review P3: a panel key references a variable slug — a typo like
        # a stray `[` (which the catalog would later try to JSON-decode) must fail
        # at build time, not crash at serve time.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\nslug = "x"\n'
            'panel_entity_key = "[personnummer"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_variant_panel_time_key_period_or_slug(self, tmp_path: Path):
        # "period" is the exempt sentinel; a non-period time_key must be a valid
        # variable slug (a non-slug-shaped value is rejected).
        path = tmp_path / "scb.toml"
        _write(
            path, '[register_variant."34.153"]\nslug = "x"\npanel_time_key = "period"\n'
        )
        assert load_provider_toml(path)[0].panel_time_key == "period"
        _write(
            path,
            '[register_variant."34.153"]\nslug = "x"\npanel_time_key = "Not A Slug"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"

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
        # A2.6.1: the vintage is baked into the slug (`class/<slug>`), so the
        # entry is just a slug — no `version` field.
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun2020"\n',
        )
        entries = load_classifications_toml(path)
        assert entries == [
            SlugEntry(
                kind="classification",
                source_id="SUN2020",
                slug="sun2020",
            )
        ]

    def test_version_field_rejected(self, tmp_path: Path):
        # A2.6.1: `version` is no longer an allowed classification field —
        # it's caught by the unknown-field guard.
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun2020"\nversion = "2020"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_classifications_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_period_shaped_slug_rejected(self, tmp_path: Path):
        # The baked slug is a normal slug — period-shaped values (bare years)
        # are rejected, same as every other slug slot.
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020"]\nslug = "2020"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_classifications_toml(path)
        assert exc.value.code == "slug_toml_invalid"

    def test_duplicate_slug_rejected(self, tmp_path: Path):
        # Slug alone is the uniqueness key now (vintage baked in).
        path = _write(
            tmp_path / "classifications.toml",
            '[classification."SUN2020A"]\nslug = "sun2020"\n'
            '[classification."SUN2020B"]\nslug = "sun2020"\n',
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
        )
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {
            "register": 1,
            "register_variant": 1,
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
            == "sun2020"
        )

    def test_populates_panel_columns(self, tmp_path: Path):
        # A4.4c: a curated panel round-trips through populate_slugs. The composite
        # entity key is JSON-encoded into the TEXT column.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\n'
            'slug = "individer-15plus"\n'
            'panel_entity_key = ["foretag", "arbetsstalle"]\n'
            'panel_time_key = "period"\n'
            'panel_time_grain = "delivery"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun2020"\n',
        )
        conn = self._make_db()
        populate_slugs(conn, d, strict=True)
        row = conn.execute(
            "SELECT panel_entity_key, panel_time_key, panel_time_grain "
            "FROM register_variant WHERE register_variant_id = 10"
        ).fetchone()
        assert json.loads(row["panel_entity_key"]) == ["foretag", "arbetsstalle"]
        assert (row["panel_time_key"], row["panel_time_grain"]) == (
            "period",
            "delivery",
        )

    def test_panel_columns_null_by_default(self, tmp_path: Path):
        # A variant entry without panel fields leaves the columns NULL.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun2020"\n',
        )
        conn = self._make_db()
        populate_slugs(conn, d, strict=True)
        row = conn.execute(
            "SELECT panel_entity_key, panel_time_key, panel_time_grain "
            "FROM register_variant WHERE register_variant_id = 10"
        ).fetchone()
        assert tuple(row) == (None, None, None)

    def test_strict_fails_when_register_missing_slug(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        _write(
            d / "classifications.toml",
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
        )
        conn = self._make_db()
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_unknown_source_id"

    # A2.6: register_version left the FQID grammar — version slugs are neither
    # curated, auto-derived, nor persisted (no `register_version.slug` column),
    # so the former version-slot tests
    # (`test_populates_register_version_from_toml`,
    # `test_strict_fails_when_unperiodized_version_missing_slug`,
    # `test_unknown_register_version_source_id_fails`,
    # `test_curated_slug_overrides_auto_derived`,
    # `test_unique_constraint_blocks_sibling_slug_collision`,
    # `test_autoderive_collision_raises_reg_meta_error`) were removed with the
    # mechanism they tested.


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
        # A2.6.1: the seed emits a slug (folded from short_name), no `version`.
        assert "version = " not in cls_body
        assert "slug = " in cls_body

    def test_omits_register_version_from_seed(self):
        # A2.6: register_version is not seeded at all (version left the FQID
        # grammar; no slug column). The former version-seed tests (unperiodized
        # stub, periodized omission, curated override, §5.3 rule-5 collision
        # annotation, rule-6 residual stub) were removed with the mechanism.
        conn = build_slugged_db(version=("Strandlinje, 2019", None, 200))
        body = seed_provider_toml(conn, "scb")
        assert "[register_version." not in body


# ---------------------------------------------------------------------------
# seed-slugs --propose-panel (A4.4c-ii)
# ---------------------------------------------------------------------------


def _flag_identifier(conn: sqlite3.Connection, register_id: int, var_id: int) -> None:
    conn.execute(
        "UPDATE variable SET is_identifier = 1 "
        "WHERE register_id = ? AND provider_key = CAST(? AS TEXT)",
        (register_id, var_id),
    )
    conn.commit()


class TestProposePanel:
    """The A4.4c-ii proposer emits starter panel lines that a curator edits in
    A4.4d. It does not have to be exhaustive — only round-trip-valid and driven
    by the persisted is_identifier signal (see propose_panel_entity_key)."""

    def test_is_identifier_drives_entity_key(self):
        # default LISA fixture: var_id=44 ("Kön"), delivery column "Kon",
        # slug folded from the column → "kon". Flag it as the variant identifier.
        conn = build_slugged_db()
        _flag_identifier(conn, register_id=1, var_id=44)
        assert propose_panel_entity_key(conn, 1, 10) == "kon"

    def test_no_identifier_proposes_none(self):
        # No is_identifier, no source_join_key → SOS-shaped arm: nothing proposed,
        # left for A4.4d curation.
        conn = build_slugged_db()
        assert propose_panel_entity_key(conn, 1, 10) is None

    def test_join_key_alone_proposes_none(self):
        # A delivery column matching an ID-kolumner join key is NOT used as a
        # signal: source_join_key.table_name doesn't map to register_id, so a
        # column-name match can't be register-scoped and would over-propose a
        # non-identifier as the entity grain (review P2). Only is_identifier
        # drives the proposal — no flagged identifier → None (curation in A4.4d).
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO source_join_key (table_name, column_name, description) "
            "VALUES ('LISA_T', 'Kon', 'join key')"
        )
        conn.commit()
        assert propose_panel_entity_key(conn, 1, 10) is None

    def test_composite_entity_key_is_tuple(self):
        # Two is_identifier variables on one variant → a sorted tuple (the
        # composite case, persisted as a JSON array by populate_slugs).
        conn = build_slugged_db()
        _flag_identifier(conn, register_id=1, var_id=44)
        add_variable(conn, register_id=1, var_id=45, name="Lopnr", slug="lopnr")
        add_state(
            conn,
            register_id=1,
            var_id=45,
            register_variant_id=10,
            delivery_column_name="Lopnr",
        )
        conn.execute(
            "UPDATE variable SET is_identifier = 1 "
            "WHERE register_id = 1 AND provider_key = '45'"
        )
        conn.commit()
        assert propose_panel_entity_key(conn, 1, 10) == ("kon", "lopnr")

    def test_seed_emits_round_trippable_panel(self, tmp_path: Path):
        # End-to-end: seed with propose_panel, then the emitted TOML must load +
        # validate without error and carry the proposed panel fields.
        conn = build_slugged_db()
        _flag_identifier(conn, register_id=1, var_id=44)
        out = tmp_path / "out"
        seed_all(conn, out, propose_panel=True)
        entries = load_provider_toml(out / "scb.toml")
        variant = next(e for e in entries if e.kind == "register_variant")
        assert variant.panel_entity_key == "kon"
        assert variant.panel_time_key == "period"
        assert variant.panel_time_grain == "delivery"

    def test_seed_without_flag_omits_panel(self, tmp_path: Path):
        # Default seed (no --propose-panel) emits no panel lines.
        conn = build_slugged_db()
        _flag_identifier(conn, register_id=1, var_id=44)
        out = tmp_path / "out"
        seed_all(conn, out)
        body = (out / "scb.toml").read_text()
        assert "panel_entity_key" not in body
        assert "panel_time_key" not in body

    def test_seed_sos_shaped_variant_proposes_no_entity_key(self, tmp_path: Path):
        # A variant with no is_identifier / join-key signal: the seed emits the
        # time-key/grain defaults plus a curation comment, NO panel_entity_key,
        # and still round-trips.
        conn = build_slugged_db()  # no identifier flagged
        out = tmp_path / "out"
        seed_all(conn, out, propose_panel=True)
        body = (out / "scb.toml").read_text()
        assert "panel_entity_key =" not in body
        assert "# panel_entity_key:" in body
        entries = load_provider_toml(out / "scb.toml")
        variant = next(e for e in entries if e.kind == "register_variant")
        assert variant.panel_entity_key is None
        assert variant.panel_time_key == "period"
        assert variant.panel_time_grain == "delivery"


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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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

    def test_drifting_variables_advisory(self, tmp_path: Path):
        # §5.3/#143: precheck lists drifting-column variables (advisory — it must
        # NOT affect `ok`). The drift var is reported with its stored slug + the
        # distinct columns in edition order; a constant-column var is omitted.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        _write(d / "classifications.toml", "")
        conn = build_slugged_db()  # var 44, constant column "Kon" → not a drifter
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '65', 'Utbildningsinriktning')"
        ).lastrowid
        for yr, col in (
            ("2000", "SunInr"),
            ("2016", "sun2000inr1"),
            ("2020", "sun2020inr1"),
        ):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        # provider_key stays raw TEXT (never int-coerced — a non-numeric SOS key
        # must not crash this advisory); for SCB it's the numeric var_id as text.
        by_pk = {row[2]: row for row in result.drifting_variables}
        assert "65" in by_pk
        assert "44" not in by_pk  # constant column is not a drifter
        prov, reg_id, provider_key, slug, name, cols = by_pk["65"]
        assert (prov, reg_id, provider_key) == ("scb", 1, "65")
        assert slug == "utbildningsinriktning"
        assert name == "Utbildningsinriktning"
        assert cols == ("SunInr", "sun2000inr1", "sun2020inr1")

    def test_drifting_advisory_tolerates_nonnumeric_provider_key(self, tmp_path: Path):
        # `variable.provider_key` is TEXT — SOS ships a merged variable name, not
        # a numeric var_id. The advisory must report it raw, not `int()` it (that
        # would crash the otherwise non-fatal precheck on a non-SCB provider).
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        _write(d / "classifications.toml", "")
        conn = build_slugged_db()
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, 'BefolkningPerKommun', 'Befolkning')"
        ).lastrowid
        for yr, col in (("2000", "BefKom"), ("2010", "BefKommun")):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)  # must not raise on the non-numeric key
        hit = [r for r in result.drifting_variables if r[2] == "BefolkningPerKommun"]
        assert len(hit) == 1
        assert hit[0][3] == "befolkning"  # name basis (cols collide-free, drift)

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
            '[classification."GHOST"]\nslug = "ghost"\n',
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

    # A2.6: register_version left the FQID grammar — no version-slug collision
    # detection in precheck (the `colliding_versions` field is gone). The former
    # collision tests (`test_periodized_sibling_collision_flagged`,
    # `test_collision_resolved_by_curated_override`,
    # `test_collision_when_override_clashes_with_auto_derived_sibling`) were
    # removed with the mechanism.


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
        # A2.6: no register_version in the snapshot (version left the grammar).
        payload = {
            "classification": {"SUN2020": "sun2020"},
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


class TestVariableOverridesAcceptedByPopulateSlugs:
    """A2.1.5: `[variable]` slug overrides are now wired (they write
    `variable.slug` via `populate_variable_slugs`). `populate_slugs` itself only
    handles register/variant/version/classification and must *accept* (ignore)
    variable rows rather than raising the old `slug_variable_override_unsupported`
    gate."""

    def _make_db(self):
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.execute("UPDATE classification SET slug = NULL")
        conn.commit()
        return conn

    def test_variable_slug_override_is_accepted(self, tmp_path: Path):
        # The lifted gate: a `[variable]` slug no longer raises in
        # populate_slugs. It's applied later by populate_variable_slugs.
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
        )
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {
            "register": 1,
            "register_variant": 1,
            "classification": 1,
        }

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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
        )
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {
            "register": 1,
            "register_variant": 1,
            "classification": 1,
        }


class TestPopulateVariableSlugs:
    """A2.1.5 (§5.3): stored `variable.slug` population — kolumnnamn-derived
    where register-unique, name-fallback otherwise, never-failing fallback
    chain, curated overrides, .auto.toml generation."""

    @staticmethod
    def _db(
        *, slug: str | None = None, kol: str = "Kon", name: str = "Kön"
    ) -> sqlite3.Connection:
        # build_slugged_db seeds variable.slug + a variable_state era carrying
        # `kol`; NULL the stored slug so the population function does the work.
        conn = build_slugged_db(variable=(name, 44, 1001, kol), variable_slug=slug)
        if slug is None:
            conn.execute("UPDATE variable SET slug = NULL")
            conn.commit()
        return conn

    @staticmethod
    def _slug_dir(tmp_path: Path, scb_body: str = "") -> Path:
        (tmp_path / "scb.toml").write_text(scb_body, encoding="utf-8")
        return tmp_path

    def _stored_slug(self, conn: sqlite3.Connection, var_id: int) -> str | None:
        row = conn.execute(
            "SELECT slug FROM variable WHERE provider_key = CAST(? AS TEXT)",
            (var_id,),
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _add_variable(conn: sqlite3.Connection, *, var_id: int, name: str, kol: str):
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) VALUES (1, ?, ?)",
            (str(var_id), name),
        ).lastrowid
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, 10, '2018-01-01', '9999-12-31', 'int', ?)",
            (vid, kol),
        )
        conn.commit()

    def test_auto_derives_and_persists(self, tmp_path: Path) -> None:
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        counts = populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon"
        assert counts["auto_new"] == 1
        # .auto.toml written, keyed register.var.
        auto = d / f"scb{AUTO_FILE_SUFFIX}"
        assert auto.is_file()
        entries = load_provider_toml(auto)
        assert any(
            e.kind == "variable" and e.source_id == "1.44" and e.slug == "kon"
            for e in entries
        )

    def test_curated_override_wins(self, tmp_path: Path) -> None:
        conn = self._db(kol="Kon")
        d = self._slug_dir(
            tmp_path,
            '[register."1"]\nslug = "lisa"\n[variable."1.44"]\nslug = "kon-curated"\n',
        )
        counts = populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon-curated"
        assert counts["curated"] == 1
        assert counts["auto_new"] == 0

    def test_stale_curated_override_rejected(self, tmp_path: Path) -> None:
        # A non-deprecated [variable] override for a (register, var) with no live
        # variable is a typo — fail rather than silently auto-slug the variable.
        conn = self._db(kol="Kon")  # only live variable is 1.44
        d = self._slug_dir(tmp_path, '[variable."1.999"]\nslug = "ghost"\n')
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_variable_override_stale"
        assert "1.999" in exc.value.message

    def test_deprecated_curated_override_may_be_stale(self, tmp_path: Path) -> None:
        # A deprecated override may outlive its (retired) variable — no error.
        conn = self._db(kol="Kon")
        d = self._slug_dir(
            tmp_path, '[variable."1.999"]\nslug = "ghost"\ndeprecated = true\n'
        )
        populate_variable_slugs(conn, d)  # must not raise
        assert self._stored_slug(conn, 44) == "kon"

    def test_deprecated_curated_slug_reserved(self, tmp_path: Path) -> None:
        # §5.4: a deprecated curated [variable] slug (retired variable kept in
        # the snapshot) is reserved — a new live variable can't auto-derive it
        # and recreate the published FQID.
        conn = self._db(kol="Kon")  # live var 44 → would derive "kon"
        d = self._slug_dir(
            tmp_path, '[variable."1.999"]\nslug = "kon"\ndeprecated = true\n'
        )
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon-2"  # not the retired "kon"

    def test_curated_override_reusing_auto_slug_rejected(self, tmp_path: Path) -> None:
        # A curated override must not reuse a slug frozen for a DIFFERENT source
        # in auto.toml — it would duplicate a published FQID (or hit UNIQUE).
        conn = self._db(kol="Kon")  # live var 44
        d = self._slug_dir(tmp_path, '[variable."1.44"]\nslug = "ghost"\n')
        # "ghost" is already frozen for a different (pruned) source 1.999.
        (d / f"scb{AUTO_FILE_SUFFIX}").write_text(
            '[variable."1.999"]\nslug = "ghost"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_variable_override_conflict"

    def test_existing_auto_not_recomputed_on_rename(self, tmp_path: Path) -> None:
        # First build: auto-derives `kon` from `Kon`, persists to .auto.toml.
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        # SCB renames the delivery column; rebuild reads the existing
        # .auto.toml and keeps the original slug (§5.3 immutability).
        conn2 = self._db(kol="Konkod")  # would derive `konkod`
        counts = populate_variable_slugs(conn2, d)
        assert self._stored_slug(conn2, 44) == "kon"
        assert counts["auto_existing"] == 1
        assert counts["auto_new"] == 0

    def test_retired_auto_slug_not_reused(self, tmp_path: Path) -> None:
        # §5.4 immutability: a frozen auto slug whose variable was pruned from
        # the delivery stays reserved — a live/new variable can't be assigned it,
        # which would duplicate the slug in the rewritten auto.toml.
        conn = self._db(kol="Kon")  # live var 44, kolumnnamn → "kon"
        d = self._slug_dir(tmp_path)
        # Retired var 1.999 (absent from the DB) froze "kon" in auto.toml.
        (d / f"scb{AUTO_FILE_SUFFIX}").write_text(
            '[variable."1.999"]\nslug = "kon"\n', encoding="utf-8"
        )
        populate_variable_slugs(conn, d)
        # var 44 wanted "kon" but it's frozen to the retired entry → uniquified.
        assert self._stored_slug(conn, 44) == "kon-2"
        # The rewritten auto.toml keeps the retired entry and adds no duplicate.
        slugs = [
            e.slug
            for e in load_provider_toml(d / f"scb{AUTO_FILE_SUFFIX}")
            if e.kind == "variable"
        ]
        assert sorted(slugs) == ["kon", "kon-2"]

    def test_collision_falls_back_to_name(self, tmp_path: Path) -> None:
        # Two distinct variables under one register whose kolumnnamn fold to the
        # SAME slug ('kon') no longer fail the build — both fall back to their
        # (distinct) names, yielding distinct register-unique slugs (§5.3).
        conn = self._db(kol="Kon", name="Kön")  # var 44
        self._add_variable(conn, var_id=88, name="Civilstånd", kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        s44, s88 = self._stored_slug(conn, 44), self._stored_slug(conn, 88)
        assert s44 and s88 and s44 != s88
        assert s88 == "civilstand"  # name-derived, since 'kon' collided

    def test_unique_kolumnnamn_keeps_short_slug(self, tmp_path: Path) -> None:
        # When the kolumnnamn slug is register-unique it wins over the (longer)
        # name even if the name differs — keeps the short common-case leaf.
        conn = self._db(kol="Sysselsattning", name="Sysselsättningsstatus i november")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "sysselsattning"

    def test_multiple_eras_single_slug(self, tmp_path: Path) -> None:
        # variable.slug is register-scoped (one row per variable), so multiple
        # variable_state eras for one variable can't multiply or fork the slug.
        conn = self._db(kol="Kon")
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE provider_key = '44'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, "
            "data_type, delivery_column_name) "
            "VALUES (?, 10, '2019-01-01', '9999-12-31', 'int', 'Kon')",
            (vid,),
        )
        conn.commit()
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        slugs = conn.execute(
            "SELECT slug FROM variable WHERE provider_key = '44'"
        ).fetchall()
        assert [r[0] for r in slugs] == ["kon"]

    def test_underivable_kol_falls_back_to_name(self, tmp_path: Path) -> None:
        # A delivery column that folds to empty ('...') no longer fails — the
        # slug derives from the variable name instead.
        conn = self._db(kol="...", name="Kön")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon"

    def test_ultimate_fallback_when_name_underivable(self, tmp_path: Path) -> None:
        # Neither kolumnnamn nor name yields a slug (both lead with a digit) →
        # last-resort v<provider_key>.
        conn = self._db(kol="3DOMR", name="3D-område")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "v44"

    def test_long_name_fallback_is_length_capped(self, tmp_path: Path) -> None:
        # A generic kolumnnamn forces the name fallback; a very long name is
        # truncated on a hyphen boundary to a readable leaf.
        long_name = (
            "Utgifter för egen FoU efter finansieringskälla EU ramprogram forskning"
        )
        conn = self._db(kol="OBS_VALUE", name=long_name)
        # Second variable sharing OBS_VALUE so the kolumnnamn slug collides.
        self._add_variable(conn, var_id=88, name="Annat värde", kol="OBS_VALUE")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        slug = self._stored_slug(conn, 44)
        assert slug is not None and len(slug) <= 60
        assert slug.startswith("utgifter-for-egen-fou")

    def test_name_slug_truncation_revalidates_reserved_word(self) -> None:
        # `Class <very-long-single-token>` derives to a valid `class-<token>`,
        # but truncating at the only hyphen ≤ cap yields the reserved word
        # `class`. _name_slug must not return that (unaddressable) — it keeps the
        # full, valid slug instead.
        from reg_meta.fqid import derive_variable_slug

        from reg_meta_build.fqid_slugs import _name_slug

        result = _name_slug("Class " + "x" * 70, cap=60)
        assert result is not None
        assert result != "class"
        # Round-trips through the validator (not reserved / period / malformed).
        assert derive_variable_slug(result) == result

    def test_auto_toml_parses_as_provider_scb(self, tmp_path: Path) -> None:
        # The generated scb.auto.toml must load via load_slug_dir as provider
        # `scb` (the `.auto` suffix must not break provider-slug grammar).
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        # classifications.toml stub so load_slug_dir is happy.
        (d / "classifications.toml").write_text("", encoding="utf-8")
        populate_variable_slugs(conn, d)
        entries = load_slug_dir(d)
        var_entries = [
            e for e in entries if e.kind == "variable" and e.provider == "scb"
        ]
        assert any(e.source_id == "1.44" and e.slug == "kon" for e in var_entries)

    def test_auto_slugs_flow_into_snapshot(self, tmp_path: Path) -> None:
        # Auto-derived variable slugs (in .auto.toml) must land in the snapshot
        # payload's "variable" kind so the §5.4 grow-only guard covers them.
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        (d / "classifications.toml").write_text("", encoding="utf-8")
        populate_variable_slugs(conn, d)
        payload = snapshot_payload(load_slug_dir(d))
        assert payload["variable"].get("scb/1.44") == "kon"
        # A rename of the auto slug is flagged by diff_snapshot.
        previous = {k: dict(v) for k, v in payload.items()}
        previous["variable"]["scb/1.44"] = "kon-old"
        diff = diff_snapshot(previous, payload)
        assert any("1.44" in r for r in diff["renamed"])

    # --- §5.3/#143: drift-stable slug basis (+ doable-now part of #141) --------

    @staticmethod
    def _add_drift_variable(
        conn: sqlite3.Connection,
        *,
        var_id: int,
        name: str,
        cols: list[str],
        register_id: int = 1,
    ) -> int:
        """Insert a variable + one variable_state era per column so its
        `delivery_column_name` drifts across editions. `cols` is earliest→latest
        (era N spans year 2000+N), so `cols[0]` is the earliest delivery column.
        Repeating a column (`["Syss", "Syss"]`) yields a constant, NON-drifting
        variable (COUNT(DISTINCT)=1)."""
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) VALUES (?, ?, ?)",
            (register_id, str(var_id), name),
        ).lastrowid
        assert vid is not None
        for i, col in enumerate(cols):
            yr = 2000 + i
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        return vid

    def _slug_of_vid(self, conn: sqlite3.Connection, vid: int) -> str | None:
        return conn.execute(
            "SELECT slug FROM variable WHERE variable_id = ?", (vid,)
        ).fetchone()[0]

    def test_drifting_column_slugs_from_name(self, tmp_path: Path) -> None:
        # §5.3/#143: a 1:1 variable whose single delivery column drifts across
        # editions (SunInr→sun2000inr1→sun2020inr1) must NOT slug from its latest
        # column (`sun2020inr1` — misleading + version-coupled). The
        # register-unique NAME wins: a version-neutral `utbildningsinriktning`.
        conn = self._db(kol="Kon")  # var 44 (kon) stays an unrelated live var
        vid = self._add_drift_variable(
            conn,
            var_id=65,
            name="Utbildningsinriktning",
            cols=["SunInr", "sun2000inr1", "sun2020inr1"],
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._slug_of_vid(conn, vid) == "utbildningsinriktning"

    def test_drifting_name_collision_falls_back_to_earliest_column(
        self, tmp_path: Path
    ) -> None:
        # The watch-out: two drifting variables in one register sharing a name.
        # The name slug collides → each routes to its EARLIEST delivery column
        # (the stable, disambiguating basis), NOT an arbitrary `name`/`name-2`.
        conn = self._db(kol="Kon")
        a = self._add_drift_variable(
            conn, var_id=70, name="Inriktning", cols=["AlfaKod", "alfa_ny"]
        )
        b = self._add_drift_variable(
            conn, var_id=71, name="Inriktning", cols=["BetaKod", "beta_ny"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        sa, sb = self._slug_of_vid(conn, a), self._slug_of_vid(conn, b)
        assert sa == "alfakod"  # earliest column, not "inriktning"
        assert sb == "betakod"

    def test_split_siblings_drift_slug_from_earliest_column(
        self, tmp_path: Path
    ) -> None:
        # #141 (doable-now part): split siblings share one provider_key AND the
        # generic name, so a drifting sibling's name collides → it slugs from its
        # own EARLIEST column (#139's discriminator basis), staying rebuild-stable
        # and distinct per sibling. Frozen-build rename-immutability across a
        # rename of that earliest column is deferred post-slug-freeze.
        conn = self._db(kol="Kon")
        a = self._add_drift_variable(
            conn, var_id=99, name="Imputerat", cols=["BoareaImp", "boarea_imp"]
        )
        b = self._add_drift_variable(
            conn, var_id=99, name="Imputerat", cols=["BantalrumImp", "bantalrum_imp"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        sa, sb = self._slug_of_vid(conn, a), self._slug_of_vid(conn, b)
        assert sa == "boareaimp"
        assert sb == "bantalrumimp"

    def test_constant_column_is_not_drift(self, tmp_path: Path) -> None:
        # Regression: two eras carrying the SAME column is not drift
        # (COUNT(DISTINCT)=1) — it keeps the register-unique column slug.
        conn = self._db(kol="Kon")
        vid = self._add_drift_variable(
            conn, var_id=80, name="Sysselsattning", cols=["Syss", "Syss"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._slug_of_vid(conn, vid) == "syss"


class TestAutoDerivationMarker:
    """A4.4a: the `# source:` derivation marker written into `scb.auto.toml` and
    the name-fallback worklist surfaced via `precheck-slugs`. The marker is a
    TOML COMMENT (provenance), so it must be invisible to tomllib/SlugEntry/
    snapshot while remaining readable by `read_auto_derivations`."""

    @staticmethod
    def _slug_dir(tmp_path: Path, scb_body: str = "") -> Path:
        d = tmp_path / "slugs"
        d.mkdir()
        (d / "scb.toml").write_text(scb_body, encoding="utf-8")
        (d / "classifications.toml").write_text("", encoding="utf-8")
        return d

    @staticmethod
    def _add_variable(
        conn: sqlite3.Connection,
        *,
        var_id: int,
        name: str,
        cols: list[str],
        register_id: int = 1,
    ) -> int:
        """Variable + one variable_state era per column. A single col → constant
        (no drift); repeated/distinct cols exercise the drift arm."""
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) VALUES (?, ?, ?)",
            (register_id, str(var_id), name),
        ).lastrowid
        assert vid is not None
        for i, col in enumerate(cols):
            yr = 2000 + i
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        return vid

    def _auto_path(self, d: Path) -> Path:
        return d / f"scb{AUTO_FILE_SUFFIX}"

    def test_marker_round_trips_invisibly_to_tomllib(self, tmp_path: Path) -> None:
        # The `# source:` comment must NOT become a parsed key — re-parsing the
        # written auto.toml yields the same slug values and only the `slug` field
        # (no `source`/derivation key leaks into SlugEntry / snapshot).
        conn = build_slugged_db(variable=("Kön", 44, 1001, "Kon"))
        conn.execute("UPDATE variable SET slug = NULL")
        conn.commit()
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        auto = self._auto_path(d)
        # The raw file carries the comment ...
        assert "# source:" in auto.read_text(encoding="utf-8")
        # ... but tomllib (via load_provider_toml) sees only the slug.
        entries = [e for e in load_provider_toml(auto) if e.kind == "variable"]
        assert len(entries) == 1
        e = entries[0]
        assert e.source_id == "1.44"
        assert e.slug == "kon"
        # No stray attribute carries the derivation — SlugEntry has a fixed
        # field set; the snapshot payload is unaffected.
        payload = snapshot_payload(load_slug_dir(d))
        assert payload["variable"] == {"scb/1.44": "kon"}

    def test_marker_class_per_derivation_arm(self, tmp_path: Path) -> None:
        # Each fallback arm stamps its own class. One register, distinct vars:
        #   - kolumnnamn-unique → `kolumnnamn`
        #   - name fallback (shared generic column) → `name-fallback`
        #   - drift (column changes across eras), unique name → `drift-name`
        #   - underivable kol+name (leading digit) → `v-provider-key`
        conn = build_slugged_db(variable=None)  # bare register/variant, no var
        self._add_variable(conn, var_id=10, name="Kön", cols=["Kon"])
        # 20 & 21 share generic OBS_VALUE → kol collides → both name-fall back.
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        self._add_variable(
            conn,
            var_id=30,
            name="Utbildningsinriktning",
            cols=["SunInr", "sun2020inr1"],
        )
        self._add_variable(conn, var_id=40, name="3D-område", cols=["3DOMR"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        deriv = read_auto_derivations(self._auto_path(d))
        assert deriv["1.10"] == "kolumnnamn"
        assert deriv["1.20"] == "name-fallback"
        assert deriv["1.21"] == "name-fallback"
        assert deriv["1.30"] == "drift-name"
        assert deriv["1.40"] == "v-provider-key"

    def test_disambiguator_marked_and_in_worklist(self, tmp_path: Path) -> None:
        # Two distinct vars whose NAMES fold to the same slug → the second is
        # `_uniquify`-suffixed. The marker carries `+disambiguated` and the row
        # lands in the worklist regardless of its base class.
        conn = build_slugged_db(variable=None)
        # Shared generic column forces the name fallback for both; identical
        # names collide so the second gets `-2`.
        self._add_variable(conn, var_id=50, name="Belopp", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=51, name="Belopp", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        deriv = read_auto_derivations(self._auto_path(d))
        # One is plain name-fallback, the other carries +disambiguated.
        kinds = {deriv["1.50"], deriv["1.51"]}
        assert kinds == {"name-fallback", "name-fallback+disambiguated"}
        result = precheck_slugs(conn, d)
        worklist = {(sid, kind) for _p, sid, _s, kind in result.name_fallback_variables}
        assert ("1.50", "name-fallback") in worklist
        assert ("1.51", "name-fallback+disambiguated") in worklist

    def test_worklist_excludes_column_derived(self, tmp_path: Path) -> None:
        # The worklist is the curation BACKLOG: name / `-N` / `v<key>` only.
        # kolumnnamn, fold, and drift-earliest-column (column-derived) bases are
        # EXCLUDED — they have a stable, canonical basis already.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=10, name="Kön", cols=["Kon"])  # kolumnnamn
        # Two drifting vars sharing a name → each routes to earliest column
        # (drift-earliest-column), which must NOT be in the worklist.
        self._add_variable(
            conn, var_id=70, name="Inriktning", cols=["AlfaKod", "alfa_ny"]
        )
        self._add_variable(
            conn, var_id=71, name="Inriktning", cols=["BetaKod", "beta_ny"]
        )
        # A name-fallback var that IS in the worklist (control).
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        by_sid = {sid: kind for _p, sid, _s, kind in result.name_fallback_variables}
        assert "1.10" not in by_sid  # kolumnnamn
        assert "1.70" not in by_sid  # drift-earliest-column
        assert "1.71" not in by_sid  # drift-earliest-column
        assert by_sid.get("1.20") == "name-fallback"
        assert by_sid.get("1.21") == "name-fallback"

    def test_worklist_carries_slug_and_provider(self, tmp_path: Path) -> None:
        # Each worklist row is (provider, source_id, slug, derivation); the slug
        # is read from the auto file (no DB join needed).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        rows = {
            sid: (prov, slug, kind)
            for prov, sid, slug, kind in result.name_fallback_variables
        }
        assert rows["1.20"] == ("scb", "inkomst", "name-fallback")
        assert rows["1.21"] == ("scb", "utgift", "name-fallback")

    def test_worklist_is_advisory_only(self, tmp_path: Path) -> None:
        # A populated worklist must NOT flip `ok` or the precheck exit. Curate the
        # register/variant/classification slugs so the gating checks are all
        # clean, then confirm `ok` is True DESPITE a non-empty name-fallback
        # worklist (variables auto-slug, so they never feed the missing/stale
        # gates anyway).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(
            tmp_path,
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        (d / "classifications.toml").write_text(
            '[classification."SUN2020"]\nslug = "sun2020"\n', encoding="utf-8"
        )
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        assert result.name_fallback_variables  # worklist non-empty
        assert result.ok  # ... yet the gating checks are all clean

    def test_read_auto_derivations_tolerates_missing_and_unmarked(
        self, tmp_path: Path
    ) -> None:
        # Robustness: a missing file → {}; an entry without a `# source:` comment
        # (legacy pre-A4.4a row) → simply absent (never crashes).
        d = self._slug_dir(tmp_path)
        auto = self._auto_path(d)
        assert read_auto_derivations(auto) == {}  # no file yet
        auto.write_text(
            '[variable."1.44"]\nslug = "kon"\n\n'
            '[variable."1.55"]\nslug = " inkomst "  # source: name-fallback\n',
            encoding="utf-8",
        )
        deriv = read_auto_derivations(auto)
        assert "1.44" not in deriv  # unmarked legacy row
        assert deriv["1.55"] == "name-fallback"

    def test_worklist_tolerates_nonnumeric_provider_key(self, tmp_path: Path) -> None:
        # `variable.provider_key` is TEXT — a SOS key is a merged variable name,
        # not a numeric var_id, so the auto.toml key is `1.BefolkningPerKommun`.
        # The validating loader rejects that grammar, so the worklist must read
        # the auto file RAW (like `_drifting_variables` tolerates the TEXT key)
        # — otherwise this otherwise-non-fatal precheck would crash.
        conn = build_slugged_db(variable=None)
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, 'BefolkningPerKommun', 'Befolkning')"
        ).lastrowid
        assert vid is not None
        for yr, col in (("2000", "BefKom"), ("2010", "BefKommun")):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)  # must not raise on the non-numeric key
        hit = [
            r for r in result.name_fallback_variables if r[1] == "1.BefolkningPerKommun"
        ]
        assert len(hit) == 1
        # drift + name-unique-among-drifters → drift-name (a worklist class).
        assert hit[0] == ("scb", "1.BefolkningPerKommun", "befolkning", "drift-name")

    def test_existing_markers_carried_forward_on_rebuild(self, tmp_path: Path) -> None:
        # An incremental rebuild (prior auto.toml + a NEW variable → auto_dirty)
        # must PRESERVE the pre-existing rows' `# source:` markers, not strip them
        # — else the worklist shrinks over time. populate_variable_slugs seeds
        # auto_derivation from the prior file before Pass 3 re-derives only the new.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        first = read_auto_derivations(self._auto_path(d))
        assert first["1.20"] == "name-fallback"
        assert first["1.21"] == "name-fallback"
        # Add a NEW variable and rebuild — the full file is rewritten. (30 is now
        # the only pending var, so its OBS_VALUE column is unique among pending →
        # it takes the kolumnnamn slug; the class differs from 20/21, which is
        # exactly why a re-derivation can't reconstruct their original markers.)
        self._add_variable(conn, var_id=30, name="Sparande", cols=["OBS_VALUE"])
        populate_variable_slugs(conn, d)
        second = read_auto_derivations(self._auto_path(d))
        assert second["1.30"] == "kolumnnamn"  # the new row is freshly classed ...
        assert second["1.20"] == "name-fallback"  # ... and the prior markers
        assert second["1.21"] == "name-fallback"  # survived the rewrite.

    def test_read_auto_derivations_tolerates_undecodable_file(
        self, tmp_path: Path
    ) -> None:
        # "Tolerant by design" (advisory): invalid UTF-8 must yield {}, not raise.
        d = self._slug_dir(tmp_path)
        auto = self._auto_path(d)
        auto.write_bytes(
            b'[variable."1.44"]\nslug = "\xff\xfe"  # source: name-fallback\n'
        )
        assert read_auto_derivations(auto) == {}

    def test_advisory_worklist_survives_malformed_auto_toml(
        self, tmp_path: Path
    ) -> None:
        # A malformed `<provider>.auto.toml` is precheck's job to report via
        # parse_errors; the advisory worklist must NOT turn it into a crash —
        # it skips the unparseable provider.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        # Unterminated string → tomllib raises → _parse_toml → RegMetaError.
        self._auto_path(d).write_text(
            '[variable."1.20"]\nslug = "inkomst\n', encoding="utf-8"
        )
        result = precheck_slugs(conn, d)  # must not raise
        assert all(r[0] != "scb" for r in result.name_fallback_variables)

    def test_worklist_excludes_curated_override(self, tmp_path: Path) -> None:
        # A variable a curator FIXED via a [variable] override in <provider>.toml
        # is no longer backlog, even though its frozen auto entry + marker linger
        # in the auto file across the rebuild.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        before = {r[1] for r in precheck_slugs(conn, d).name_fallback_variables}
        assert {"1.20", "1.21"} <= before
        # Curate 1.20 — its auto entry/marker persist, but it drops from the list.
        (d / "scb.toml").write_text(
            '[variable."1.20"]\nslug = "hushalls-inkomst"\n', encoding="utf-8"
        )
        populate_variable_slugs(conn, d)
        after = {r[1] for r in precheck_slugs(conn, d).name_fallback_variables}
        assert "1.20" not in after  # curated → excluded from the backlog
        assert "1.21" in after  # still backlog

    def test_advisory_worklist_survives_nontable_variable(self, tmp_path: Path) -> None:
        # A syntactically-valid auto.toml whose `variable` is a non-table value
        # must not crash the worklist (precheck reports it via parse_errors).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        self._auto_path(d).write_text('variable = "bad"\n', encoding="utf-8")
        result = precheck_slugs(conn, d)  # must not raise on the odd shape
        assert all(r[0] != "scb" for r in result.name_fallback_variables)

    def test_worklist_keeps_metadata_only_override(self, tmp_path: Path) -> None:
        # A [variable] entry WITHOUT a string slug leaves the auto slug unchanged,
        # so it stays backlog regardless of other metadata: same_as (a cross-rename
        # edge) and deprecated (still slugged so old references resolve) both keep
        # their variable in the list. Only a string `slug` override drops it
        # (Codex: don't treat every [variable] row as a slug fix).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        (d / "scb.toml").write_text(
            '[variable."1.20"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "inkomst" }]\n'
            '[variable."1.21"]\ndeprecated = true\n',
            encoding="utf-8",
        )
        populate_variable_slugs(conn, d)
        worklist = {r[1] for r in precheck_slugs(conn, d).name_fallback_variables}
        assert "1.20" in worklist  # same_as only → slug unfixed → stays
        assert "1.21" in worklist  # deprecated-only → slug still ships → stays

    def test_dotted_provider_key_fails_fast(self, tmp_path: Path) -> None:
        # A provider_key containing '.' would mis-parse the variable source-ID as
        # a split-sibling 3-part key (silent slug mis-attribution). Fail fast at
        # source-ID construction instead. (Inert for SCB — its keys are ints — but
        # a non-SCB provider_key is an arbitrary name, so guard it. A4.4b review.)
        conn = build_slugged_db(variable=None)
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, 'FOO.BAR', 'Foo')"
        ).lastrowid
        assert vid is not None
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, 10, '2000-01-01', '2000-12-31', 'int', 'FooBar')",
            (vid,),
        )
        conn.commit()
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, self._slug_dir(tmp_path))
        assert "contains '.'" in exc.value.message


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
        # A2.6.1: classification is (short_name, name, slug) — no version.
        conn = build_slugged_db(
            classification=(
                'KÅL "2020"',
                "Svensk utbildning",
                "sun2020",
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
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SUN2020', 'Svensk utbildning', 'sun2020')"
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '{"classification":{"SUN2020":"sun2020"},'
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
            '[classifications."SUN2020"]\nslug = "sun2020"\n',
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
            '[classification."SUN2020"]\nslug = "sun2020"\n'
            'same_as = [{ provider = "scb", classifcation_slug = "sun-v1" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_classifications_toml(path)
        assert "classifcation_slug" in exc.value.message


# A2.6.1: TestClassificationVersionMismatch (the slug-TOML-vs-DB version
# cross-check) is gone — there's no `version` column left to disagree with.
# The slug alone is the FQID and `class.slug UNIQUE` keeps it globally distinct.


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
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SUN2020', 'Svensk utbildning', 'sun2020')"
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
            '{"classification":{"SUN2020":"sun2020"},'
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '{"classification":{"SUN2020":"sun2020"},'
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '{"classification":{"SUN2020":"sun2020"},'
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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
            '[classification."SUN2020"]\nslug = "sun2020"\n',
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

    # A4.4c-ii: default the new flag off so the existing seed-slugs CLI tests
    # (which don't pass it) keep their byte-identical, panel-free output.
    kw.setdefault("propose_panel", False)
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
        # Add a second variable under the same register. `materialize_same_as_edges`
        # anchors on the stored `variable.slug`, so var 88 (a source in the
        # reciprocal-cycle test) only needs its slug set — no instance/alias/state
        # rows are read by the same_as materializer.
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '88', 'Civilstånd', 'civilstand')"
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

    def test_register_variant_key_rejected(self, tmp_path: Path) -> None:
        # A2.1.5: variable same_as is variable-grain — the `register_variant`
        # narrowing key was dropped (§5.5), so it's now an unknown key rejected
        # at TOML load.
        conn = build_slugged_db()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'register_variant = "individer-15plus", '
            'variable_slug = "kon" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_toml_invalid"

    def test_source_without_stored_slug_rejected(self, tmp_path: Path) -> None:
        # A2.1.5: same_as anchors on the stored `variable.slug`. If the source
        # variable has no stored slug (underivable / absent at build),
        # materialization must refuse with an actionable error rather than
        # silently emitting an unanchorable edge. (The old derive-from-aliases
        # ambiguity path is gone — the stored slug is single by construction.)
        conn = self._db_with_two_variables()
        # Drop the source variable's stored slug to simulate the no-slug case.
        conn.execute("UPDATE variable SET slug = NULL WHERE provider_key = '44'")
        conn.commit()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "civilstand" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_unresolved_source"

    def test_ambiguous_split_source_rejected(self, tmp_path: Path) -> None:
        # A2.2 forward guard: when a triage split leaves two variables sharing
        # (register_id, provider_key), a same_as anchored on the bare source key
        # is ambiguous — it would attach to an arbitrary sibling — so reject it
        # rather than pick one with LIMIT 1.
        conn = build_slugged_db()  # var 44, provider_key '44', slug 'kon'
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '44', 'Kön 5-pos', 'kon-5pos')"
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
        conn = build_slugged_db()  # fixture seeds SUN2020 with slug 'sun2020'
        # Insert a second classification so the target slot resolves. A2.6.1:
        # the slug bakes in the vintage and is globally UNIQUE.
        conn.execute(
            "INSERT INTO classification (short_name, name, slug, publisher) "
            "VALUES ('SUN_OLD', 'Legacy SUN', 'sun1996', 'SCB')"
        )
        # Tag the seed classification's publisher too so the source resolves
        # to a stable provider key (default in the fixture is NULL).
        conn.execute(
            "UPDATE classification SET publisher = 'SCB' WHERE slug = 'sun2020'"
        )
        conn.commit()
        slug_dir = tmp_path
        (slug_dir / "scb.toml").write_text("", encoding="utf-8")
        (slug_dir / "classifications.toml").write_text(
            '[classification."SUN2020"]\n'
            'slug = "sun2020"\n'
            'same_as = [{ provider = "scb", classification_slug = "sun1996" }]\n',
            encoding="utf-8",
        )
        counts = materialize_same_as_edges(conn, slug_dir)
        assert counts == {"variable": 0, "classification": 1}
        rows = conn.execute(
            "SELECT a_classification_slug, b_classification_slug "
            "FROM classification_same_as ORDER BY a_classification_slug"
        ).fetchall()
        assert [(r[0], r[1]) for r in rows] == [
            ("sun1996", "sun2020"),
            ("sun2020", "sun1996"),
        ]

    # A2.6.1: test_classification_ambiguous_target_rejected is gone — a slug
    # naming "multiple versions" is structurally impossible now (slug UNIQUE),
    # and the `slug_same_as_ambiguous_classification` check was removed. Two
    # rows sharing a slug would fail the UNIQUE constraint at insert anyway.

    def test_multiple_targets_share_source(self, tmp_path: Path) -> None:
        # A single [variable] entry with two same_as targets produces two
        # edges sharing the same source endpoint. Both directions stored
        # for each → 4 DB rows total.
        conn = self._db_with_two_variables()
        # Add a third variable (slug 'fodar') as a distinct second same_as
        # target. `materialize_same_as_edges` anchors on `variable.slug` only.
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, '99', 'Födelseår', 'fodar')"
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
        conn = build_slugged_db()  # slug 'sun2020'
        conn.execute(
            "UPDATE classification SET publisher = 'SCB' WHERE slug = 'sun2020'"
        )
        conn.commit()
        slug_dir = tmp_path
        (slug_dir / "scb.toml").write_text("", encoding="utf-8")
        (slug_dir / "classifications.toml").write_text(
            '[classification."SUN2020"]\n'
            'slug = "sun2020"\n'
            'same_as = [{ provider = "scb", classification_slug = "sun2020" }]\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_same_as_self_loop"

    def test_period_key_rejected(self, tmp_path: Path) -> None:
        # A2.1.5: the `period` same_as narrowing key was dropped (§5.5) — it's
        # now an unknown key rejected at TOML load. (Previously a period-only
        # narrowing raised slug_same_as_period_without_variant; that whole
        # narrowing concept is gone at variable grain.)
        conn = self._db_with_two_variables()
        slug_dir = self._slug_dir_with_same_as(
            tmp_path,
            '[variable."1.44"]\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'period = "2018", variable_slug = "civilstand" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(conn, slug_dir)
        assert exc.value.code == "slug_toml_invalid"
