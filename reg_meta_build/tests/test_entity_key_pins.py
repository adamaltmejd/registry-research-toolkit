"""Tests for the panel entity-key slug-pin generator (#546; `fqid_slugs.py`).

The generator (`infer_entity_key_pins` / `render_entity_key_pins_toml`) emits a
mandatory-curation worklist: a `[variable]` slug pin per `panel_entity_key`
variable so the slug its panel ref binds to can't drift across the
re-derive-every-build churn. The build-side gate that consumes the SAME
enumeration (`iter_entity_key_variables`) is tested in `test_validate.py`.

Fully synthetic (CLAUDE.md): builds its own in-memory DBs and an empty/seeded
slug dir under tmp_path; never reads the shipped fqid_slugs TOMLs."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_register,
    add_state,
    add_variable,
    add_variant,
    build_slugged_db,
)
from reg_meta.errors import RegMetaError

from reg_meta_build.fqid_slugs import (
    MANDATORY_ENTITY_KEY_PROVIDERS,
    _curated_variable_slugs,
    infer_entity_key_pins,
    iter_entity_key_variables,
    load_provider_toml,
    populate_variable_slugs,
    render_entity_key_pins_toml,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


def _db_with_entity_key(entity_key: str | list[str]) -> sqlite3.Connection:
    """Fixture DB: register 1 (lisa) with variable `kon` (provider_key 44,
    source_id `1.44`) plus a second variable `ar` (provider_key 99, source_id
    `1.99`), and variant 10 carrying `panel_entity_key = entity_key` (a bare
    slug, or a json-array composite when a list)."""
    conn = build_slugged_db(classification=None)
    add_variable(conn, register_id=1, var_id=99, name="År", slug="ar")
    add_state(
        conn,
        register_id=1,
        variable_slug="ar",
        register_variant_id=10,
        delivery_column_name="Ar",
    )
    stored = json.dumps(entity_key) if isinstance(entity_key, list) else entity_key
    conn.execute(
        "UPDATE register_variant SET panel_entity_key = ? WHERE register_variant_id = 10",
        (stored,),
    )
    conn.commit()
    return conn


def _add_sos_entity_key(conn: sqlite3.Connection) -> None:
    """Add a NON-SCB (sos, provider_id 2) register/variant/variable carrying a
    `panel_entity_key`, so enumeration sees it but the SCB-scoped generator/gate
    must NOT (#546: only SCB is under mandatory curation today)."""
    add_register(conn, register_id=500, slug="dors", name="Dödsorsaker", provider_id=2)
    add_variable(conn, register_id=500, var_id="LOPNR", name="Löpnummer", slug="lopnr")
    add_variant(conn, register_variant_id=5000, register_id=500, slug="grund", name="G")
    add_state(
        conn,
        register_id=500,
        variable_slug="lopnr",
        register_variant_id=5000,
        delivery_column_name="Lopnr",
    )
    conn.execute(
        "UPDATE register_variant SET panel_entity_key = 'lopnr' "
        "WHERE register_variant_id = 5000"
    )
    conn.commit()


def _slug_dir(tmp_path: Path, scb_body: str = "") -> Path:
    d = tmp_path / "slugs"
    d.mkdir()
    (d / "scb.toml").write_text(scb_body, encoding="utf-8")
    return d


def _db_with_split_sibling_entity_key(
    *, entity_key_slug: str | None
) -> sqlite3.Connection:
    """Fixture DB where the panel entity-key variable is a SPLIT SIBLING.

    Register 1 (lisa) holds two variables sharing one `provider_key` (`50`), so
    `populate_variable_slugs` keys them 3-part `<reg>.<pk>.<disc>` via
    `_split_sibling_disc` (the disc is each sibling's EARLIEST delivery-column
    slug). Variant 10's `panel_entity_key = "lopnr"` names the entity-key
    sibling.

    Crucially the two are decoupled: the 3-part key's discriminator is the
    sibling's column slug (`lopnrny`, folded from `LopNrNy`), while the panel ref
    binds to its `variable.slug` (`lopnr`). The entity-key sibling's stored slug
    is `entity_key_slug`:
      - pass `"lopnr"` for the BUILT-DB shape the generator reads (the ref
        resolves, so the pin is emitted);
      - pass `None` for the FRESH-DB shape `populate_variable_slugs` re-derives
        from scratch — auto-derivation would then pick `lopnrny` (the column
        slug), the DIFFERENT slug a reslug drifts the panel ref to (#539). The
        pin must override that back to `lopnr`.

    The sibling (`kon`) keeps `provider_key` `50` too, so the key stays split in
    both shapes (the disc is column-derived, hence stable across them)."""
    conn = build_slugged_db(variable=None, classification=None)
    # Entity-key sibling: earliest/only column `LopNrNy` → disc `lopnrny`; its
    # stored slug (`lopnr`) is the panel-referenced one, deliberately != the
    # auto-derivable column slug.
    ek_vid = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (1, '50', 'Löpnummer', ?)",
        (entity_key_slug,),
    ).lastrowid
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name) "
        "VALUES (?, 10, '2000-01-01', '2000-12-31', 'int', 'LopNrNy')",
        (ek_vid,),
    )
    # Split sibling sharing provider_key `50`: column `Kon` → disc `kon`.
    sib_vid = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (1, '50', 'Kön', ?)",
        ("kon" if entity_key_slug is not None else None,),
    ).lastrowid
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name) "
        "VALUES (?, 10, '2000-01-01', '2000-12-31', 'int', 'Kon')",
        (sib_vid,),
    )
    conn.execute(
        "UPDATE register_variant SET panel_entity_key = 'lopnr' "
        "WHERE register_variant_id = 10"
    )
    conn.commit()
    return conn


class TestEnumerate:
    def test_resolves_bare_key_to_source_id(self):
        """A bare `panel_entity_key` slug resolves to its variable in the
        variant's register, carrying the build source_id (`<reg>.<provider_key>`)."""
        conn = _db_with_entity_key("kon")
        eks = list(iter_entity_key_variables(conn))
        assert len(eks) == 1
        (ek,) = eks
        assert (ek.provider_slug, ek.source_id, ek.variable_slug) == (
            "scb",
            "1.44",
            "kon",
        )
        assert ek.register_slug == "lisa"

    def test_composite_key_resolves_every_element(self):
        """A composite (json-array) key resolves element-wise; each element that
        names a real variable is enumerated once."""
        conn = _db_with_entity_key(["kon", "ar"])
        keyed = {
            (ek.source_id, ek.variable_slug) for ek in iter_entity_key_variables(conn)
        }
        assert keyed == {("1.44", "kon"), ("1.99", "ar")}

    def test_dangling_element_skipped(self):
        """A composite element naming no variable is SKIPPED here — the
        resolution gate (`_check_panel_refs_resolve`) owns the dangle finding, so
        the generator must not emit a pin for a slug that binds nothing."""
        conn = _db_with_entity_key(["kon", "ghost"])
        keyed = {ek.variable_slug for ek in iter_entity_key_variables(conn)}
        assert keyed == {"kon"}

    def test_enumerates_all_providers_unscoped(self):
        """The enumerator is PROVIDER-GENERAL — it yields a non-SCB (sos)
        entity-key var too; the SCB scoping is applied by the callers, not here.
        (Guards against re-narrowing `iter_entity_key_variables` itself, which
        would break its reusability.)"""
        conn = _db_with_entity_key("kon")
        _add_sos_entity_key(conn)
        providers = {ek.provider_slug for ek in iter_entity_key_variables(conn)}
        assert providers == {"scb", "sos"}

    def test_yields_three_part_source_id_for_split_sibling(self):
        """A split-sibling entity-key var carries a 3-part `<reg>.<pk>.<disc>`
        source_id (the disc is its own column slug, NOT the panel-ref slug), so a
        pin keys the right sibling — the #539 regression class."""
        conn = _db_with_split_sibling_entity_key(entity_key_slug="lopnr")
        eks = list(iter_entity_key_variables(conn))
        assert len(eks) == 1
        (ek,) = eks
        # 3-part key: register.provider_key.discriminator (the column slug).
        assert ek.source_id.count(".") == 2
        assert (ek.source_id, ek.variable_slug) == ("1.50.lopnrny", "lopnr")

    def test_dot_in_provider_key_raises(self):
        """A `provider_key` containing '.' would mis-parse the source-ID as a
        phantom split-sibling 3-part key, so enumeration fails fast (the build's
        own guard) rather than mis-attributing a pin to the wrong sibling."""
        conn = _db_with_entity_key("kon")
        # A dotted provider_key in the entity-key var's OWN register trips the
        # source-ID grammar when `_variable_source_ids` enumerates the register.
        conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (1, 'FOO.BAR', 'Foo', 'foo')"
        )
        conn.commit()
        with pytest.raises(RegMetaError) as exc:
            list(iter_entity_key_variables(conn))
        assert exc.value.code == "slug_toml_invalid"
        assert "contains '.'" in exc.value.message


class TestGenerator:
    def test_emits_pin_for_non_curated(self, tmp_path: Path):
        conn = _db_with_entity_key("kon")
        pins = infer_entity_key_pins(conn, _slug_dir(tmp_path))
        assert len(pins) == 1
        (pin,) = pins
        assert (pin.provider_slug, pin.source_id, pin.slug) == ("scb", "1.44", "kon")

    def test_skips_already_curated(self, tmp_path: Path):
        """A variable with a hand-curated `[variable]` slug is skipped (the
        idempotency the gate relies on, and what keeps the 35 existing #539 pins
        from re-appearing)."""
        conn = _db_with_entity_key(["kon", "ar"])
        # Curate only `kon` (1.44); `ar` (1.99) stays un-pinned.
        slug_dir = _slug_dir(tmp_path, '[variable."1.44"]\nslug = "kon"\n')
        pins = infer_entity_key_pins(conn, slug_dir)
        assert [(p.source_id, p.slug) for p in pins] == [("1.99", "ar")]

    def test_idempotent_after_pinning(self, tmp_path: Path):
        """Re-running after every entity-key var is pinned emits nothing."""
        conn = _db_with_entity_key("kon")
        slug_dir = _slug_dir(tmp_path, '[variable."1.44"]\nslug = "kon"\n')
        assert infer_entity_key_pins(conn, slug_dir) == []

    def test_emitted_toml_binds_to_source_ids(self, tmp_path: Path):
        """The rendered block re-parses through `load_provider_toml` and binds to
        the exact `(provider, source_id)` keys `populate_variable_slugs` consumes
        — proving the pin is actually applied, not just well-formed text."""
        conn = _db_with_entity_key(["kon", "ar"])
        pins = infer_entity_key_pins(conn, _slug_dir(tmp_path))
        toml = render_entity_key_pins_toml(pins)
        reparse_dir = tmp_path / "reparse"
        reparse_dir.mkdir()
        (reparse_dir / "scb.toml").write_text(toml, encoding="utf-8")
        curated = _curated_variable_slugs(load_provider_toml(reparse_dir / "scb.toml"))
        assert curated == {("scb", "1.44"): "kon", ("scb", "1.99"): "ar"}

    def test_emitted_toml_populates_variable_slug(self, tmp_path: Path):
        """End-to-end round-trip: the rendered pin, loaded back, makes
        `populate_variable_slugs` LOCK the entity-key var's `variable.slug` to the
        pinned value (precedence 1, curated wins). Goes one step past
        `test_emitted_toml_binds_to_source_ids` (which stops at the parsed dict) —
        proving the slug is actually written to the DB, not just well-keyed."""
        conn = _db_with_entity_key("kon")
        pins = infer_entity_key_pins(conn, _slug_dir(tmp_path))
        toml = render_entity_key_pins_toml(pins)
        # Fresh DB whose slugs are unpopulated, so populate_variable_slugs does
        # the work; with the pin loaded, `kon` (1.44) must land as the pin says.
        fresh = _db_with_entity_key("kon")
        fresh.execute("UPDATE variable SET slug = NULL")
        fresh.commit()
        slug_dir = tmp_path / "apply"
        slug_dir.mkdir()
        (slug_dir / "scb.toml").write_text(toml, encoding="utf-8")
        populate_variable_slugs(fresh, slug_dir)
        kon = fresh.execute(
            "SELECT slug FROM variable WHERE register_id = 1 AND provider_key = '44'"
        ).fetchone()[0]
        assert kon == "kon"

    def test_split_sibling_pin_binds_to_correct_sibling(self, tmp_path: Path):
        """#539 regression, tight: the entity-key var is a SPLIT SIBLING, so the
        pin's 3-part `source_id` discriminator (`lopnrny`, the column slug) differs
        from the panel-referenced `variable.slug` (`lopnr`). The generator emits
        the 3-part key; loading that pin into a fresh DB whose auto-derivation
        would otherwise pick the column slug (`lopnrny`) must instead bind `lopnr`
        to the RIGHT sibling — proving the pin freezes the panel-ref slug against
        the reslug that drifted it, and lands on the correct sibling (not its
        `kon` sibling)."""
        # BUILT-DB shape: the entity-key sibling already carries `lopnr`, so the
        # panel ref resolves and the generator emits a 3-part pin.
        built = _db_with_split_sibling_entity_key(entity_key_slug="lopnr")
        pins = infer_entity_key_pins(built, _slug_dir(tmp_path))
        assert len(pins) == 1
        (pin,) = pins
        # NOT a 2-part key — the split discriminator (column slug) is present.
        assert pin.source_id.count(".") == 2
        assert (pin.source_id, pin.slug) == ("1.50.lopnrny", "lopnr")
        toml = render_entity_key_pins_toml(pins)

        # FRESH-DB shape: slugs unpopulated → auto-derivation would pick the
        # column slug `lopnrny`. The pin must override the entity-key sibling back
        # to `lopnr`, leaving its `kon` sibling untouched.
        fresh = _db_with_split_sibling_entity_key(entity_key_slug=None)
        slug_dir = tmp_path / "apply"
        slug_dir.mkdir()
        (slug_dir / "scb.toml").write_text(toml, encoding="utf-8")
        populate_variable_slugs(fresh, slug_dir)
        slugs = dict(
            fresh.execute(
                "SELECT v.slug, ("
                " SELECT vs.delivery_column_name FROM variable_state vs "
                " WHERE vs.variable_id = v.variable_id LIMIT 1) "
                "FROM variable v WHERE v.register_id = 1 AND v.provider_key = '50'"
            ).fetchall()
        )
        # Keyed by column to assert per-sibling: entity-key sibling (col LopNrNy)
        # gets the pinned `lopnr`, NOT its auto column slug `lopnrny`; the other
        # sibling (col Kon) auto-derives `kon` and is untouched.
        assert slugs == {"lopnr": "LopNrNy", "kon": "Kon"}

    def test_pins_sorted_numeric_aware(self, tmp_path: Path):
        """Pins sort numeric-aware by source_id (matching `write_auto_toml`), so
        `1.44` precedes `1.99` regardless of insertion order."""
        conn = _db_with_entity_key(["ar", "kon"])
        pins = infer_entity_key_pins(conn, _slug_dir(tmp_path))
        assert [p.source_id for p in pins] == ["1.44", "1.99"]

    def test_non_scb_entity_key_not_emitted(self, tmp_path: Path):
        """#546: only mandatory-curation providers (SCB today) get pins. A non-SCB
        (sos) entity-key var is enumerated but NOT emitted, while the SCB one is —
        so the gate never fails the build on un-pinned non-SCB vars (#209)."""
        assert "scb" in MANDATORY_ENTITY_KEY_PROVIDERS
        assert "sos" not in MANDATORY_ENTITY_KEY_PROVIDERS
        conn = _db_with_entity_key("kon")
        _add_sos_entity_key(conn)
        pins = infer_entity_key_pins(conn, _slug_dir(tmp_path))
        assert [(p.provider_slug, p.source_id) for p in pins] == [("scb", "1.44")]
