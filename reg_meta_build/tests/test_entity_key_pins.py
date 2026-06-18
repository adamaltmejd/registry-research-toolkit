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

from _slugged_db import (
    add_register,
    add_state,
    add_variable,
    add_variant,
    build_slugged_db,
)

from reg_meta_build.fqid_slugs import (
    MANDATORY_ENTITY_KEY_PROVIDERS,
    _curated_variable_slugs,
    infer_entity_key_pins,
    iter_entity_key_variables,
    load_provider_toml,
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
        assert ek.register_slug == "lisa" and ek.entity_key == "kon"

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
