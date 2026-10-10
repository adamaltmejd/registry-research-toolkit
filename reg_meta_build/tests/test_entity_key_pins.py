"""The entity-key pins applied by `populate_variable_slugs` (#546, #559;
`fqid_slugs.py`).

The generator (`entity-key-pins`: steward scope, composite keys, partition-member
native ids, idempotence, the overwrite guard and the usage refusals) is pinned at the
CLI boundary (`cases/cli/entity-key-pins/`). What stays here is the other half of
the round trip: a rendered pin, loaded back into a fresh DB, makes
`populate_variable_slugs` lock the entity-key variable's slug. That function runs
only inside extend-db, so these blocks move to the extend-db boundary with it.

Fully synthetic (CLAUDE.md): builds its own in-memory DBs and a flat steward-style
slug dir under tmp_path; never reads the shipped fqid_slugs TOMLs."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from _slugged_db import add_state, add_variable, build_slugged_db

from reg_meta_build.fqid_slugs import (
    infer_entity_key_pins,
    populate_variable_slugs,
    render_entity_key_pins_toml,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


def _steward_pin_file(root: Path, body: str) -> Path:
    """A flat steward slug dir whose `scb.toml` names register 1 (lisa), plus
    ``body`` (rendered pins)."""
    root.mkdir(parents=True, exist_ok=True)
    path = root / "scb.toml"
    path.write_text('[register."1"]\nslug = "lisa"\n' + body, encoding="utf-8")
    return path


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


def _slug_dir(tmp_path: Path, scb_body: str = "") -> Path:
    """A steward slug dir naming both fixture registers; ``scb_body`` adds pins."""
    d = tmp_path / "slugs"
    d.mkdir()
    (d / "scb.toml").write_text(
        '[register."1"]\nslug = "lisa"\n' + scb_body, encoding="utf-8"
    )
    (d / "sos.toml").write_text('[register."500"]\nslug = "dors"\n', encoding="utf-8")
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
        "INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name) "
        "VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), ?, 10, '2000-01-01', '2000-12-31', 'int', 'LopNrNy')",
        (ek_vid,),
    )
    # Split sibling sharing provider_key `50`: column `Kon` → disc `kon`.
    sib_vid = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (1, '50', 'Kön', ?)",
        ("kon" if entity_key_slug is not None else None,),
    ).lastrowid
    conn.execute(
        "INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name) "
        "VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), ?, 10, '2000-01-01', '2000-12-31', 'int', 'Kon')",
        (sib_vid,),
    )
    conn.execute(
        "UPDATE register_variant SET panel_entity_key = 'lopnr' "
        "WHERE register_variant_id = 10"
    )
    conn.commit()
    return conn


class TestGenerator:
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
        _steward_pin_file(slug_dir, toml)
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
        _steward_pin_file(slug_dir, toml)
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
