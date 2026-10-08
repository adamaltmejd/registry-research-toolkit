"""Tests for the steward panel entity-key slug-pin generator (#546, #559;
`fqid_slugs.py`).

The generator (`infer_entity_key_pins` / `render_entity_key_pins_toml`) emits a
mandatory-curation worklist for a flavored DB: a `[variable]` slug pin per
`panel_entity_key` variable of the steward registers a steward slug dir names, so
the slug its panel ref binds to can't drift when extend-db re-derives it. The
steward gate consumes the SAME enumeration (`iter_entity_key_variables`).

Fully synthetic (CLAUDE.md): builds its own in-memory DBs and a flat steward-style
slug dir under tmp_path; never reads the shipped fqid_slugs TOMLs."""

from __future__ import annotations

import json
import sqlite3
import tomllib
from pathlib import Path

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
    EntityKeyPin,
    infer_entity_key_pins,
    iter_entity_key_variables,
    populate_variable_slugs,
    render_entity_key_pins_toml,
    write_entity_key_pins,
)


def _steward_pin_file(root: Path, body: str) -> Path:
    """A flat steward slug dir whose `scb.toml` names register 1 (lisa), plus
    ``body`` (rendered pins)."""
    root.mkdir(parents=True, exist_ok=True)
    path = root / "scb.toml"
    path.write_text('[register."1"]\nslug = "lisa"\n' + body, encoding="utf-8")
    return path


def _pin_values(path: Path) -> dict[str, str]:
    return {
        source_id: row["slug"]
        for source_id, row in tomllib.loads(path.read_text())["variable"].items()
    }


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
    `panel_entity_key` (`lopnr`, source_id `500.LOPNR`), a second register the
    steward scope can include or leave out."""
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


# The fixture registers' native ids by (provider, register slug), as a slug dir's
# register entries name them; pins key on these, never on the catalog's id.
_NATIVE_IDS = {("scb", "lisa"): "1", ("sos", "dors"): "500"}


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
        eks = list(iter_entity_key_variables(conn, _NATIVE_IDS))
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
            (ek.source_id, ek.variable_slug)
            for ek in iter_entity_key_variables(conn, _NATIVE_IDS)
        }
        assert keyed == {("1.44", "kon"), ("1.99", "ar")}

    def test_dangling_element_skipped(self):
        """A composite element naming no variable is SKIPPED here — the
        resolution gate (`_check_panel_refs_resolve`) owns the dangle finding, so
        the generator must not emit a pin for a slug that binds nothing."""
        conn = _db_with_entity_key(["kon", "ghost"])
        keyed = {
            ek.variable_slug for ek in iter_entity_key_variables(conn, _NATIVE_IDS)
        }
        assert keyed == {"kon"}

    def test_yields_three_part_source_id_for_split_sibling(self):
        """A split-sibling entity-key var carries a 3-part `<reg>.<pk>.<disc>`
        source_id (the disc is its own column slug, NOT the panel-ref slug), so a
        pin keys the right sibling — the #539 regression class."""
        conn = _db_with_split_sibling_entity_key(entity_key_slug="lopnr")
        eks = list(iter_entity_key_variables(conn, _NATIVE_IDS))
        assert len(eks) == 1
        (ek,) = eks
        # 3-part key: register.provider_key.discriminator (the column slug).
        assert ek.source_id.count(".") == 2
        assert (ek.source_id, ek.variable_slug) == ("1.50.lopnrny", "lopnr")


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
        path = _steward_pin_file(tmp_path / "reparse", toml)
        assert _pin_values(path) == {"1.44": "kon", "1.99": "ar"}

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

    def test_pins_sorted_numeric_aware(self, tmp_path: Path):
        """Pins sort numeric-aware by source_id (matching `write_auto_toml`), so
        `1.44` precedes `1.99` regardless of insertion order."""
        conn = _db_with_entity_key(["ar", "kon"])
        pins = infer_entity_key_pins(conn, _slug_dir(tmp_path))
        assert [p.source_id for p in pins] == ["1.44", "1.99"]

    def test_header_points_at_steward_dir(self, tmp_path: Path):
        """#559: the rendered block tells the curator to regenerate with
        `--slug-dir <steward dir>` and fold into `fqid_slugs/<steward>/`."""
        conn = _db_with_entity_key("kon")
        pins = infer_entity_key_pins(conn, _slug_dir(tmp_path))
        toml = render_entity_key_pins_toml(pins)
        assert "--slug-dir <steward dir>" in toml
        assert "fqid_slugs/<steward>/<provider>.toml" in toml
        assert '[variable."1.44"]' in toml
        assert 'slug = "kon"' in toml

    def test_scopes_to_steward_dir_registers(self, tmp_path: Path):
        """#559: `infer_entity_key_pins(conn, steward_dir)` scopes to the steward
        REGISTERS the dir curates. With a DB carrying BOTH a global (scb) and a
        steward (sos register 500) entity-key var and a steward dir curating ONLY
        the steward register, the emitted pins are for that register alone — the
        scb var (the global base's, whose `_variable_source_ids` is flavored-unsafe)
        is excluded entirely."""
        conn = _db_with_entity_key("kon")  # scb register 1, 1.44/kon
        _add_sos_entity_key(conn)  # sos register 500, 500.LOPNR/lopnr
        steward_dir = tmp_path / "steward"
        steward_dir.mkdir()
        # Register scope is derived from the curated `[register]` ENTRIES' source
        # ids: a register slug entry for 500 in sos.toml puts register 500 in scope
        # without pinning the entity-key VARIABLE, so the gate/generator still emit
        # it. (Mirrors the real steward dir, which always carries register/variant
        # slug entries — a TOML with no `[register]` entries yields no scope.)
        (steward_dir / "sos.toml").write_text(
            '[register."500"]\nslug = "dors"\n', encoding="utf-8"
        )

        pins = infer_entity_key_pins(conn, steward_dir)
        assert [(p.provider_slug, p.source_id) for p in pins] == [("sos", "500.LOPNR")]

    def test_write_groups_per_provider_regardless_of_order(self, tmp_path: Path):
        """`write_entity_key_pins` groups by provider via dict accumulation, so a
        non-provider-sorted input still lands every provider's pins in its own
        file (guards against the dropped `itertools.groupby` sorted-input
        assumption)."""
        pins = [
            EntityKeyPin(
                provider_slug="scb",
                source_id="1.44",
                slug="kon",
                register_slug="lisa",
                variable_slug="kon",
            ),
            EntityKeyPin(
                provider_slug="sos",
                source_id="500.LOPNR",
                slug="lopnr",
                register_slug="dors",
                variable_slug="lopnr",
            ),
            # Interleaved: scb again AFTER sos — groupby would split this run.
            EntityKeyPin(
                source_id="1.99",
                provider_slug="scb",
                slug="ar",
                register_slug="lisa",
                variable_slug="ar",
            ),
        ]
        out_dir = tmp_path / "pins"
        written = write_entity_key_pins(pins, out_dir)
        assert set(written) == {"scb", "sos"}
        assert _pin_values(out_dir / "scb.toml") == {"1.44": "kon", "1.99": "ar"}
        assert _pin_values(out_dir / "sos.toml") == {"500.LOPNR": "lopnr"}

    def test_overwrite_guard_refuses_without_force(self, tmp_path: Path):
        """A non-empty `out_dir` (any `*.toml`) refuses without `force`, then
        succeeds with `force=True` — mirrors `seed-slugs`, so pointing `--out-dir`
        at a curated steward dir can't clobber it."""
        pin = EntityKeyPin(
            provider_slug="scb",
            source_id="1.44",
            slug="kon",
            register_slug="lisa",
            variable_slug="kon",
        )
        out_dir = tmp_path / "pins"
        out_dir.mkdir()
        (out_dir / "scb.toml").write_text("# pre-existing\n", encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            write_entity_key_pins([pin], out_dir)
        assert exc.value.code == "entity_key_pins_would_overwrite"

        written = write_entity_key_pins([pin], out_dir, force=True)
        assert set(written) == {"scb"}
        assert _pin_values(out_dir / "scb.toml") == {"1.44": "kon"}


def _file_db(conn: sqlite3.Connection, tmp_path: Path) -> Path:
    """Persist a fixture conn as the `--db` directory the CLI opens.

    `open_built_db` checks the `import_manifest` schema_version, so stamp the
    current one, COMMIT (the sqlite backup API stalls on an open write
    transaction) and copy into `<dir>/reg_meta.db`."""
    from reg_meta_build.db import SCHEMA_VERSION

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    conn.execute(
        "INSERT INTO import_manifest (key, value) VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()
    dest = sqlite3.connect(db_dir / "reg_meta.db")
    conn.backup(dest)
    dest.close()
    conn.close()
    return db_dir


def _cli(
    capsys: pytest.CaptureFixture[str], db: Path, *args: str | Path
) -> tuple[int, dict]:
    """Run `reg-meta-build --db <db> entity-key-pins <args>`; return the exit code
    and the JSON written to stdout (the data payload, or the `error` envelope)."""
    from reg_meta_build.cli import run

    code = run(["--db", str(db), "entity-key-pins", *map(str, args)])
    return code, json.loads(capsys.readouterr().out)


class TestCli:
    """`reg-meta-build entity-key-pins` driven through `cli.run` against a file
    DB. Usage-guard cases point `--db` at a missing directory: each guard fires
    before the DB open, so a missing guard surfaces a different error code."""

    def test_counts_only_the_steward_provider(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        # The DB carries an scb and an sos entity key; the steward dir curates only
        # sos register 500, so the pins are scoped to sos alone.
        conn = _db_with_entity_key("kon")
        _add_sos_entity_key(conn)
        db = _file_db(conn, tmp_path)
        steward_dir = tmp_path / "steward"
        steward_dir.mkdir()
        (steward_dir / "sos.toml").write_text(
            '[register."500"]\nslug = "dors"\n', encoding="utf-8"
        )
        code, data = _cli(capsys, db, "--slug-dir", steward_dir)
        assert code == 0
        assert data["counts"] == {"sos": 1}
        assert data["count"] == 1

    def test_without_slug_dir_is_usage_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        code, data = _cli(capsys, tmp_path / "missing")
        assert code == 2
        assert data["error"]["code"] == "entity_key_pins_flavored_needs_slug_dir"

    def test_slug_dir_that_is_not_a_directory_is_usage_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        code, data = _cli(
            capsys,
            tmp_path / "missing",
            "--slug-dir",
            tmp_path / "does-not-exist",
        )
        assert code == 2
        assert data["error"]["code"] == "slug_dir_not_a_directory"

    def test_checkout_global_slug_root_is_usage_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        # The checkout's global curation root (read only). It both equals the
        # repo slug dir and nests subdirectories, so either global-root marker
        # rejects it; the boundary observes the shared error, not which fired.
        global_root = Path(__file__).resolve().parents[1] / "curation"
        assert any(child.is_dir() for child in global_root.iterdir())
        code, data = _cli(capsys, tmp_path / "missing", "--slug-dir", global_root)
        assert code == 2
        assert data["error"]["code"] == "entity_key_pins_flavored_global_slug_dir"

    def test_slug_dir_nesting_a_directory_is_global_root(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        # Not the checkout's root, but it nests a steward dir (only the global
        # root does) and carries a [register] entry, so the empty-scope guard
        # would pass: the content marker is what rejects it.
        global_root = tmp_path / "fqid_slugs"
        global_root.mkdir()
        (global_root / "sos.toml").write_text(
            '[register."500"]\nslug = "dors"\n', encoding="utf-8"
        )
        (global_root / "swecov").mkdir()
        code, data = _cli(capsys, tmp_path / "missing", "--slug-dir", global_root)
        assert code == 2
        assert data["error"]["code"] == "entity_key_pins_flavored_global_slug_dir"

    def test_slug_dir_without_register_entries_is_usage_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        steward_dir = tmp_path / "steward"
        steward_dir.mkdir()
        (steward_dir / "sos.toml").write_text(
            '[variable."500.LOPNR"]\nslug = "sosvar"\n', encoding="utf-8"
        )
        code, data = _cli(capsys, tmp_path / "missing", "--slug-dir", steward_dir)
        assert code == 2
        assert data["error"]["code"] == "entity_key_pins_flavored_empty_scope"

    def test_out_dir_writes_one_file_per_provider(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        conn = _db_with_entity_key("kon")
        _add_sos_entity_key(conn)
        db = _file_db(conn, tmp_path)
        slug_dir = _slug_dir(tmp_path)
        out_dir = tmp_path / "pins"
        code, data = _cli(capsys, db, "--slug-dir", slug_dir, "--out-dir", out_dir)
        assert code == 0
        assert data["counts"] == {"scb": 1, "sos": 1}
        assert set(data["files"]) == {"scb", "sos"}
        assert _pin_values(out_dir / "scb.toml") == {"1.44": "kon"}
        assert _pin_values(out_dir / "sos.toml") == {"500.LOPNR": "lopnr"}

    def test_counts_in_no_target_and_output_toml_modes(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        conn = _db_with_entity_key("kon")
        _add_sos_entity_key(conn)
        db = _file_db(conn, tmp_path)
        slug_dir = _slug_dir(tmp_path)

        code, data = _cli(capsys, db, "--slug-dir", slug_dir)
        assert code == 0
        assert data["count"] == 2
        assert data["counts"] == {"scb": 1, "sos": 1}
        assert "toml" in data
        assert "out_dir" not in data and "files" not in data

        out_toml = tmp_path / "combined.toml"
        code, data = _cli(capsys, db, "--slug-dir", slug_dir, "--output-toml", out_toml)
        assert code == 0
        assert data["counts"] == {"scb": 1, "sos": 1}
        assert data["output_toml"] == str(out_toml.resolve())

    def test_out_dir_and_output_toml_is_usage_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        code, data = _cli(
            capsys,
            tmp_path / "missing",
            "--slug-dir",
            _slug_dir(tmp_path),
            "--out-dir",
            tmp_path / "pins",
            "--output-toml",
            tmp_path / "combined.toml",
        )
        assert code == 2
        assert data["error"]["code"] == "entity_key_pins_output_conflict"
