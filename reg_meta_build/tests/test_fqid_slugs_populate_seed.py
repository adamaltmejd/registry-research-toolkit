"""populate_slugs (register/variant slugs and panel columns) and the freeze-state auto.toml regeneration gate."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _fqid_slug_support import write_text_file as _write
from _slugged_db import (
    add_register,
    add_state,
    add_variable,
    add_variant,
    build_slugged_db,
)
from reg_meta.errors import RegMetaError

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    FREEZE_STATE_FILE,
    load_provider_toml,
    populate_slugs,
    populate_variable_slugs,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


def test_pinned_register_requires_its_auto_file(tmp_path: Path):
    root = tmp_path / "curation"
    directory = root / "registers" / "scb"
    directory.mkdir(parents=True)
    (directory / "lisa.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "1"\n'
    )
    (root / "slug_state.toml").write_text('scb = "curating"\n')
    with pytest.raises(RegMetaError) as exc:
        populate_variable_slugs(build_slugged_db(), root)
    assert exc.value.code == "slug_freeze_auto_missing"


class TestPopulateSlugs:
    def _make_db(self) -> sqlite3.Connection:
        # The slugged-db fixture pre-populates slugs; we want the empty-slug
        # state to test population. Build the DB then clear the slug columns.
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
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
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {"register": 1, "register_variant": 1}
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

    def test_populates_composite_time_key(self, tmp_path: Path):
        # #567: a composite panel_time_key is JSON-encoded into the TEXT column
        # exactly like the composite entity key.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "utrikeshandel-tjanster"\n'
            '[register_variant."1.10"]\n'
            'slug = "_default"\n'
            'panel_entity_key = "peorgnr"\n'
            'panel_time_key = ["ar", "kvartal"]\n'
            'panel_time_grain = "row"\n',
        )
        conn = self._make_db()
        populate_slugs(conn, d, strict=True)
        row = conn.execute(
            "SELECT panel_entity_key, panel_time_key, panel_time_grain "
            "FROM register_variant WHERE register_variant_id = 10"
        ).fetchone()
        assert row["panel_entity_key"] == "peorgnr"
        assert json.loads(row["panel_time_key"]) == ["ar", "kvartal"]
        assert row["panel_time_grain"] == "row"

    def test_panel_columns_null_by_default(self, tmp_path: Path):
        # A variant entry without panel fields leaves the columns NULL.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
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
        conn = self._make_db()
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_missing_for_source_id"

    def test_non_strict_skips_coverage_check(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."1"]\nslug = "lisa"\n')
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
        conn = self._make_db()
        with pytest.raises(RegMetaError) as exc:
            populate_slugs(conn, d, strict=True)
        assert exc.value.code == "slug_unknown_source_id"


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
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {"register": 1, "register_variant": 1}

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
        conn = self._make_db()
        counts = populate_slugs(conn, d, strict=True)
        assert counts == {"register": 1, "register_variant": 1}


class TestFreezeStateAutoRegenerate:
    """The auto-regeneration gate in `populate_variable_slugs` (#470): a
    churning zone re-derives a present `<provider>.auto.toml`; curating/frozen
    pin it (read back, never recompute)."""

    @staticmethod
    def _db(kol: str) -> sqlite3.Connection:
        conn = build_slugged_db(variable=("Kön", 44, 1001, kol))
        conn.execute("UPDATE variable SET slug = NULL")
        conn.commit()
        return conn

    @staticmethod
    def _stored(conn: sqlite3.Connection) -> str | None:
        row = conn.execute(
            "SELECT slug FROM variable WHERE provider_key = CAST(44 AS TEXT)"
        ).fetchone()
        return row[0] if row else None

    def _dir(self, tmp_path: Path, *, freeze: str | None) -> Path:
        (tmp_path / "scb.toml").write_text("", encoding="utf-8")
        # A stale auto.toml pins "kon" for var 1.44 (the column now folds to
        # "konkod"); whether it's honored is the whole question.
        (tmp_path / f"scb{AUTO_FILE_SUFFIX}").write_text(
            '[variable."1.44"]\nslug = "kon"\n', encoding="utf-8"
        )
        if freeze is not None:
            (tmp_path / FREEZE_STATE_FILE).write_text(
                f'scb = "{freeze}"\n', encoding="utf-8"
            )
        return tmp_path

    def test_churning_rederives_present_auto(self, tmp_path: Path) -> None:
        # Default (no freeze.toml) ⇒ churning ⇒ the committed auto slug is
        # IGNORED and the slug re-derives from the current column ("konkod").
        conn = self._db(kol="Konkod")
        d = self._dir(tmp_path, freeze=None)
        counts = populate_variable_slugs(conn, d)
        assert self._stored(conn) == "konkod"
        assert counts["auto_existing"] == 0
        assert counts["auto_new"] == 1
        # The gitignored auto.toml is still rewritten with the fresh slug.
        slugs = [
            e.slug
            for e in load_provider_toml(d / f"scb{AUTO_FILE_SUFFIX}")
            if e.kind == "variable"
        ]
        assert slugs == ["konkod"]

    @pytest.mark.parametrize("state", ["curating", "frozen"])
    def test_curating_and_frozen_pin_present_auto(
        self, tmp_path: Path, state: str
    ) -> None:
        # curating/frozen ⇒ the committed auto slug is read back and kept even
        # though the column now folds to "konkod".
        conn = self._db(kol="Konkod")
        d = self._dir(tmp_path, freeze=state)
        counts = populate_variable_slugs(conn, d)
        assert self._stored(conn) == "kon"
        assert counts["auto_existing"] == 1
        assert counts["auto_new"] == 0

    @staticmethod
    def _dir_no_auto(tmp_path: Path, *, freeze: str | None) -> Path:
        # Like `_dir` but WITHOUT committing the auto.toml — the maintainer-forgot
        # case (`*.auto.toml` is gitignored, so a pin needs `git add -f`).
        (tmp_path / "scb.toml").write_text("", encoding="utf-8")
        if freeze is not None:
            (tmp_path / FREEZE_STATE_FILE).write_text(
                f'scb = "{freeze}"\n', encoding="utf-8"
            )
        return tmp_path

    @pytest.mark.parametrize("state", ["curating", "frozen"])
    def test_missing_auto_raises_when_pinned(self, tmp_path: Path, state: str) -> None:
        # #471 pin guard: a curating/frozen provider WITH variables but no
        # committed auto.toml must fail loud, not silently re-derive (which would
        # defeat the pin).
        conn = self._db(kol="Kon")
        d = self._dir_no_auto(tmp_path, freeze=state)
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_freeze_auto_missing"
        assert "scb" in exc.value.message
        assert state in exc.value.message
        # The remediation must point the maintainer at the concrete fix — the
        # exact gitignored file to force-add and the command to do it with.
        assert "scb.auto.toml" in exc.value.remediation
        assert "git add -f" in exc.value.remediation

    def test_missing_auto_does_not_raise_when_churning(self, tmp_path: Path) -> None:
        # The critical false-fire regression: the default (churning) provider
        # legitimately has no committed auto.toml and re-derives every build. The
        # #471 guard lives strictly inside the curating/frozen branch, so churning
        # must be UNAFFECTED.
        conn = self._db(kol="Kon")
        d = self._dir_no_auto(tmp_path, freeze=None)
        counts = populate_variable_slugs(conn, d)  # must NOT raise
        assert self._stored(conn) == "kon"
        assert counts["auto_new"] == 1

    def test_missing_auto_guard_is_per_provider(self, tmp_path: Path) -> None:
        # #471 pin guard fires inside a per-provider loop, so it must be scoped to
        # the OFFENDING provider — not the first one seen, and not a churning
        # sibling. Here `scb` is curating with no committed auto.toml (must fire),
        # while a second live provider (`fohm`, which sorts BEFORE `scb` in the
        # `_live_providers` ORDER BY p.slug loop) is churning with no auto.toml
        # (must NOT fire — churning legitimately re-derives every build). A
        # loop-scoping bug that mis-attributed the state would either skip `scb`
        # or wrongly flag the churning sibling.
        conn = self._db(kol="Kon")  # scb register/variant/variable
        # Wire a genuine second live provider (fohm, provider_id 3) the same way
        # the fixture seeds scb: a register + variant + variable + state era.
        add_register(
            conn,
            register_id=2,
            slug="folkbokforing",
            name="Folkbokföring",
            provider_id=3,
        )
        add_variant(
            conn,
            register_variant_id=20,
            register_id=2,
            slug="personer",
            name="Personer",
        )
        add_variable(conn, register_id=2, var_id=77, name="Kommun", slug=None)
        add_state(
            conn,
            register_id=2,
            var_id=77,
            register_variant_id=20,
            delivery_column_name="Kommun",
        )
        conn.commit()
        # scb is pinned but missing its auto.toml; fohm stays churning (unlisted).
        d = self._dir_no_auto(tmp_path, freeze="curating")
        (tmp_path / "fohm.toml").write_text("", encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_freeze_auto_missing"
        assert "scb" in exc.value.message
        assert "fohm" not in exc.value.message  # the churning sibling is not flagged

    @pytest.mark.parametrize("state", ["curating", "frozen"])
    def test_missing_auto_skipped_for_variableless_provider(
        self, tmp_path: Path, state: str
    ) -> None:
        # The guard gates on the provider HAVING variables: `_live_providers`
        # returns providers with >= 1 register (a superset of those with variable
        # rows), so a register-only provider has no slugs to pin and must not be
        # flagged even when pinned.
        conn = build_slugged_db(variable=None)  # scb register, zero variables
        d = self._dir_no_auto(tmp_path, freeze=state)
        populate_variable_slugs(conn, d)  # must NOT raise
