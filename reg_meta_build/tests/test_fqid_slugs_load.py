"""Slug directory loading and load-time validation: cross-file slug reuse, source-ID shape, field types, unknown tables, and freeze.toml parsing."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _fqid_slug_support import write_text_file as _write
from _slugged_db import (
    build_slugged_db,
)
from reg_meta.errors import RegMetaError

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    FREEZE_STATE_FILE,
    freeze_state,
    frozen_zones,
    load_freeze_states,
    load_provider_toml,
    load_slug_dir,
    pinned_zones,
    populate_slugs,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_register_slug_reused_across_files_fails(tmp_path: Path):
    root = tmp_path / "curation" / "registers" / "scb"
    (root / "family").mkdir(parents=True)
    (root / "lisa.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "1"\n'
    )
    (root / "family" / "lisa.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "2"\n'
    )
    with pytest.raises(RegMetaError) as exc:
        load_slug_dir(tmp_path / "curation")
    assert "duplicate register" in exc.value.message


def test_variant_slug_reused_within_register_fails(tmp_path: Path):
    root = tmp_path / "curation" / "registers" / "scb"
    root.mkdir(parents=True)
    (root / "lisa.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "same"\n'
        '[[variant]]\nnative_id = "1.11"\nslug = "same"\n'
    )
    with pytest.raises(RegMetaError) as exc:
        load_slug_dir(tmp_path / "curation")
    assert "slug 'same' reused" in exc.value.message


def test_variable_slug_reused_across_authored_and_auto_fails(tmp_path: Path):
    root = tmp_path / "curation"
    directory = root / "registers" / "scb"
    directory.mkdir(parents=True)
    (directory / "lisa.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "1"\n'
        '[[variable]]\nnative_id = "1.44"\nslug = "same"\n'
    )
    (directory / "lisa.auto.toml").write_text(
        '[[variable]]\nnative_id = "1.45"\nslug = "same"\n'
    )
    (root / "slug_state.toml").write_text('scb = "curating"\n')
    with pytest.raises(RegMetaError) as exc:
        load_slug_dir(root)
    assert "variable slug 'same' reused" in exc.value.message


class TestLoadSlugDir:
    def test_empty_dir(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        assert load_slug_dir(d) == []

    def test_missing_dir(self, tmp_path: Path):
        with pytest.raises(RegMetaError) as exc:
            load_slug_dir(tmp_path / "missing")
        assert exc.value.code == "slug_dir_not_found"

    def test_loads_provider_toml(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."34"]\nslug = "lisa"\n')
        entries = load_slug_dir(d)
        assert [e.kind for e in entries] == ["register"]


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


class TestRepoSlugDir:
    """`repo_slug_dir()` returns the live directory in a repo checkout."""

    def test_repo_layout_resolves(self):
        from reg_meta_build.fqid_slugs import repo_slug_dir

        result = repo_slug_dir()
        assert result is not None
        assert result.is_dir()
        assert (result / "registers" / "scb" / "lisa.toml").is_file()


class TestGraphSemanticsRejectedInSlugToml:
    """#522: graph semantics (same_as / replaced_by succession) are NOT a slug
    surface anymore — they moved to `curation/relations.toml`. An inline `same_as`
    field or a top-level `[[replaced_by]]` array in a slug TOML must now fail as an
    unknown key, not silently no-op."""

    def test_inline_same_as_rejected_as_unknown_field(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.137"]\nslug = "civilstand"\n'
            'same_as = [{ provider = "scb", register = "lisa", '
            'variable_slug = "civilstand-legacy" }]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "same_as" in exc.value.message

    def test_toplevel_replaced_by_array_rejected(self, tmp_path: Path):
        # The succession `[[replaced_by]]` array (note: the per-entry scalar
        # `replaced_by` rename pointer survives — a different relation).
        path = _write(
            tmp_path / "scb.toml",
            '[[replaced_by]]\nfrom = "scb/lisa/old"\nto = "scb/lisa/new"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "replaced_by" in exc.value.message


class TestCanonicalIntegerKeys:
    """`"1.10"` and `"1.010"` must not alias the same DB row. Reject the
    leading-zero form at populate-time so the maintainer gets a loud error
    instead of silent collisions."""

    def _db_with_register_1_10(self):
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        conn.commit()
        return conn

    def test_leading_zero_register_key_rejected(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", '[register."01"]\nslug = "lisa"\n')
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

    def test_provider_lineage_blocks_are_not_identity_input(self, tmp_path: Path):
        path = _write(
            tmp_path / "scb.toml",
            '[lineage_defaults]\nrtb = "folkbokforda-personer"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "lineage_defaults" in exc.value.message


class TestLoadFreezeStates:
    """`freeze.toml` parsing + the `freeze_state`/`frozen_zones` accessors."""

    @staticmethod
    def _dir(tmp_path: Path, *, providers=("scb",), freeze_body: str | None = None):
        for p in providers:
            (tmp_path / f"{p}.toml").write_text("", encoding="utf-8")
        if freeze_body is not None:
            (tmp_path / FREEZE_STATE_FILE).write_text(freeze_body, encoding="utf-8")
        return tmp_path

    def test_absent_file_is_empty_all_churning(self, tmp_path: Path) -> None:
        d = self._dir(tmp_path)
        states = load_freeze_states(d)
        assert states == {}
        assert freeze_state(states, "scb") == "churning"
        assert frozen_zones(states) == frozenset()

    def test_valid_map(self, tmp_path: Path) -> None:
        d = self._dir(
            tmp_path,
            providers=("scb", "sos"),
            freeze_body='scb = "frozen"\nsos = "curating"\n',
        )
        states = load_freeze_states(d)
        assert states == {"scb": "frozen", "sos": "curating"}
        assert freeze_state(states, "scb") == "frozen"
        assert freeze_state(states, "sos") == "curating"
        # An unlisted (but present) provider still defaults churning.
        assert freeze_state(states, "unlisted") == "churning"
        assert frozen_zones(states) == frozenset({"scb"})
        # `pinned_zones` is the auto.toml-OWES set: both frozen AND curating are
        # pinned (the non-obvious "frozen treated same as curating" branch).
        assert pinned_zones(states) == frozenset({"scb", "sos"})

    def test_unknown_state_value_rejected(self, tmp_path: Path) -> None:
        d = self._dir(tmp_path, freeze_body='scb = "thawing"\n')
        with pytest.raises(RegMetaError) as exc:
            load_freeze_states(d)
        assert exc.value.code == "slug_freeze_state_invalid"
        assert "thawing" in exc.value.message

    def test_non_string_value_rejected(self, tmp_path: Path) -> None:
        d = self._dir(tmp_path, freeze_body="scb = true\n")
        with pytest.raises(RegMetaError) as exc:
            load_freeze_states(d)
        assert exc.value.code == "slug_freeze_state_invalid"

    def test_unknown_zone_rejected(self, tmp_path: Path) -> None:
        # `ghost` is neither a provider stem in this dir nor the reserved zone.
        d = self._dir(tmp_path, freeze_body='ghost = "frozen"\n')
        with pytest.raises(RegMetaError) as exc:
            load_freeze_states(d)
        assert exc.value.code == "slug_freeze_zone_unknown"
        assert "ghost" in exc.value.message

    def test_auto_toml_is_not_a_zone(self, tmp_path: Path) -> None:
        # A `<provider>.auto.toml` doesn't introduce a separate zone — its
        # provider is already covered by the curated companion.
        (tmp_path / "scb.toml").write_text("", encoding="utf-8")
        (tmp_path / f"scb{AUTO_FILE_SUFFIX}").write_text("", encoding="utf-8")
        (tmp_path / FREEZE_STATE_FILE).write_text('scb = "frozen"\n', encoding="utf-8")
        assert load_freeze_states(tmp_path) == {"scb": "frozen"}
        # But naming the auto stem as a zone is rejected — `scb.auto` is no zone.
        (tmp_path / FREEZE_STATE_FILE).write_text(
            '"scb.auto" = "frozen"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            load_freeze_states(tmp_path)
        assert exc.value.code == "slug_freeze_zone_unknown"
