"""Provider slug TOML loading: entry shapes, panel fields, slug grammar and reserved/duplicate slugs, and declared column ownership."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _fqid_slug_support import write_text_file as _write
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_tree import load_register_files

from reg_meta_build.fqid_slugs import (
    SlugEntry,
    declared_column_ownership,
    load_provider_toml,
    load_slug_dir,
)

if TYPE_CHECKING:
    from pathlib import Path


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

    def test_display_group_whitespace_trimmed(self, tmp_path: Path):
        # SCB names carry stray whitespace that the seed inherits; the read
        # boundary trims it so the built label is clean. A whitespace-only
        # value collapses to no label.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\n'
            'slug = "individer-15plus"\n'
            'display_group = "Individer  "\n'
            '[register_variant."34.154"]\n'
            'slug = "blank"\n'
            'display_group = "   "\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].display_group == "Individer"
        assert entries[1].display_group is None

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

    def test_variant_with_panel_composite_time_key(self, tmp_path: Path):
        # #567: list panel_time_key → stored as a tuple on the entry (mirrors the
        # composite entity key). UHT's (year, quarter) time coordinate.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\n'
            'slug = "utrikeshandel-tjanster"\n'
            'panel_entity_key = "peorgnr"\n'
            'panel_time_key = ["ar", "kvartal"]\n'
            'panel_time_grain = "row"\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].panel_entity_key == "peorgnr"
        assert entries[0].panel_time_key == ("ar", "kvartal")
        assert entries[0].panel_time_grain == "row"

    def test_variant_composite_time_key_rejects_period_element(self, tmp_path: Path):
        # #567: "period" is the single-only delivery-aligned sentinel — it may not
        # appear INSIDE a composite list.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\nslug = "x"\n'
            'panel_time_key = ["ar", "period"]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "period" in exc.value.message

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

    def test_panel_ref_reserved_slug_rejected(self, tmp_path: Path):
        # Codex P2 on #228: a panel key references a VARIABLE slug, so it's
        # validated against the variable slot — a reserved HTTP-suffix token
        # (which no variable can ever be slugged with) is dangling metadata and
        # must fail at build time, not silently persist (esp. under --no-validate).
        path = tmp_path / "scb.toml"
        _write(
            path,
            '[register_variant."34.153"]\nslug = "x"\npanel_entity_key = "variants"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "reserved" in exc.value.message
        _write(
            path,
            '[register_variant."34.153"]\nslug = "x"\npanel_time_key = "states"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "reserved" in exc.value.message

    def test_panel_time_key_period_and_valid_slug_still_accepted(self, tmp_path: Path):
        # Regression for the Codex P2 fix: the "period" sentinel is exempted
        # BEFORE the variable-slot validation runs, and a bare valid variable-slug
        # reference still passes — neither is affected by the reserved-token gate.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\nslug = "x"\n'
            'panel_time_key = "period"\npanel_entity_key = "kon"\n',
        )
        entry = load_provider_toml(path)[0]
        assert entry.panel_time_key == "period"
        assert entry.panel_entity_key == "kon"

    @pytest.mark.parametrize(
        "panel_lines",
        [
            # "period" sentinel is delivery-aligned; "row" contradicts it.
            'panel_time_key = "period"\npanel_time_grain = "row"\n',
            # A bare slug key is a per-row coordinate; "delivery" contradicts it.
            'panel_time_key = "manad"\npanel_time_grain = "delivery"\n',
            # A composite key is likewise per-row; "delivery" contradicts it.
            'panel_time_key = ["ar", "kvartal"]\npanel_time_grain = "delivery"\n',
        ],
    )
    def test_variant_panel_time_key_grain_mismatch_rejected(
        self, tmp_path: Path, panel_lines: str
    ):
        # #576: panel_time_key and panel_time_grain must agree — the delivery-aligned
        # "period" sentinel pairs only with "delivery", a slug/composite key only
        # with "row". A contradictory pair would render an incoherent panel axis.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\nslug = "x"\n' + panel_lines,
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        # Both kind_desc branches ("period" sentinel vs slug/composite) end the
        # message with `expected {grain!r}.`, so every reject case names the
        # expected grain.
        assert "expected" in exc.value.message

    @pytest.mark.parametrize(
        ("panel_lines", "expected_key", "expected_grain"),
        [
            (
                'panel_time_key = "period"\npanel_time_grain = "delivery"\n',
                "period",
                "delivery",
            ),
            ('panel_time_key = "period"\n', "period", None),  # grain optional
            ('panel_time_key = "manad"\npanel_time_grain = "row"\n', "manad", "row"),
            ('panel_time_key = "manad"\n', "manad", None),  # grain optional
            (
                'panel_time_key = ["ar", "kvartal"]\npanel_time_grain = "row"\n',
                ("ar", "kvartal"),
                "row",
            ),
            (
                'panel_time_key = ["ar", "kvartal"]\n',
                ("ar", "kvartal"),
                None,
            ),  # grain optional for composite
        ],
    )
    def test_variant_panel_time_key_grain_consistent_accepted(
        self,
        tmp_path: Path,
        panel_lines: str,
        expected_key: str | tuple[str, ...],
        expected_grain: str | None,
    ):
        # #576: a consistent pair (or a key with grain unset — the coupling stays
        # optional) loads cleanly.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\nslug = "x"\n' + panel_lines,
        )
        entry = load_provider_toml(path)[0]
        assert entry.panel_time_key == expected_key
        assert entry.panel_time_grain == expected_grain

    def test_variant_panel_time_grain_without_key_rejected(self, tmp_path: Path):
        # #576: grain is optional, but a lone `panel_time_grain` (no
        # `panel_time_key`) qualifies a time key that does not exist — incoherent
        # metadata — and must fail at load, not silently persist.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.153"]\nslug = "x"\npanel_time_grain = "row"\n',
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

    def test_reserved_http_suffix_slug_rejected_in_register(self, tmp_path: Path):
        # A binding-suffix token (`states`) would shadow the
        # `/catalog/{fqid:path}/states` route if minted as a register slug.
        path = _write(
            tmp_path / "scb.toml",
            '[register."34"]\nslug = "states"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "reserved" in exc.value.message

    def test_reserved_http_suffix_slug_rejected_in_variable(self, tmp_path: Path):
        # The variable slot reserves both the 6 binding suffixes AND `variants`
        # (the `/{provider}/{register}/variants` sub-resource shadows the 3-seg
        # variable leaf).
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4"]\nslug = "variants"\n',
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "reserved" in exc.value.message

    def test_reserved_http_suffix_slug_allowed_for_register_variant(
        self, tmp_path: Path
    ):
        # register_variant rides a `?variant=` query value, never a path segment,
        # so it carries no reservation — `states` is a valid variant slug.
        path = _write(
            tmp_path / "scb.toml",
            '[register_variant."34.0"]\nslug = "states"\n',
        )
        entries = load_provider_toml(path)
        assert entries[0].kind == "register_variant"
        assert entries[0].slug == "states"

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

    # Checked register identity declarations authorize retaining a base slug.
    _BASE = '[variable."34.4"]\nslug = "kon"\n'
    _SPLIT = '[variable."34.4.kon"]\nslug = "kon"\n'

    def _write_partition(self, tmp_path: Path, body: str) -> tuple[Path, Path]:
        slug_dir = tmp_path / "fqid_slugs"
        slug_dir.mkdir(exist_ok=True)
        slug_path = _write(
            slug_dir / "scb.toml",
            '[register."34"]\nslug = "lisa"\n' + self._BASE + self._SPLIT,
        )
        curation = tmp_path / "curation"
        register_path = curation / "registers" / "scb" / "lisa.toml"
        register_path.parent.mkdir(parents=True, exist_ok=True)
        register_path.write_text(
            '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n\n' + body,
            encoding="utf-8",
        )
        return slug_path, curation

    def test_split_with_partition_reuses_own_base_slug(self, tmp_path: Path):
        body = (
            '[[identity.partition]]\nvariable = "34.4"\n'
            'columns = { Kon = "34.4.kon" }\n'
            'columns_ref = "curation evidence"\n'
        )
        slug_path, _curation = self._write_partition(tmp_path, body)
        entries = load_provider_toml(slug_path)
        assert {e.source_id: e.slug for e in entries} == {
            "34": "lisa",
            "34.4": "kon",
            "34.4.kon": "kon",
        }

    def test_split_without_partition_cannot_reuse_base_slug(self, tmp_path: Path):
        slug_path, curation = self._write_partition(tmp_path, "")
        assert load_provider_toml(slug_path)
        register = curation / "registers" / "scb" / "lisa.toml"
        register.write_text(
            register.read_text()
            + '[[variable]]\nnative_id = "34.4"\nslug = "kon"\n[[variable]]\nnative_id = "34.4.kon"\nslug = "kon"\n'
        )
        with pytest.raises(RegMetaError) as exc:
            load_slug_dir(curation)
        assert exc.value.code == "slug_toml_invalid"

    @pytest.mark.parametrize("owner", ["34.4.kon", "34.4.other"])
    def test_finite_column_owner_retains_only_its_own_base_slug(
        self, tmp_path: Path, owner: str
    ) -> None:
        body = (
            '[[identity.column_owner]]\nvariable = "34.4"\nvariant = "34.44"\n'
            f'column = "Kon"\nowner = "{owner}"\nsource_editions = ["2023"]\n'
            'ref = "documented source ownership"\n'
            '[[variable]]\nnative_id = "34.4"\nslug = "kon"\n'
            '[[variable]]\nnative_id = "34.4.kon"\nslug = "kon"\n'
        )
        _, curation = self._write_partition(tmp_path, body)
        if owner == "34.4.kon":
            assert {e.source_id for e in load_slug_dir(curation)} >= {
                "34.4",
                "34.4.kon",
            }
        else:
            with pytest.raises(RegMetaError) as exc:
                load_slug_dir(curation)
            assert "reused" in exc.value.message

    @pytest.mark.parametrize("auto_base", [False, True])
    @pytest.mark.parametrize("declared_owner", ["current", "undeclared"])
    def test_sos_split_retains_only_declared_owner_base_slug(
        self, tmp_path: Path, auto_base: bool, declared_owner: str
    ) -> None:
        curation = tmp_path / "curation"
        directory = curation / "registers" / "sos"
        directory.mkdir(parents=True)
        base = '[[variable]]\nnative_id = "123.EXAMAR"\nslug = "examar"\n'
        body = (
            '[register]\nprovider = "sos"\nslug = "lova"\nnative_id = "123"\n'
            '[[identity.split]]\nvariable = "EXAMAR"\nby = "data_type"\n'
            f'parts = [{{ data_type = "integer", owner = "123.EXAMAR.{declared_owner}" }}, '
            '{ data_type = "text", owner = "123.EXAMAR.highest" }]\n'
            '[[variable]]\nnative_id = "123.EXAMAR.current"\nslug = "examar"\n'
        )
        if auto_base:
            (curation / "slug_state.toml").write_text('sos = "curating"\n')
            (directory / "lova.auto.toml").write_text(base)
        else:
            body += base
        (directory / "lova.toml").write_text(body)
        if declared_owner == "current":
            entries = load_slug_dir(curation)
            assert {e.source_id for e in entries} >= {
                "123.EXAMAR",
                "123.EXAMAR.current",
            }
        else:
            with pytest.raises(RegMetaError) as exc:
                load_slug_dir(curation)
            assert "reused" in exc.value.message

    @pytest.mark.parametrize(
        "field",
        ['columns = { Kon = "34.4.kon" }\n', 'columns_ref = "reference"\n'],
    )
    def test_slug_entries_reject_removed_ownership_fields(
        self, tmp_path: Path, field: str
    ) -> None:
        path = _write(
            tmp_path / "scb.toml",
            '[variable."34.4.kon"]\nslug = "kon"\n' + field,
        )
        with pytest.raises(RegMetaError) as exc:
            load_provider_toml(path)
        assert exc.value.code == "slug_toml_invalid"
        assert "columns" in exc.value.message

    def test_register_file_partition_loads_as_naming_ownership(self, tmp_path: Path):
        body = (
            '[[identity.partition]]\nvariable = "34.4"\n'
            'columns = { Kon = "34.4.kon" }\n'
            'columns_ref = "curation evidence"\n'
        )
        slug_path, curation = self._write_partition(tmp_path, body)
        ownership = declared_column_ownership(
            load_provider_toml(slug_path),
            provider="scb",
            source_id="34.4",
            curation_dir=curation,
        )
        assert ownership.split_ids == ("34.4.kon",)
        assert dict(ownership.declared_columns) == {"Kon": "34.4.kon"}
        assert ownership.declaration_reference == "curation evidence"

    def test_columns_ref_is_required(self, tmp_path: Path):
        body = (
            '[[identity.partition]]\nvariable = "34.4"\n'
            'columns = { Kon = "34.4.kon" }\n'
        )
        _slug_path, curation = self._write_partition(tmp_path, body)
        with pytest.raises(RegMetaError) as exc:
            load_register_files(curation)
        assert "columns_ref" in exc.value.message

    def test_partition_without_slug_siblings_is_unresolved(self, tmp_path: Path):
        slug_dir = tmp_path / "fqid_slugs"
        slug_dir.mkdir()
        slug_path = _write(
            slug_dir / "scb.toml",
            '[register."34"]\nslug = "lisa"\n[variable."34.4.kon"]\nslug = "kon"\n',
        )
        curation = tmp_path / "curation"
        register_path = curation / "registers" / "scb" / "lisa.toml"
        register_path.parent.mkdir(parents=True, exist_ok=True)
        register_path.write_text(
            '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n',
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="no declared literal column ownership"):
            declared_column_ownership(
                load_provider_toml(slug_path),
                provider="scb",
                source_id="34.4",
                curation_dir=curation,
            )

    def test_duplicate_partition_fails(self, tmp_path: Path):
        body = (
            '[[identity.partition]]\nvariable = "34.4"\n'
            'columns = { Kon = "34.4.kon" }\n\n'
            'columns_ref = "first"\n\n'
            '[[identity.partition]]\nvariable = "34.4"\n'
            'columns = { Kon2 = "34.4.kon" }\n'
            'columns_ref = "second"\n'
        )
        slug_path, curation = self._write_partition(tmp_path, body)
        with pytest.raises(RegMetaError) as exc:
            declared_column_ownership(
                load_provider_toml(slug_path),
                provider="scb",
                source_id="34.4",
                curation_dir=curation,
            )
        assert "duplicate [[identity.partition]]" in exc.value.message
