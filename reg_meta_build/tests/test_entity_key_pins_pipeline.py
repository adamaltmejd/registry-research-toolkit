"""The entity-key curation gate and `entity-key-pins` on a pipeline-built catalog.

A built catalog's register ids are surrogates minted from the slug path and it
keeps no native register id (#1215), while a curated variable pin is keyed on the
register's native id. In the `shared_var` catalog both registers deliver native
variable 101, so the two entity-key variables differ only by that native register
id (`1.101`, `2.101`)."""

from __future__ import annotations

import json
import tomllib
from typing import TYPE_CHECKING

import pytest
from _curation_partition_boundary_support import (
    REGISTER,
    build_sample,
    partition,
    row,
    summary,
)
from _pipeline_catalog_support import built_db_dir
from reg_meta_build.cli import run
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path

    from _pipeline_catalog_support import CatalogFixture

shared_var = pytest.mark.parametrize("catalog", ["shared_var"], indirect=True)


def _catalog_with_one_pin_dropped(
    catalog: CatalogFixture, tmp_path: Path
) -> tuple[Path, Path]:
    """Build the catalog with both `value` variables keying their register's panel,
    then drop the `other` register's variable pin from the curation tree. Returns
    the `--db` dir and the `other` register file."""
    registers = catalog.curation / "registers" / "scb"
    for path in registers.glob("*.toml"):
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                'slug = "people"\n', 'slug = "people"\npanel_entity_key = "value"\n'
            ),
            encoding="utf-8",
        )
    db_dir = built_db_dir(catalog, tmp_path, registers=("1", "2"))
    other = registers / "other.toml"
    other.write_text(
        other.read_text(encoding="utf-8").split("[[variable]]", 1)[0],
        encoding="utf-8",
    )
    return db_dir, other


@shared_var
def test_gate_keys_pins_on_native_register_id(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    """The pinned `sample/value` (`1.101`) is accepted and the unpinned
    `other/value` is refused under its native key `2.101`. Fails if the gate keys a
    variable on the catalog's surrogate `register_id`: it then refuses both, under
    ids no pin can carry."""
    db_dir, _other = _catalog_with_one_pin_dropped(catalog, tmp_path)

    result = validate_built_db(db_dir / "reg_meta.db", slug_dir=catalog.curation)

    assert result.failures == [
        "other/value (source_id 2.101, panel_entity_key 'value') has no curated "
        "[variable] slug pin"
    ]


@shared_var
def test_generated_pin_satisfies_the_gate(
    catalog: CatalogFixture, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """`entity-key-pins` emits the one missing pin keyed `2.101`; folded into the
    register file, the gate accepts the catalog. Fails if the generator keys pins on
    the surrogate `register_id` (it emits both variables, and the register file
    refuses a pin outside its native id)."""
    db_dir, other = _catalog_with_one_pin_dropped(catalog, tmp_path)
    out = tmp_path / "pins"

    code = run(
        [
            "--db",
            str(db_dir),
            "entity-key-pins",
            "--slug-dir",
            str(catalog.curation),
            "--out-dir",
            str(out),
        ]
    )

    assert code == 0
    assert set(json.loads(capsys.readouterr().out)["files"]) == {"scb/other"}
    pins = (out / "registers" / "scb" / "other.toml").read_text(encoding="utf-8")
    assert tomllib.loads(pins) == {
        "variable": [{"native_id": "2.101", "slug": "value"}]
    }
    other.write_text(other.read_text(encoding="utf-8") + pins, encoding="utf-8")
    result = validate_built_db(db_dir / "reg_meta.db", slug_dir=catalog.curation)
    assert "all 2 entity-key var(s) are curated" in result.format_report()
    assert result.passed, result.failures


def test_gate_keys_a_partitioned_split_sibling_on_its_native_id(
    tmp_path: Path,
) -> None:
    """A partition owner keeps its split id in its provider_key (`5.first`), so the
    gate keys it `1.5.first` and accepts its pin. Fails if the gate refuses a dotted
    provider_key, as it did for 238 entity-key variables of the v0.42.0 catalog
    (#1222)."""
    toml = REGISTER.replace(
        'slug = "people"\n', 'slug = "people"\npanel_entity_key = "first"\n'
    ) + partition({"FIRST": "1.5.first", "SECOND": "1.5.second"})
    built = build_sample(
        tmp_path,
        [row("FIRST", 10), row("SECOND", 11)],
        toml,
        summaries=[summary("FIRST"), summary("SECOND")],
    )

    result = validate_built_db(built.db, slug_dir=tmp_path / "curation")

    assert "all 1 entity-key var(s) are curated" in result.format_report()
    assert result.passed, result.failures
