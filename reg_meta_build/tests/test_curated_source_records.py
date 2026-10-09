"""The committed thin-provider slug pins follow the columns their authored TOML declares."""

from __future__ import annotations

import tomllib
from pathlib import Path

from reg_meta_build.slug_grammar import derive_variable_slug


def test_fohm_thin_slug_entries_follow_authored_columns() -> None:
    root = Path(__file__).parents[1]
    source = tomllib.loads(
        (root / "input_data/Folkhalsomyndigheten/fohm.toml").read_text()
    )
    for register, columns in (
        ("sminet", {"provtagningsdatum", "statistikdatum"}),
        ("nvr", {"nplid"}),
    ):
        declared = next(item for item in source["register"] if item["key"] == register)
        assert columns <= {item["column"] for item in declared["variable"]}
        auto = tomllib.loads(
            (root / f"curation/registers/fohm/{register}.auto.toml").read_text()
        )
        for column in columns:
            entry = next(
                item
                for item in auto["variable"]
                if item["native_id"].endswith(f".{column}")
            )
            assert entry["slug"] == derive_variable_slug(column)
