#!/usr/bin/env python3
"""rebuild_transcribe — the thin-provider TOMLs into the transcription format, once.

    uv run --no-project scripts/rebuild_transcribe.py <curation dir> <inputs root> <out dir>

Temporary (REBUILD.md, milestone A; deleted at F). Reads today's hand transcriptions
(`<curation dir>/providers/*.toml` and `scb_canonical/scb_canonical.toml`) and writes the
format `crates/reg-readers/src/transcription.rs` reads, one directory per provider under
`<out dir>`. Value sets named by `value_set` are copied from
`<inputs root>/catalog/source/scb_canonical/`. A variable without `variants` is in every
table of its register; an omitted flag is `false`; `classification` becomes the code
system it names. `data_warning` and a table's `period_scope` have no home before
milestone D and are listed on stdout, as is anything else the format drops.
"""

from __future__ import annotations

import csv
import shutil
import sys
import tomllib
from pathlib import Path

CANONICAL = "catalog/source/scb_canonical"
# The provider of scb_canonical.toml's registers.
CANONICAL_PROVIDER = "scb"
VARIABLE_KEYS = {
    "name",
    "column",
    "definition",
    "data_type",
    "measurement_unit",
    "is_identifier",
    "is_sensitive",
    "valid_from",
    "valid_to",
    "variants",
    "classification",
    "value_set",
}


def flag(value: object) -> str:
    return "true" if value is True else "false"


def write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        out = csv.writer(fh, lineterminator="\n")
        out.writerow(header)
        out.writerows(rows)


def transcribe(source: Path, provider: str, inputs: Path, out: Path) -> list[str]:
    dropped = []
    doc = tomllib.loads(source.read_text(encoding="utf-8"))
    registers, tables = [], []
    for reg in doc["register"]:
        key = reg["key"]
        registers.append(
            [
                key,
                reg["name"],
                reg.get("purpose", "").strip(),
                reg.get("valid_from", ""),
                reg.get("valid_to", ""),
            ]
        )
        variants = [v["key"] for v in reg.get("variant", [])]
        for v in reg.get("variant", []):
            tables.append(
                [
                    key,
                    v["key"],
                    v["name"],
                    v.get("description", "").strip(),
                    v.get("valid_from", ""),
                    v.get("valid_to", ""),
                ]
            )
            if "period_scope" in v:
                dropped.append(
                    f"{provider}/{key}/{v['key']}: period_scope = {v['period_scope']!r}"
                )
        rows = []
        for var in reg["variable"]:
            for extra in sorted(set(var) - VARIABLE_KEYS):
                dropped.append(f"{provider}/{key}/{var['column']}: {extra}")
            codes = ""
            if name := var.get("value_set"):
                codes = f"@{name}"
                shutil.copyfile(
                    inputs / CANONICAL / f"{name}.csv",
                    out / provider / "codes" / f"{name}.csv",
                )
            rows.append(
                [
                    ";".join(var.get("variants") or variants),
                    var["column"],
                    var["name"],
                    var.get("definition", "").strip(),
                    var["data_type"],
                    var.get("measurement_unit", ""),
                    codes,
                    var.get("classification", ""),
                    flag(var.get("is_sensitive")),
                    flag(var.get("is_identifier")),
                    var.get("valid_from", ""),
                    var.get("valid_to", ""),
                ]
            )
        write(
            out / provider / f"{key}.csv",
            [
                "table",
                "column",
                "label",
                "definition",
                "data_type",
                "unit",
                "codes",
                "code_system",
                "sensitive",
                "identifier",
                "from",
                "to",
            ],
            rows,
        )
    write(
        out / provider / "registers.csv",
        ["register", "name", "description", "from", "to"],
        registers,
    )
    if tables:
        write(
            out / provider / "tables.csv",
            ["register", "table", "name", "description", "from", "to"],
            tables,
        )
    return dropped


def main() -> int:
    curation, inputs, out = map(Path, sys.argv[1:4])
    sources = [(p, p.stem) for p in sorted((curation / "providers").glob("*.toml"))]
    sources.append((curation / "scb_canonical/scb_canonical.toml", CANONICAL_PROVIDER))
    for source, provider in sources:
        (out / provider / "codes").mkdir(parents=True, exist_ok=True)
        for line in transcribe(source, provider, inputs, out):
            print(f"dropped: {line}")
        codes = out / provider / "codes"
        if not any(codes.iterdir()):
            codes.rmdir()
    return 0


if __name__ == "__main__":
    sys.exit(main())
