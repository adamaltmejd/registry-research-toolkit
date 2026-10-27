#!/usr/bin/env python3
"""Extract the plugin's project schema from the committed server API contract.

Run with `uv run --no-project scripts/gen_project_schema.py`; `--check` refuses drift.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = (
    ROOT / "plugins/microdata-tools-se/skills/register-project/project_data.schema.json"
)
PREFIX = "#/components/schemas/"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    schemas = json.loads((ROOT / "crates/reg-meta/openapi.json").read_text())[
        "components"
    ]["schemas"]
    definitions = {}

    def visit(node):
        if isinstance(node, list):
            for value in node:
                visit(value)
        elif isinstance(node, dict):
            for key, value in node.items():
                if (
                    key == "$ref"
                    and isinstance(value, str)
                    and value.startswith(PREFIX)
                ):
                    name = value.removeprefix(PREFIX)
                    if name not in definitions:
                        definitions[name] = schemas[name]
                        visit(schemas[name])
                else:
                    visit(value)

    visit(schemas["ProjectData"])
    definitions.pop("ProjectData", None)
    document = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "project_data.json",
        **schemas["ProjectData"],
        "$defs": dict(sorted(definitions.items())),
    }
    text = (
        json.dumps(document, indent=2, ensure_ascii=False).replace(
            f'"{PREFIX}', '"#/$defs/'
        )
        + "\n"
    )
    if args.check:
        if OUT.read_text() != text:
            parser.exit(
                1, "Plugin project schema drift: run scripts/gen_project_schema.py\n"
            )
    else:
        OUT.write_text(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
