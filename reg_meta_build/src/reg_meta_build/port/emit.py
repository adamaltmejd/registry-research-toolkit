"""TOML for the converted curation files: deterministic, in insertion order.

Arrays of tables are written as `[[...]]` sections only at the verb tables and their
`link` lists; every other value is inline (`source_curation.toml_inline`). The Rust
reader compares parsed values, so the layout only needs to be valid and stable.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any

from reg_meta_build.source_curation import toml_inline

_BARE = re.compile(r"[A-Za-z0-9_-]+")
# Sub-tables written as their own `[...]` section under an entry, as the golden corpus
# does; any other nested table stays inline.
SECTION_KEYS = frozenset({"guard", "payload", "authority"})
ARRAY_SECTION_KEYS = frozenset({"link"})


def key(name: str) -> str:
    return name if _BARE.fullmatch(name) else json.dumps(name, ensure_ascii=False)


def value(item: Any) -> str:
    if isinstance(item, (dt.date, dt.datetime, dt.time)):
        return item.isoformat()
    if isinstance(item, float):
        return repr(item)
    if isinstance(item, dict):
        return (
            "{ "
            + ", ".join(
                f"{key(k)} = {value(v)}" for k, v in item.items() if v is not None
            )
            + " }"
            if item
            else "{}"
        )
    if isinstance(item, (list, tuple)):
        return "[" + ", ".join(value(v) for v in item) + "]"
    return toml_inline(item)


def _entry(path: str, entry: dict[str, Any], lines: list[str]) -> None:
    sections = []
    for name, item in entry.items():
        if item is None:
            continue
        if name in SECTION_KEYS and isinstance(item, dict):
            sections.append((name, item, False))
        elif (
            name in ARRAY_SECTION_KEYS
            and isinstance(item, list)
            and all(isinstance(v, dict) for v in item)
        ):
            sections.append((name, item, True))
        else:
            lines.append(f"{key(name)} = {value(item)}")
    for name, item, many in sections:
        sub = f"{path}.{key(name)}"
        for part in item if many else (item,):
            lines.append("")
            lines.append(f"[[{sub}]]" if many else f"[{sub}]")
            _entry(sub, part, lines)


def document(header: str, tables: list[tuple[str, Any]]) -> str:
    """A file from `(dotted table path, value)` pairs: a dict is one table, a list of
    dicts an array of tables."""
    lines = [f"# {line}" if line else "#" for line in header.splitlines()]
    for path, item in tables:
        if isinstance(item, list):
            for entry in item:
                lines.append("")
                lines.append(f"[[{path}]]")
                _entry(path, entry, lines)
        else:
            lines.append("")
            lines.append(f"[{path}]")
            _entry(path, item, lines)
    return "\n".join(lines) + "\n"
