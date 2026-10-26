#!/usr/bin/env python3
"""rebuild_ledger — today's slug pins into the rebuild's id ledgers.

    uv run --no-project scripts/rebuild_ledger.py <pins dir> <frames dir> <catalog db> \
        <curation dir> <pins commit> <provider>...

Temporary (REBUILD.md): milestone A ran it, B runs it for the SCB ledgers, and it is
deleted at F. Writes `<curation dir>/registers/<provider>/<register>/ids.toml` for
every register of the named providers in the frames `reg-build read` wrote to
`<frames dir>`, from:

- the variable pins in `<pins dir>/<provider>/<register>.auto.toml`, shadowed by the
  hand file `<register>.toml` (`[variable]`, `"<native id>.<column>" = slug`);
- the table slugs of the old catalog `<catalog db>` (`register_variant`), matched to a
  frame table by its display group, slug or name.

Each id anchors on the first table, in table order, documenting its column. Pins that
name a split part (`<native id>.<column>.<part>`) and pins whose column no table
documents are listed on stdout and left out: they have no anchor until milestone D.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tomllib
from collections import defaultdict
from pathlib import Path


def fold(text: str) -> str:
    return text.strip().lower()


def quote(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)


def frames(directory: Path) -> dict[tuple[str, str], dict[str, set[str]]]:
    """(provider, register) -> table handle -> folded columns."""
    out: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(dict)
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            key = (rec.get("provider"), rec.get("register"))
            if rec["kind"] == "table":
                out[key].setdefault(rec["table"], set())
            elif rec["kind"] == "occurrence":
                out[key].setdefault(rec["table"], set()).add(fold(rec["column"]))
    return out


def pins(directory: Path, provider: str, register: str) -> dict[str, str]:
    """`<native>.<column>[.<part>]` -> slug, hand overrides shadowing the auto pins."""
    out: dict[str, str] = {}
    for name in (f"{register}.auto.toml", f"{register}.toml"):
        path = directory / provider / name
        if path.is_file():
            out |= tomllib.loads(path.read_text(encoding="utf-8")).get("variable", {})
    return out


def table_slugs(db: Path) -> dict[tuple[str, str], dict[str, str]]:
    """(provider, register) -> folded display group, slug or name -> table slug."""
    out: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    rows = sqlite3.connect(db).execute(
        "SELECT p.slug, r.slug, v.display_group, v.slug, v.name FROM register_variant v"
        " JOIN register r USING (register_id) JOIN provider p USING (provider_id)"
        " WHERE v.slug IS NOT NULL ORDER BY v.slug"
    )
    for provider, register, group, slug, name in rows:
        for handle in (name, group, slug):
            if handle:
                out[(provider, register)][fold(handle)] = slug
    return out


def ledger(
    tables: dict[str, set[str]], slugs: dict[str, str], pinned: dict[str, str]
) -> tuple[list[str], list[str]]:
    lines = ["[tables]"]
    lines += [
        f"{quote(t)} = {quote(slugs[fold(t)])}"
        for t in sorted(tables)
        if t and fold(t) in slugs
    ]
    lines += ["", "[variables]"]
    skipped = []
    anchors: dict[str, list[str]] = defaultdict(list)
    for key, slug in sorted(pinned.items()):
        parts = key.split(".")
        if len(parts) != 2:
            skipped.append(f"split part {key} = {slug}")
            continue
        column = parts[1]
        table = next((t for t in sorted(tables) if fold(column) in tables[t]), None)
        if table is None:
            skipped.append(f"no table documents {column} = {slug}")
            continue
        anchors[slug].append(f"{table}: {column}" if table else column)
    lines += [
        f"{quote(slug)} = [{', '.join(quote(a) for a in sorted(set(found)))}]"
        for slug, found in sorted(anchors.items())
    ]
    return lines, skipped


def main() -> int:
    pin_dir, frame_dir, db, curation = map(Path, sys.argv[1:5])
    commit, providers = sys.argv[5], set(sys.argv[6:])
    if not providers:
        sys.exit(
            "name the providers whose ledgers to write; the others are hand-edited"
        )
    slugs = table_slugs(db)
    for (provider, register), tables in sorted(frames(frame_dir).items()):
        if provider not in providers:
            continue
        pinned = pins(pin_dir, provider, register)
        if not pinned:
            print(f"{provider}/{register}: no pins")
            continue
        lines, skipped = ledger(tables, slugs[(provider, register)], pinned)
        for line in skipped:
            print(f"{provider}/{register}: {line}")
        path = curation / "registers" / provider / register / "ids.toml"
        path.parent.mkdir(parents=True, exist_ok=True)
        header = (
            f"# Public ids of {provider}/{register}, from the slug pins at {commit}.\n"
        )
        path.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
