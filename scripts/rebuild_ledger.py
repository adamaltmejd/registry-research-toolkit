#!/usr/bin/env python3
"""rebuild_ledger — today's slug pins into the rebuild's id ledgers.

    uv run --no-project scripts/rebuild_ledger.py <pins dir> <frames dir> <catalog db> \
        <curation dir> <pins commit> <provider>...

Temporary (REBUILD.md): milestone A ran it, B runs it for the SCB ledgers, and it is
deleted when B ends: after that, D's statements replace the slug pins it reads.
Writes `<curation dir>/registers/<provider>/<register>/ids.toml` for
every register of the named providers in the frames `reg-build read` wrote to
`<frames dir>`, from:

- the variable pins in `<pins dir>/<provider>/<register>.auto.toml`, shadowed by the
  hand file `<register>.toml` (`[variable]`, `"<native id>.<column>" = slug`);
- the table slugs of the old catalog `<catalog db>` (`register_variant`), matched to a
  frame table by its display group, slug or name.

Each id anchors on the first table, in table order, documenting its column. Pins that
name a split part (`<native id>.<column>.<part>`) and pins whose column no table
documents are listed on stdout and left out: they have no anchor until milestone D.

SCB pins are keyed `<RegisterId>.<VarId>`, or `<RegisterId>.<VarId>.<column>` where
today's builder split a VarId by spelling or role, and their file is named by today's register
slug. They go to `registers/scb/r<RegisterId>/ids.toml`, anchored on a table
documenting source key `v<VarId>`, preferring a spelling that folds to the slug. A split
pin anchors on the columns its hand file's `[[identity.partition]]` maps to it, else on
the spelling that slugifies to its last part. A pin keyed `<RegisterId>.<column>` anchors
on that column. Where another source key shares the spelling in that table, the anchor
carries the first day the pin's key covers and no rival does (`<table>: <spelling> @
<day>`); a pin no day separates is left out. Registers the export does not hold (the transcriptions) are skipped. A
variable pin equal to one of its register's table slugs is left out: tables and
variables now share the register's namespace, and the table keeps its id.
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


Keyed = dict[tuple[str, str], dict[str, set[tuple[str, str]]]]
# (provider, register) -> (table, folded column) -> source key -> periods
Periods = dict[tuple[str, str], dict[tuple[str, str], dict[str, list[tuple[str, str]]]]]


def frames(
    directory: Path,
) -> tuple[dict[tuple[str, str], dict[str, set[str]]], Keyed, Periods]:
    """(provider, register) -> table handle -> folded columns,
    (provider, register) -> source key -> (table, column) pairs, and each spelling's
    periods by source key (an occurrence's own bounds, else its release's)."""
    out: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(dict)
    keyed: Keyed = defaultdict(lambda: defaultdict(set))
    releases: dict[tuple[str, str, str], tuple[str, str]] = {}
    stated: list[tuple[tuple[str, str], tuple[str, str], str, str | None, tuple]] = []
    for path in sorted(directory.glob("*.jsonl")):
        with path.open(encoding="utf-8") as lines:
            for line in lines:
                rec = json.loads(line)
                key = (rec.get("provider"), rec.get("register"))
                if rec["kind"] == "table":
                    out[key].setdefault(rec["table"], set())
                elif rec["kind"] == "release":
                    releases[(*key, rec["release"])] = (rec["from"], rec["to"])
                elif rec["kind"] == "occurrence":
                    out[key].setdefault(rec["table"], set()).add(fold(rec["column"]))
                    keyed[key][rec["source_key"]].add((rec["table"], rec["column"]))
                    stated.append(
                        (
                            key,
                            (rec["table"], fold(rec["column"])),
                            rec["source_key"],
                            rec.get("release"),
                            (rec.get("from"), rec.get("to")),
                        )
                    )
    periods: Periods = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for key, spelling, source, release, bounds in stated:
        start, end = bounds
        if release and (*key, release) in releases:
            start, end = (
                start or releases[(*key, release)][0],
                end or releases[(*key, release)][1],
            )
        periods[key][spelling][source].append(
            (start or "0001-01-01", end or "9999-12-31")
        )
    return out, keyed, periods


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
        if slug.startswith("_"):
            continue  # today's placeholder for a register's only variant
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


def anchor(
    pairs: list[tuple[str, str]],
    source: str,
    shared: dict[tuple[str, str], dict[str, list[tuple[str, str]]]],
) -> str | None:
    """The first of `pairs` that names `source` alone, dated when its spelling there is
    shared with another source key: on the first day `source` covers and no rival does."""
    dated = []
    for table, column in pairs:
        rivals = {k: v for k, v in shared[(table, fold(column))].items() if k != source}
        if not rivals:
            return f"{table}: {column}"
        taken = [p for spans in rivals.values() for p in spans]
        days = sorted(start for start, _ in shared[(table, fold(column))][source])
        free = next((d for d in days if not any(a <= d <= b for a, b in taken)), None)
        if free:
            dated.append(f"{table}: {column} @ {free}")
    return dated[0] if dated else None


def scb_ledgers(
    pin_dir: Path,
    found: dict[tuple[str, str], dict[str, set[str]]],
    keyed: Keyed,
    periods: Periods,
    slugs: dict[tuple[str, str], dict[str, str]],
) -> dict[str, tuple[list[str], list[str], str]]:
    """`r<RegisterId>` -> (ledger lines, skipped pins, today's register slug)."""
    out = {}
    for path in sorted((pin_dir / "scb").glob("*.auto.toml")):
        old = path.name.removesuffix(".auto.toml")
        pinned = pins(pin_dir, "scb", old)
        numeric = {
            k: v["slug"] if isinstance(v, dict) else v
            for k, v in pinned.items()
            if k.split(".")[0].isdigit()
            and not (isinstance(v, dict) and v.get("deprecated"))
        }
        hand = pin_dir / "scb" / f"{old}.toml"
        partition: dict[str, set[str]] = defaultdict(set)
        if hand.is_file():
            doc = tomllib.loads(hand.read_text(encoding="utf-8"))
            for entry in doc.get("identity", {}).get("partition", []):
                for column, part in entry.get("columns", {}).items():
                    partition[part].add(fold(column))
        if not numeric:
            continue
        (rid,) = {k.split(".")[0] for k in numeric}
        register = f"r{rid}"
        if ("scb", register) not in found:
            continue
        tables = found.get(("scb", register), {})
        by_key = keyed.get(("scb", register), {})
        table_slugs = slugs.get(("scb", old), {})
        lines = ["[tables]"]
        lines += [
            f"{quote(t)} = {quote(table_slugs[fold(t)])}"
            for t in sorted(tables)
            if fold(t) in table_slugs
        ]
        lines += ["", "[variables]"]
        skipped = []
        anchors: dict[str, set[str]] = defaultdict(set)
        taken = {table_slugs[fold(t)] for t in tables if fold(t) in table_slugs}
        for key, slug in sorted(numeric.items()):
            if slug in taken:
                skipped.append(f"{key} = {slug} is a table id")
                continue
            _, var, *spelling = key.split(".")
            if var.isdigit():
                pairs = sorted(p for p in by_key.get(f"v{var}", ()) if p[1].strip())
            else:
                pairs = sorted(
                    (t, c)
                    for pairs_ in by_key.values()
                    for t, c in pairs_
                    if fold(c) == fold(var)
                )
            if spelling:
                columns = partition.get(key) or {spelling[0]}
                pairs = [
                    p
                    for p in pairs
                    if fold(p[1]).replace("_", "-") in columns or fold(p[1]) in columns
                ]
            if not pairs:
                skipped.append(f"no table documents {key} = {slug}")
                continue
            folds_to_slug = [p for p in pairs if fold(p[1]).replace("_", "-") == slug]
            others = [p for p in pairs if p not in folds_to_slug]
            source = f"v{var}" if var.isdigit() else None
            if source is None:
                found_ = f"{pairs[0][0]}: {pairs[0][1]}"
            else:
                found_ = anchor(
                    folds_to_slug + others, source, periods[("scb", register)]
                )
            if found_ is None:
                skipped.append(
                    f"{key} = {slug} shares its spelling on every day it has"
                )
                continue
            anchors[slug].add(found_)
        lines += [
            f"{quote(slug)} = [{', '.join(quote(a) for a in sorted(found_))}]"
            for slug, found_ in sorted(anchors.items())
        ]
        out[register] = (lines, skipped, old)
    return out


def main() -> int:
    if len(sys.argv) < 7:
        sys.exit(
            "usage: rebuild_ledger.py <pins dir> <frames dir> <catalog db> <curation dir>"
            " <pins commit> <provider>...; the other providers' ledgers are hand-edited"
        )
    pin_dir, frame_dir, db, curation = map(Path, sys.argv[1:5])
    commit, providers = sys.argv[5], set(sys.argv[6:])
    slugs = table_slugs(db)
    found, keyed, periods = frames(frame_dir)
    if "scb" in providers:
        providers.discard("scb")
        for register, (lines, skipped, old) in scb_ledgers(
            pin_dir, found, keyed, periods, slugs
        ).items():
            for line in skipped:
                print(f"scb/{register} ({old}): {line}")
            path = curation / "registers" / "scb" / register / "ids.toml"
            path.parent.mkdir(parents=True, exist_ok=True)
            header = (
                f"# Public ids of scb/{register} (today's scb/{old}), from the slug"
                f" pins at {commit}.\n"
            )
            path.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")
    for provider, register in sorted(k for k in found if k[0] in providers):
        tables = found[(provider, register)]
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
