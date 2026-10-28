#!/usr/bin/env python3
"""One-shot numeric type curation; deleted at milestone F (REBUILD.md).

Build a dev catalog first, then pass --catalog FILE --assets DIR --report FILE.
The catalog supplies resolved, post-identity-curation occurrence membership and
frame value sets, never holdings values. Exact source locators supply raw types.
Default: report only. --write appends variable-wide set statements; run reg-build
lock afterwards. Existing locks/statements are never replaced by this script.
Unpinned variables remain report-only until the identity work supplies ledger ids.
"""

import argparse
import collections
import csv
import hashlib
import io
import json
import re
import sqlite3
import tomllib
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NUMERIC = {"int", "integer", "decimal"}
SCALE = re.compile(r"(?:decimal|numeric)\s*\(\s*\d+\s*,\s*(\d+)\s*\)", re.IGNORECASE)


def digest_tree(root):
    """Same path/digest inventory as reg-build's curation_digest."""
    return hashlib.sha256(
        "".join(
            f"{p.relative_to(root).as_posix()}\t{hashlib.sha256(p.read_bytes()).hexdigest()}\n"
            for p in sorted(root.rglob("*"))
            if p.is_file()
            and not any(s.startswith(".") for s in p.relative_to(root).parts)
        ).encode()
    ).hexdigest()


def source_types(archive, locators):
    """Read only Datatyp at the SCB metadata rows the catalog cites."""
    wanted = set(locators)
    found = {}
    with (
        zipfile.ZipFile(archive) as bundle,
        bundle.open("Registerinformation.csv") as stream,
    ):
        rows = csv.DictReader(
            io.TextIOWrapper(stream, encoding="cp1252"), delimiter="|"
        )
        for line, row in enumerate(rows, 2):
            if line in wanted:
                found[line] = row["Datatyp"].strip()
    if found.keys() != wanted:
        raise ValueError("source locator rows missing from pinned SCB export")
    return found


def decide(codes, decimal_types, types_complete):
    if codes:
        try:
            numbers = [Decimal(c) for c in codes]
        except InvalidOperation:
            return None, "value set includes nonnumeric codes; precision unresolved"
        if not all(n.is_finite() for n in numbers):
            return None, "value set includes nonfinite codes; precision unresolved"
        if all(n == n.to_integral_value() for n in numbers):
            return "integer", f"value set: {len(codes)} integer codes"
        return "decimal", f"value set: {len(codes)} codes, including non-integer codes"
    scales = [SCALE.fullmatch(t) for t in decimal_types]
    if any(m and int(m[1]) > 0 for m in scales):
        return "decimal", "source declares scale > 0 on a decimal delivery"
    if types_complete and scales and all(m and int(m[1]) == 0 for m in scales):
        return "integer", "scale 0 on all decimal deliveries"
    return None, "no value set; decimal source declarations lack explicit scale"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, type=Path)
    parser.add_argument("--curation", type=Path, default=ROOT / "curation")
    parser.add_argument("--assets", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    manifest = tomllib.loads((args.curation / "inputs.toml").read_text())
    scb = next(i for i in manifest["input"] if i["name"] == "scb-export")
    archive = args.assets / scb["release"] / scb["path"]
    with archive.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != scb["sha256"]:
            raise ValueError("SCB export digest differs from curation input pin")
    db = sqlite3.connect(args.catalog.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    meta = dict(db.execute("select key, value from meta"))
    if meta["curation_digest"] != digest_tree(args.curation):
        raise ValueError(
            "catalog curation is stale; rebuild before generating statements"
        )
    expected_pins = {
        i["name"]: i.get("release", manifest["commit"]) for i in manifest["input"]
    }
    if dict(db.execute("select name, commit_sha from input")) != expected_pins:
        raise ValueError("catalog input commits differ from curation input pins")
    # Use the compiler's membership, including curated assignments, rather than
    # attempting to duplicate identity resolution from ledger anchor strings.
    variables = collections.defaultdict(list)
    for row in db.execute("""
        select o.*, v.pid from occurrence o join variable v on v.id=o.variable_id
        order by v.pid, o.id
    """):
        variables[row["pid"]].append(row)
    locators = collections.defaultdict(list)
    for row in db.execute("""
        select l.occurrence_id, i.name, l.locator
        from occurrence_locator l join input i on i.id=l.input_id
    """):
        if row[1] == "scb-export":
            match = re.fullmatch(
                re.escape(scb["path"]) + r"/Registerinformation.csv:row=(\d+)", row[2]
            )
            if not match:
                raise ValueError(f"unsupported SCB occurrence locator: {row[2]}")
            locators[row[0]].append(int(match[1]))
    wanted = {
        at for rows in variables.values() for r in rows for at in locators[r["id"]]
    }
    raw = source_types(archive, wanted)
    type_map = tomllib.loads((args.curation / "readers/scb.toml").read_text())["types"]
    # Merged population rows can hide a rival dtype behind the first row. Include
    # every source locator, not just the provisional normalized occurrence dtype.
    candidates = {}
    for pid, rows in variables.items():
        types = {r["dtype"] for r in rows}
        types.update(type_map.get(raw[at]) for r in rows for at in locators[r["id"]])
        if "decimal" in types and types & {"int", "integer"}:
            candidates[pid] = rows
    population_sets = collections.defaultdict(set)
    for oid, vs in db.execute(
        "select occurrence_id, valueset_id from occurrence_population_valueset"
    ):
        population_sets[oid].add(vs)
    sets = collections.defaultdict(set)
    for vs, code in db.execute(
        "select v.valueset_id, t.body from vs_item v join text t on t.id=v.code_text"
    ):
        sets[vs].add(code)
    paths = {}
    pinned = set()
    for path in sorted((args.curation / "registers").glob("*/*/ids.toml")):
        ledger = tomllib.loads(path.read_text())
        provider, handle = path.parts[-3:-1]
        register = f"{provider}/{ledger.get('public', handle)}"
        pinned.update(f"{register}/{slug}" for slug in ledger.get("variables", {}))
        paths[register] = path.with_name("curation.toml")
    report = []
    writes = collections.defaultdict(str)
    for pid, rows in sorted(candidates.items()):
        source = []
        complete = True
        for row in rows:
            source.extend(raw[at] for at in locators[row["id"]])
            if row["dtype"] == "decimal" and not locators[row["id"]]:
                complete = False
        decimal_types = sorted(
            {t for t in source if type_map.get(t) == "decimal" or SCALE.fullmatch(t)}
        )
        vs_ids = {r["valueset_id"] for r in rows if r["valueset_id"] is not None}
        for row in rows:
            vs_ids.update(population_sets[row["id"]])
        codes = set().union(*(sets[v] for v in vs_ids))
        dtype, evidence = decide(codes, decimal_types, complete)
        types = {r["dtype"] for r in rows} | {type_map.get(t) for t in source}
        if types - NUMERIC - {None, "unspecified"}:
            dtype, evidence = None, "also has a nonnumeric type family; requires review"
        if not decimal_types and not codes:
            dtype, evidence = None, "no value set; decimal source precision unavailable"
        if dtype and pid not in pinned:
            evidence += (
                f"; proposed {dtype}, no pinned ledger id for variable-wide selector"
            )
            dtype = None
        report.append(
            {
                "variable": pid,
                "type": dtype,
                "evidence": evidence,
                "source_types": sorted(set(source)),
                "value_sets": len(vs_ids),
            }
        )
        if dtype:
            register, slug = pid.rsplit("/", 1)
            if not re.fullmatch(r"[a-z0-9_-]+", slug):
                raise ValueError(f"unsupported variable slug: {pid}")
            writes[paths[register]] += (
                '\n[[statement]]\nkind = "set"\n'
                f'subject = "var:{slug}"\nfields = {{ type = "{dtype}" }}\n'
                f"evidence = {json.dumps(evidence, ensure_ascii=False)}\n"
            )
    counts = collections.Counter(r["type"] or "unresolved" for r in report)
    args.report.write_text(
        json.dumps(
            {"total": len(report), "counts": counts, "variables": report},
            indent=2,
            ensure_ascii=False,
        )
        + "\n"
    )
    if args.write:
        for path, statements in sorted(writes.items()):
            with path.open("a") as stream:
                stream.write(statements)
    print(
        json.dumps(
            {
                "total": len(report),
                **counts,
                "registers_written": len(writes) if args.write else 0,
            }
        )
    )


if __name__ == "__main__":
    main()
