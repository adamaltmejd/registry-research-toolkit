#!/usr/bin/env python3
"""One-shot numeric type curation; deleted at milestone F (REBUILD.md).

Run with uv run python. Build a dev catalog first, then pass --catalog FILE
--inputs DIR --assets DIR --report FILE. The JSON report contains private MONA
schema targets: keep it local; only publish catalog ids, evidence and counts.
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
from dataclasses import asdict
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from rebuild_compare import implicit_table_alias
from reg_meta_build._curation import fold_column
from reg_meta_build.extend_db import read_private_holdings_input
from reg_meta_build.inventory import edition_bounds, load_inventory
from reg_meta_build.steward_storage import read_storage_columns

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


def code_type(codes):
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
    return None, "no value set"


def source_type(decimal_types, types_complete):
    scales = [SCALE.fullmatch(t) for t in decimal_types]
    if any(m and int(m[1]) > 0 for m in scales):
        return "decimal", "source declares scale > 0 on a decimal delivery"
    if types_complete and scales and all(m and int(m[1]) == 0 for m in scales):
        return "integer", "scale 0 on all decimal deliveries"
    return None, "no value set; decimal source declarations lack explicit scale"


def held_columns(db, inputs, manifest, candidates):
    """Consume the accepted inventory mapping; adapt its exact coordinates to v5.

    No grounding, table-name parsing, prefix stripping or new ownership rule.
    A stale coordinate is reported, never silently used as type evidence.
    """
    root = inputs / "swecov"
    manifest_sha = hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
    accepted = read_private_holdings_input(
        root, input_commit=manifest["commit"], input_manifest_sha256=manifest_sha
    )
    inventory = load_inventory(root / accepted["roles"]["inventory"])
    storage = read_storage_columns(root / accepted["roles"]["storage_export"])
    coordinates = collections.defaultdict(list)
    candidate_coordinates = collections.defaultdict(set)
    catalog_variables = {row[0] for row in db.execute("select pid from variable")}
    new_tables = collections.defaultdict(set)
    for register, table in db.execute(
        "select r.pid, t.pid from tbl t join register r on r.id=t.register_id"
    ):
        new_tables[register].add(table)
    old_tables = collections.defaultdict(set)
    for table in inventory.tables:
        for column in table.columns:
            for mapping in column.mappings:
                register, variant = mapping.register_variant.rsplit("/", 1)
                old_tables[register].add("" if variant == "_default" else variant)
    for pid, table, spelling, lo, hi in db.execute("""
        select v.pid, t.pid, s.exact, o.start, o.end
        from occurrence o join variable v on v.id=o.variable_id
        join tbl t on t.id=o.table_id join spelling s on s.id=o.spelling_id
    """):
        coordinates[pid, table, fold_column(spelling)].append((lo, hi))
        if pid in candidates:
            candidate_coordinates[table, fold_column(spelling)].add(pid)
    held = collections.defaultdict(list)
    epoch = date(1970, 1, 1)
    for table in inventory.tables:
        edition = (
            edition_bounds(table.edition) if table.period_scope == "intervals" else ()
        )
        for column in table.columns:
            for mapping in column.mappings:
                catalog_table = mapping.register_variant.removesuffix("/_default")
                if mapping.register_variant.endswith("/_default"):
                    catalog_table = (
                        implicit_table_alias(
                            old_tables[catalog_table], new_tables[catalog_table]
                        )
                        or catalog_table
                    )
                served = mapping.served(edition)
                periods = [
                    (
                        (date.fromisoformat(lo) - epoch).days,
                        (date.fromisoformat(hi) - epoch).days + 1,
                    )
                    for lo, hi in served
                ]
                representation = fold_column(mapping.representation)
                targets = (
                    {mapping.variable} if mapping.variable in candidates else set()
                )
                # A missing legacy id can cite a current occurrence's exact
                # coordinate. Expose an identity refusal, never transfer ownership
                # or use that coordinate to decide a type.
                if mapping.variable not in catalog_variables:
                    targets.update(candidate_coordinates[catalog_table, representation])
                schema = storage.get((table.id, fold_column(column.name)))
                for pid in sorted(targets):
                    matches = coordinates[pid, catalog_table, representation]
                    overlaps = any(
                        not periods
                        or any(lo < end and start < hi for start, end in periods)
                        for lo, hi in matches
                        if lo is not None
                    )
                    if pid != mapping.variable and not overlaps:
                        continue
                    held[pid].append(
                        {
                            "table_name": table.id,
                            "column_name": column.name,
                            "inventory_variable": mapping.variable,
                            "ownership_verified": pid == mapping.variable,
                            "catalog_table": catalog_table,
                            "representation": mapping.representation,
                            "served": served,
                            "catalog_match": overlaps and pid == mapping.variable,
                            "storage": asdict(schema) if schema else None,
                        }
                    )
    return held, {
        "manifest_sha256": manifest_sha,
        "storage_export": accepted["roles"]["storage_export"],
        "inventory": accepted["roles"]["inventory"],
    }


def storage_type(columns):
    if not columns:
        return None, "not held in the accepted SWECOV inventory"
    if any(not c["catalog_match"] or c["storage"] is None for c in columns):
        return None, "held; inventory/catalog or storage match unresolved"
    schemas = [c["storage"] for c in columns]
    if all(
        s["data_type"].lower() in {"int", "bigint", "smallint", "tinyint"}
        or (
            s["data_type"].lower() in {"decimal", "numeric"} and s["numeric_scale"] == 0
        )
        for s in schemas
    ):
        return (
            "integer",
            f"MONA storage: integer/scale 0 on all {len(columns)} held deliveries",
        )
    if any(
        s["data_type"].lower()
        in {"decimal", "numeric", "float", "real", "money", "smallmoney"}
        for s in schemas
    ):
        return None, "decimal storage, values unverified"
    return None, "held storage does not establish an integer type"


def write_reports(rows, public_report, mona_checks):
    """Keep physical schema targets out of the public curation report."""
    unresolved = [row for row in rows if row["type"] is None]
    if public_report:
        lines = [
            "# Unresolved integer/decimal switchers",
            "",
            "One-shot evidence report; deleted at milestone F (REBUILD.md).",
            "",
            "Generated by `rebuild_curate_numeric.py --public-report`. Inputs:",
            "`9186fcbceeb2283a50c7f684764869e4dda5765e`. Metadata only; no MONA probe.",
            "",
            "Physical table/column targets stay in the local `--mona-checks` CSV and",
            "private JSON report. No storage with fractional capacity decides decimal.",
            "Stale inventory identities or unmatched catalog coordinates remain",
            "refusals to type curation. Their target metadata requires review.",
            "",
        ]
        for held in (True, False):
            subset = [row for row in unresolved if row["held"] == held]
            title = (
                "Held targets (including identity refusals)"
                if held
                else "Not held in accepted inventory"
            )
            lines += [f"## {title}: {len(subset)} variables", ""]
            if not held:
                lines += [
                    "No MONA target is established by the accepted inventory mapping.",
                    "These variables remain uncurated and retain any existing F1 notice.",
                    "",
                ]
            groups = collections.defaultdict(list)
            for row in subset:
                register, slug = row["variable"].rsplit("/", 1)
                groups[
                    register,
                    row["evidence"],
                    row["storage_evidence"],
                    tuple(row["source_types"]),
                ].append(slug)
            previous = None
            for (register, evidence, storage, types), slugs in sorted(groups.items()):
                if register != previous:
                    lines += [f"### {register}", ""]
                    previous = register
                lines += [
                    f"**{evidence}.** {storage}. Source types: "
                    + ", ".join(f"`{t or '(blank)'}`" for t in types)
                    + ".",
                    "",
                    ", ".join(f"`{slug}`" for slug in slugs) + ".",
                    "",
                ]
        public_report.write_text("\n".join(lines))
    if mona_checks:
        fields = [
            "variable",
            "inventory_variable",
            "table_schema",
            "table_name",
            "column_name",
            "data_type",
            "numeric_precision",
            "numeric_scale",
            "ownership_verified",
            "catalog_match",
            "served",
        ]
        with mona_checks.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for row in unresolved:
                for target in row["mona_targets"]:
                    physical = target["storage"] or {
                        "table_name": target["table_name"],
                        "column_name": target["column_name"],
                    }
                    writer.writerow(
                        {
                            **physical,
                            "variable": row["variable"],
                            "inventory_variable": target["inventory_variable"],
                            "ownership_verified": target["ownership_verified"],
                            "catalog_match": target["catalog_match"],
                            "served": json.dumps(target["served"]),
                        }
                    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, type=Path)
    parser.add_argument("--inputs", required=True, type=Path)
    parser.add_argument("--curation", type=Path, default=ROOT / "curation")
    parser.add_argument("--assets", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--public-report", type=Path)
    parser.add_argument("--mona-checks", type=Path)
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
    held, holdings_pin = held_columns(db, args.inputs, manifest, candidates)
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
    existing = {}
    for path in sorted((args.curation / "registers").glob("*/*/ids.toml")):
        ledger = tomllib.loads(path.read_text())
        provider, handle = path.parts[-3:-1]
        register = f"{provider}/{ledger.get('public', handle)}"
        pinned.update(f"{register}/{slug}" for slug in ledger.get("variables", {}))
        paths[register] = path.with_name("curation.toml")
        if paths[register].exists():
            for statement in tomllib.loads(paths[register].read_text()).get(
                "statement", []
            ):
                if (
                    statement.get("kind") == "set"
                    and statement["subject"].startswith("var:")
                    and (dtype := statement.get("fields", {}).get("type"))
                ):
                    existing[f"{register}/{statement['subject'][4:]}"] = dtype
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
        dtype, evidence = code_type(codes)
        level = "value_set" if dtype else None
        columns = held[pid]
        storage_dtype, storage_evidence = storage_type(columns)
        if dtype is None and storage_dtype:
            dtype, evidence, level = storage_dtype, storage_evidence, "storage"
            evidence += f"; SWECOV manifest {holdings_pin['manifest_sha256']}"
        if dtype is None:
            source_dtype, source_evidence = source_type(decimal_types, complete)
            if source_dtype:
                dtype, evidence, level = source_dtype, source_evidence, "source_scale"
            elif not codes:
                evidence = source_evidence
        types = {r["dtype"] for r in rows} | {type_map.get(t) for t in source}
        if types - NUMERIC - {None, "unspecified"}:
            dtype, evidence = None, "also has a nonnumeric type family; requires review"
        if dtype is None and not decimal_types and not codes:
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
                "evidence_level": level if dtype else None,
                "held": bool(columns),
                "storage_evidence": storage_evidence,
                "mona_targets": columns,
            }
        )
        if dtype:
            if pid in existing:
                if existing[pid] != dtype:
                    raise ValueError(f"existing type statement disagrees for {pid}")
                continue
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
            {
                "total": len(report),
                "counts": counts,
                "holdings_pin": holdings_pin,
                "variables": report,
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n"
    )
    write_reports(report, args.public_report, args.mona_checks)
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
