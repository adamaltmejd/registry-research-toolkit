"""Independent raw-input accounting at the compiled-artifact boundary.

Private input identifiers are compared in memory. Return values and errors contain
only counts, digests and contract labels. No compiler/accounting helper is called.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import subprocess
import tomllib
from collections import Counter, defaultdict
from pathlib import Path

from reg_meta.db import get_manifest, open_db

DISPOSITIONS = ("dated", "year_independent", "retained_unknown", "excluded", "lookup")
POLICY_NAMES = ("source_policy", "inventory_overlay", "holdings_policy")


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def admit_holdings_input(path: Path, artifact_dir: Path) -> Path:
    """Fail closed on missing, dirty, mismatched or incomplete accepted inputs."""
    try:
        root = path.expanduser().resolve(strict=True)
        with open_db(artifact_dir / "reg_meta.db") as conn:
            manifest = get_manifest(conn)
        if manifest["catalog_artifact_kind"] != "steward":
            raise ValueError("Holdings input requires a steward artifact")
        candidate = root / "manifest.json"
        if file_digest(candidate) != manifest["holdings_manifest_sha256"]:
            raise ValueError("Holdings input manifest pin does not match artifact")
        commit = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if commit != manifest["holdings_input_commit"]:
            raise ValueError("Holdings input commit pin does not match artifact")
        status = subprocess.check_output(
            ["git", "-C", str(root), "status", "--porcelain"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        if status.strip():
            raise ValueError("Holdings input Git tree is not clean")
        members = json.loads(candidate.read_text())["files"]
        names = set()
        for member in members:
            relative = Path(member["path"])
            selected = (root / relative).resolve(strict=True)
            if relative.is_absolute() or not selected.is_relative_to(root):
                raise ValueError("Holdings input member escapes candidate root")
            if relative.as_posix() in names:
                raise ValueError("Holdings input manifest contains duplicate members")
            names.add(relative.as_posix())
            if (
                selected.stat().st_size != member["size"]
                or file_digest(selected) != member["sha256"]
            ):
                raise ValueError("Holdings input member does not match its manifest")
        required = {f"policy/{name}.toml" for name in (*POLICY_NAMES, "inventory")}
        sources = list((root / "swecov").glob("SWECOV_variables_full_*.csv"))
        if len(sources) != 1:
            raise ValueError("Holdings input requires one complete census")
        required.add(sources[0].relative_to(root).as_posix())
        if not required.issubset(names):
            raise ValueError("Holdings input manifest omits consumed accounting inputs")
        return root
    except KeyError as exc:
        raise ValueError(
            f"Holdings input manifest is missing required key: {exc.args[0]}"
        ) from None
    except (
        OSError,
        TypeError,
        IndexError,
        json.JSONDecodeError,
        subprocess.SubprocessError,
    ):
        raise ValueError(
            "Holdings input path or accepted manifest is invalid"
        ) from None


def period_bounds(token: str | int) -> tuple[str, str]:
    """Expand documented finite period tokens independently of product helpers."""
    token = str(token)
    # The period grammar synthesizes February29 for month bounds regardless of leap year;
    # request clipping later snaps synthetic month ends to real dates.
    ends = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    if match := re.fullmatch(r"(LA|HT|VT)([0-9]{4})", token):
        kind, year = match.groups()
        if kind == "LA":
            return f"{year}-07-01", f"{int(year) + 1:04d}-06-30"
        if kind == "HT":
            return f"{year}-07-01", f"{year}-12-31"
        return f"{year}-01-01", f"{year}-06-30"
    if match := re.fullmatch(r"([0-9]{4})-([QH])([1-4])", token):
        year, kind, index = match.groups()
        width = 3 if kind == "Q" else 6
        lower = (int(index) - 1) * width + 1
        upper = lower + width - 1
        return f"{year}-{lower:02d}-01", f"{year}-{upper:02d}-{ends[upper - 1]}"
    parts = token.split("-")
    if len(parts) == 1:
        return f"{token}-01-01", f"{token}-12-31"
    if len(parts) == 2:
        return f"{token}-01", f"{token}-{ends[int(parts[1]) - 1]}"
    return token, token


def edition_periods(edition: object) -> list[tuple[str, str]]:
    result = []
    for part in edition if isinstance(edition, list) else [edition]:
        if isinstance(part, dict):
            result.append(
                (period_bounds(part["from"])[0], period_bounds(part["to"])[1])
            )
        else:
            result.append(period_bounds(part))
    return result


def compare_holdings_input(path: Path, artifact_dir: Path) -> dict:
    """Compare table/cell census and all four relations, keeping identifiers private."""
    db = artifact_dir / "reg_meta.db"
    before = file_digest(db)
    policies = {
        name: tomllib.loads((path / f"policy/{name}.toml").read_text())
        for name in (*POLICY_NAMES, "inventory")
    }
    sources = list((path / "swecov").glob("SWECOV_variables_full_*.csv"))
    if len(sources) != 1:
        raise ValueError("Holdings comparison requires one complete census")
    source = sources[0]
    source_name = source.relative_to(path).as_posix()
    raw, subjects, guards = defaultdict(set), defaultdict(set), defaultdict(list)
    with source.open(newline="", encoding="utf-8") as stream:
        for line, row in enumerate(csv.reader(stream), 1):
            if len(row) >= 3:
                guards[row[2].strip()].append({"line": line, "cells": row})
            if line == 1:
                if row[:3] != ["Category", "Detail", "Table"]:
                    raise ValueError("Holdings census header differs from contract")
                continue
            if len(row) < 3 or not row[0].strip():
                continue
            category, detail, table = (cell.strip() for cell in row[:3])
            raw[table].update(cell.strip() for cell in row[3:] if cell.strip())
            subjects[table].add((category, detail))
    routes = {
        (row["category"], row["detail"]): index
        for index, row in enumerate(policies["source_policy"]["route"])
        if row["status"] == "lookup"
    }
    lookups = {
        table: {
            f"policy/source_policy.toml:route[{routes[s]}]"
            for s in scopes
            if s in routes
        }
        for table, scopes in subjects.items()
    }
    claims = {}

    def claim(table: str, disposition: str, locators: set[str]) -> None:
        if table not in raw or table in claims:
            raise ValueError(
                "Holdings census has missing or duplicate table disposition"
            )
        claims[table] = disposition, locators | {f"{source_name}:table[{table!r}]"}

    for table, refs in lookups.items():
        if refs:
            claim(table, "lookup", refs)
    for name in ("inventory_overlay", "holdings_policy"):
        for index, row in enumerate(policies[name].get("exclude", [])):
            if lookups.get(row["table"]):
                continue
            claim(row["table"], "excluded", {f"policy/{name}.toml:exclude[{index}]"})
    retention = policies["holdings_policy"]
    if file_digest(source) != retention["source_sha256"]:
        raise ValueError("Holdings retention census digest differs from policy")
    unknown = retention.get("retain_unknown", [])
    for index, row in enumerate(unknown):
        if digest(guards[row["table"]]) != row["rows_sha256"]:
            raise ValueError("Holdings retained-unknown row digest differs from census")
        claim(
            row["table"],
            "retained_unknown",
            {f"policy/holdings_policy.toml:retain_unknown[{index}]"},
        )
    inventory = policies["inventory"]["table"]
    for index, row in enumerate(inventory):
        disposition = (
            "year_independent"
            if row.get("period_scope") == "year_independent"
            else "dated"
        )
        claim(row["id"], disposition, {f"policy/inventory.toml:table[{index}]"})
        columns = [column["name"] for column in row["column"]]
        if set(columns) != raw[row["id"]] or len(columns) != len(set(columns)):
            raise ValueError("Holdings inventory cell census differs from raw input")
    if set(raw) != set(claims):
        raise ValueError("Holdings raw census is not a disjoint complete partition")
    counts = {name: {"tables": 0, "columns": 0} for name in ("raw", *DISPOSITIONS)}
    projection = []
    for table in sorted(raw):
        disposition, refs = claims[table]
        for key in ("raw", disposition):
            counts[key]["tables"] += 1
            counts[key]["columns"] += len(raw[table])
        projection.extend(
            {
                "table": table,
                "column": column,
                "disposition": disposition,
                "evidence": sorted(refs),
            }
            for column in (None, *sorted(raw[table]))
        )
    expected_tables, expected_columns, expected_periods, expected_mappings = (
        [],
        [],
        [],
        [],
    )
    unmapped_reasons = Counter()
    with open_db(db) as conn:
        manifest = get_manifest(conn)
        variables = {
            f"{p}/{r}/{v}": i
            for p, r, v, i in conn.execute(
                "SELECT p.slug,r.slug,v.slug,v.variable_id FROM variable v JOIN register r USING(register_id) JOIN provider p USING(provider_id)"
            )
        }
        variants = {
            f"{p}/{r}/{v}": i
            for p, r, v, i in conn.execute(
                "SELECT p.slug,r.slug,rv.slug,rv.register_variant_id FROM register_variant rv JOIN register r USING(register_id) JOIN provider p USING(provider_id)"
            )
        }
        for index, table in enumerate(inventory):
            physical = table["id"]
            scope = table.get("period_scope", "intervals")
            expected_tables.append(
                (
                    physical,
                    scope,
                    canonical(table["edition"]),
                    table.get("partition"),
                    None,
                    f"policy/inventory.toml:table[{index}]",
                )
            )
            if scope == "intervals":
                expected_periods.extend(
                    (physical, lo, hi) for lo, hi in edition_periods(table["edition"])
                )
            for column in table["column"]:
                expected_columns.append(
                    (physical, column["name"], column.get("unmapped_reason"))
                )
                if not column.get("mapping"):
                    unmapped_reasons[column.get("unmapped_reason")] += 1
                for mapping in column.get("mapping", []):
                    if (
                        mapping["register_variant"] not in variants
                        or mapping["variable"] not in variables
                    ):
                        raise ValueError(
                            "Holdings mapping coordinate is missing from artifact"
                        )
                    expected_mappings.append(
                        (
                            physical,
                            column["name"],
                            variants[mapping["register_variant"]],
                            variables[mapping["variable"]],
                            mapping["representation"],
                        )
                    )
        for index, table in enumerate(unknown):
            expected_tables.append(
                (
                    table["table"],
                    "unknown",
                    canonical(table["edition"])
                    if table.get("edition") is not None
                    else None,
                    table.get("partition"),
                    table["reason"],
                    f"policy/holdings_policy.toml:retain_unknown[{index}]",
                )
            )
            expected_columns.extend(
                (table["table"], name, None) for name in sorted(raw[table["table"]])
            )
        queries = {
            "holding_table": "SELECT physical_id,scope,edition_json,partition,retain_unknown_reason,source_ref FROM holding_table",
            "holding_column": "SELECT ht.physical_id,hc.name,hc.unmapped_reason FROM holding_column hc JOIN holding_table ht USING(table_id)",
            "holding_period": "SELECT ht.physical_id,hp.lo,hp.hi FROM holding_period hp JOIN holding_table ht USING(table_id)",
            "holding_mapping": "SELECT ht.physical_id,hc.name,hm.variant_id,hm.variable_id,hm.representation_literal FROM holding_mapping hm JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id)",
        }
        relations = {}
        for (name, sql), expected in zip(
            queries.items(),
            (expected_tables, expected_columns, expected_periods, expected_mappings),
            strict=True,
        ):
            actual = [tuple(row) for row in conn.execute(sql)]
            relations[name] = {
                "equal": Counter(actual) == Counter(expected),
                "expected_rows": len(expected),
                "actual_rows": len(actual),
                "expected_sha256": digest(sorted(expected, key=canonical)),
                "actual_sha256": digest(sorted(actual, key=canonical)),
            }
        folded_equal = all(
            literal.lower() == representative.lower()
            for literal, representative in conn.execute(
                "SELECT representation_literal,representation_canonical FROM holding_mapping"
            )
        )
    policy_sha = digest(
        {
            f"policy/{name}.toml": file_digest(path / f"policy/{name}.toml")
            for name in POLICY_NAMES
        }
    )
    exclusion_reasons = Counter(
        row["reason"]
        for name in ("inventory_overlay", "holdings_policy")
        for row in policies[name].get("exclude", [])
        if not lookups.get(row["table"])
    )
    return {
        "counts": counts,
        "projection_records": len(projection),
        "accounting_sha256": digest(projection),
        "policy_sha256": policy_sha,
        "counts_equal": counts == json.loads(manifest["holdings_accounting_counts"]),
        "accounting_digest_equal": digest(projection)
        == manifest["holdings_accounting_sha256"],
        "policy_digest_equal": policy_sha == manifest["holdings_policy_sha256"],
        "relations": relations,
        "partitioned_tables": sum(bool(table.get("partition")) for table in inventory),
        "range_list_tables": sum(
            isinstance(table["edition"], (list, dict)) for table in inventory
        ),
        "exclusion_reason_classes": [
            {"reason_sha256": digest(reason), "tables": count}
            for reason, count in sorted(exclusion_reasons.items())
        ],
        "unmapped_reason_classes": [
            {
                "reason_sha256": digest(reason),
                "columns": count,
                "authored": bool(reason),
            }
            for reason, count in sorted(
                unmapped_reasons.items(), key=lambda item: item[0] or ""
            )
        ],
        "canonical_fold_equal": folded_equal,
        "artifact_unchanged": before == file_digest(db),
    }
