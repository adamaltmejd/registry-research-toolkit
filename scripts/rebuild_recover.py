#!/usr/bin/env python3
"""Recover source-evidenced D conversion drops; deleted at milestone F (REBUILD.md).

Append only outputs absent from the existing statements. The built catalog is
read-only identity evidence; reg-build lock certifies every retained proposal.
Existing hand curation, field rulings and classifications are never replaced.
"""

import argparse
import collections
import json
import re
import tomllib
from pathlib import Path

from rebuild_convert import ROOT, Converter
from rebuild_convert_cases import parse_selector
from rebuild_convert_groups import normalize_sets
from rebuild_convert_selectors import collapse

SOURCE = re.compile(r"reg_meta_build/curation/[^\s;]+\.toml:\d+")


def effect(statement):
    return json.dumps(
        {k: v for k, v in statement.items() if k not in {"evidence", "caveat"}},
        sort_keys=True,
        ensure_ascii=False,
    )


def covered_unit(statement, setters):
    """Retain an explicit unit through its existing covering scalar correction."""
    fact = statement.get("fact", {})
    fields = fact.get("fields", {})
    if fact.get("entity") != "occurrence" or "unit" not in fields:
        return False
    own = parse_selector(statement["subject"])
    if own is None:
        return False
    for setter in setters:
        if setter["kind"] != "set" or setter.get("fields") != {"unit": fields["unit"]}:
            continue
        other = parse_selector(setter["subject"])
        if (
            other is not None
            and own[0] == other[0]
            and own[1] == other[1]
            and not other[3]
            and all(any(a <= lo and hi <= b for a, b in other[2]) for lo, hi in own[2])
        ):
            del fields["unit"]
            statement["evidence"] = statement.get("evidence", "") + (
                "\n\nThe explicit unit is supplied by the existing covering unit setter; "
                "omitted from this constructor to avoid overlapping claims."
            )
            return True
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frames", required=True, type=Path)
    parser.add_argument("--catalog", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out = args.out.resolve()
    authoritative = (ROOT / "curation").resolve()
    if args.out == authoritative or authoritative in args.out.parents:
        parser.error("--out must be a new scratch directory outside curation/")
    args.out.mkdir(parents=True, exist_ok=False)
    existing = set()
    existing_effects = collections.defaultdict(set)
    existing_sets = collections.defaultdict(set)
    existing_statements = collections.defaultdict(list)
    for path in (ROOT / "curation").rglob("curation.toml"):
        for statement in tomllib.loads(path.read_text()).get("statement", []):
            existing.update(SOURCE.findall(statement.get("evidence", "")))
            existing_statements[path.parent.parts[-2:]].append(statement)
            existing_effects[path.parent.parts[-2:]].add(effect(statement))
            if statement["kind"] == "set":
                existing_sets[path.parent.parts[-2:], statement["subject"]].update(
                    statement.get("fields", {}).get("sets", [])
                )
    converter = Converter(args.frames, args.out, args.catalog)
    print("Loaded catalog and frames", flush=True)
    for provider in ("scb", "fk", "sos"):
        for path in sorted(
            (ROOT / "reg_meta_build/curation/registers" / provider).glob("*.toml")
        ):
            if not path.name.endswith(".auto.toml"):
                converter.convert_file(path)
    print("Converted legacy declarations", flush=True)
    converter.normalize_identity()
    print("Reviewed catalog identities", flush=True)
    normalize_sets(converter)
    print("Normalized memberships", flush=True)
    collapse(converter)
    (args.out / "proposals.json").write_text(
        json.dumps(
            [
                {
                    "owner": owner,
                    "statement": record["statement"],
                    "origins": record["origins"],
                }
                for owner, records in converter.statements.items()
                for record in records.values()
            ],
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    )
    carried = collections.Counter()
    for owner, records in converter.statements.items():
        for key, record in list(records.items()):
            statement = record["statement"]
            fields = statement.get("fields", {})
            membership_carried = (
                statement["kind"] == "set"
                and set(fields) == {"sets"}
                and set(fields["sets"]) <= existing_sets[owner, statement["subject"]]
            )
            if effect(statement) in existing_effects[owner] or membership_carried:
                carried.update(set(record["origins"]))
                del records[key]
            else:
                covered_unit(statement, existing_statements[owner])
    retained = collections.Counter(
        origin
        for records in converter.statements.values()
        for record in records.values()
        for origin in set(record["origins"])
    )
    for result in converter.results:
        if carried[result["source"]] and not retained[result["source"]]:
            result["outcome"] = "already carried"
        elif result["source"] in existing:
            result["previously_carried_source"] = True
        elif result["outcome"] == "converted" and not retained[result["source"]]:
            result["outcome"] = "dropped"
            result["reason"] = "no additional statement after catalog identity review"
    converter.write()
    converter.report({"status": "not run", "refusals": []})
    summary = collections.Counter(
        (result["category"], result["outcome"]) for result in converter.results
    )
    print(
        json.dumps(
            {
                f"{kind}: {outcome}": count
                for (kind, outcome), count in sorted(summary.items())
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
