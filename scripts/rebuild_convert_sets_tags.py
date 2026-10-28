#!/usr/bin/env python3
"""Convert legacy set/tag membership only; deleted at milestone F (REBUILD.md).

Uses exact register public ids and variable ids from the current ledgers. Missing
ids are located D-12 refusals, never inferred from labels or delivery hints. This
static conversion does not certify occurrence placement or review lock premises.
Run with --write-curation to apply; otherwise only the source-accounting report is
written. Existing locks are preserved for the later scheduled lock/build review.
"""

import argparse
import collections
import re
import tomllib
from pathlib import Path

from rebuild_convert import ROOT, entries, literal


def source_rows():
    """Yield each membership and each discarded metadata payload at its source."""
    for path in sorted((ROOT / "reg_meta_build/curation/registers").glob("*/*.toml")):
        if path.name.endswith(".auto.toml"):
            continue
        lines = path.read_text().splitlines()
        for kind, line, group in entries(path):
            if kind != "group":
                continue
            source = f"{path.relative_to(ROOT)}:{line}"
            metadata = sorted(group.keys() - {"register", "key", "members"})
            yield source, "set declaration", group["register"], group["key"], metadata
            member_line = (
                next(
                    number
                    for number in range(line, len(lines))
                    if re.match(r"\s*members\s*=", lines[number])
                )
                + 1
            )
            for member in group["members"]:
                yield (
                    f"{path.relative_to(ROOT)}:{member_line}",
                    "sets",
                    f"{group['register']}/{member['variable']}",
                    group["key"],
                    sorted(member.keys() - {"variable"}),
                )
    path = ROOT / "reg_meta_build/curation/tags.toml"
    data = tomllib.loads(path.read_text())
    tag_index = member_index = -1
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if line == "[[tag]]":
            tag_index += 1
            member_index = -1
            tag = data["tag"][tag_index]
            yield (
                f"{path.relative_to(ROOT)}:{number}",
                "tag declaration",
                "",
                tag["slug"],
                sorted(tag.keys() - {"slug", "member"}),
            )
        elif line == "[[tag.member]]":
            member_index += 1
            member = tag["member"][member_index]
            field = "tags" if "variable" in member else "register tags"
            yield (
                f"{path.relative_to(ROOT)}:{number}",
                field,
                member.get("variable", member.get("register")),
                tag["slug"],
                sorted(member.keys() - {"variable", "register"}),
            )


def merge_memberships(path, additions):
    """Replace only affected membership blocks; preserve all other authored text."""
    text = path.read_text() if path.exists() else ""
    starts = [match.start() for match in re.finditer(r"(?m)^\[\[statement\]\]", text)]
    prefix = text[: starts[0]] if starts else text
    blocks = [text[start:end] for start, end in zip(starts, [*starts[1:], len(text)])]
    kept = []
    merged = {}
    for block in blocks:
        statement = tomllib.loads(block)["statement"][0]
        key = statement["subject"], next(iter(statement.get("fields", {})), None)
        if statement["kind"] != "set" or key not in additions:
            kept.append(block)
            continue
        if set(statement["fields"]) != {key[1]}:
            raise ValueError(
                f"{path}: combined membership/scalar setter requires review"
            )
        own = merged.setdefault(key, {"members": set(), "evidence": [], "caveat": []})
        own["members"].update(statement["fields"][key[1]])
        for field in ("evidence", "caveat"):
            if statement.get(field) and statement[field] not in own[field]:
                own[field].append(statement[field])
    if kept and not kept[-1].endswith("\n\n"):
        kept[-1] += "\n"
    for key, addition in sorted(additions.items()):
        own = merged.setdefault(key, {"members": set(), "evidence": [], "caveat": []})
        own["members"].update(addition["members"])
        evidence = "D-9 membership only: " + "; ".join(sorted(addition["sources"]))
        if evidence not in "\n\n".join(own["evidence"]).split("\n\n"):
            own["evidence"].append(evidence)
        statement = {
            "kind": "set",
            "subject": key[0],
            "fields": {key[1]: sorted(own["members"])},
            "evidence": "\n\n".join(own["evidence"]),
        }
        if own["caveat"]:
            statement["caveat"] = "\n\n".join(own["caveat"])
        kept.append(
            "[[statement]]\n"
            + "\n".join(
                f"{field} = {literal(value)}" for field, value in statement.items()
            )
            + "\n\n"
        )
    return (prefix + "".join(kept)).rstrip() + "\n"


def convert(*, write_curation=False):
    ledgers = {}
    for path in sorted((ROOT / "curation/registers").glob("*/*/ids.toml")):
        data = tomllib.loads(path.read_text())
        provider, register = path.parts[-3:-1]
        public = f"{provider}/{data.get('public', register)}"
        if public in ledgers:
            raise ValueError(f"duplicate register public id: {public}")
        ledgers[public] = path.parent / "curation.toml", data.get("variables", {})
    additions = collections.defaultdict(dict)
    outcomes = []
    counts = collections.Counter()
    for source, field, target, member, metadata in source_rows():
        counts[field] += 1
        if metadata:
            outcomes.append(
                (
                    "dropped",
                    source,
                    field,
                    target,
                    member,
                    "presentation metadata: "
                    + ", ".join(metadata)
                    + "; membership-only ruling, deferred webapp presentation",
                )
            )
        if field.endswith("declaration"):
            continue
        if field == "register tags":
            outcomes.append(
                (
                    "dropped",
                    source,
                    field,
                    target,
                    member,
                    "deferred: no existing register tag storage; never propagated to variables",
                )
            )
            continue
        public, _, variable = target.rpartition("/")
        if public not in ledgers or variable not in ledgers[public][1]:
            outcomes.append(
                (
                    "refused",
                    source,
                    field,
                    target,
                    member,
                    "D-12: no exact local variable ledger id; no id or delivery-column mapping inferred",
                )
            )
            continue
        path, _ = ledgers[public]
        key = f"var:{variable}", field
        own = additions[path].setdefault(key, {"members": set(), "sources": set()})
        own["members"].add(member)
        own["sources"].add(source)
        outcomes.append(
            (
                "converted",
                source,
                field,
                target,
                member,
                f"{path.relative_to(ROOT)}: {key[0]}",
            )
        )
    # Compute every edit before writing, so an unsupported existing setter fails closed.
    edits = {path: merge_memberships(path, items) for path, items in additions.items()}
    if write_curation:
        for path, text in edits.items():
            path.write_text(text)
    lines = [
        "# D-9 set/tag membership conversion",
        "",
        "Generated by `uv run --no-project scripts/rebuild_convert_sets_tags.py --write-curation`.",
        "",
        f"Source inventory: {counts['set declaration']} explicit sets, {counts['sets']} set memberships; {counts['tag declaration']} tags, {counts['tags']} variable memberships and {counts['register tags']} register memberships.",
        "Exact current local ledger ids are the conversion authority. Missing ids are refused for D-12; delivery-column hints never supply identity. Existing membership setters are consolidated per variable and field. Unrelated authored statements and all locks are preserved.",
        "",
        "This is static source accounting. Lock, curated build and verify were not run; placement and final resolver selection remain unverified. Tag search is not implemented. Presentation fields are dropped with reasons below; register memberships remain deferred.",
        "",
        "| Field | Converted memberships | Refused memberships | Deferred register memberships |",
        "| --- | ---: | ---: | ---: |",
    ]
    for field in ("sets", "tags", "register tags"):
        summary = collections.Counter(
            row[0]
            for row in outcomes
            if row[2] == field and not row[5].startswith("presentation metadata:")
        )
        lines.append(
            f"| {field} | {summary['converted']} | {summary['refused']} | {summary['dropped']} |"
        )
    lines += [
        "",
        "## Source-located outcomes",
        "",
        "Each membership has one converted/refused/deferred row; presentation payloads have separate dropped rows.",
        "",
        "| Outcome | Source | Field | Target | Membership | Result / reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in outcomes:
        lines.append(
            "| " + " | ".join(value.replace("|", "\\|") for value in row) + " |"
        )
    return "\n".join(lines) + "\n", edits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-curation", action="store_true")
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "scripts/rebuild_convert_sets_tags_report.md",
    )
    args = parser.parse_args()
    report, edits = convert(write_curation=args.write_curation)
    args.report.write_text(report)
    print(
        f"Accounted for set/tag sources; {len(edits)} register files {'written' if args.write_curation else 'proposed'}. Report: {args.report}"
    )


if __name__ == "__main__":
    main()
