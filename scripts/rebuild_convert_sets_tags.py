#!/usr/bin/env python3
"""Convert legacy set/tag membership only; deleted at milestone F (REBUILD.md).

Uses exact register public ids and variable ids from the current ledgers. Missing
ids are located next-id-sprint refusals, never inferred from labels or delivery hints. This
static conversion does not certify occurrence placement or review lock premises.
Run with --write-curation to apply; otherwise only the source-accounting report is
written. Existing locks are preserved for the later scheduled lock/build review.
"""

import argparse
import collections
import json
import re
import tomllib
from pathlib import Path

from rebuild_convert import ROOT, entries, literal


def frame_deferrals(frames, ledgers):
    """Recheck the remaining shared-identity refusal against its reviewed source facts.

    This one-shot conversion cannot invent release placement or split variable
    identity. A changed premise needs a new reviewed address, not a stale exception.
    """
    rows = []
    for path in sorted(frames.glob("*.jsonl")):
        for line in path.open():
            row = json.loads(line)
            if (row.get("provider"), row.get("register")) != ("sos", "lova"):
                continue
            if row["kind"] == "occurrence":
                rows.append(row)
    if not rows:
        raise ValueError("--frames contains no current LOVA occurrence frames")
    deferred = {}
    notes = []
    selected = [
        row
        for row in rows
        if (row["provider"], row["register"], row["table"], row["column"].lower())
        == ("sos", "lova", "A_LOVA_PERSON", "invarn8")
    ]
    if (
        len(selected) != 2
        or {row["label"] for row in selected}
        != {"Invandringsdatum 8 numerisk", "Invandringsdatum 9 numerisk"}
        or {row["source_key"] for row in selected} != {"n:invarn8"}
    ):
        raise ValueError(
            "LOVA INVARN8 source changed; review the numeric membership address"
        )
    lova_statements = tomllib.loads(ledgers["sos/lova"][0].read_text()).get(
        "statement", []
    )
    if any(
        "invarn8" in statement["subject"].lower()
        and {"variable", "spelling"} & statement.get("fields", {}).keys()
        for statement in lova_statements
    ):
        raise ValueError(
            "LOVA INVARN8 identity curation changed; review membership address"
        )
    deferred["sos/lova/invandringsdatum-8-numerisk"] = (
        "LOVA: shared INVARN8 variable remains ambiguous after exact-label selection; final identity review required"
    )
    notes.append(
        "LOVA's current frame has eighth/ninth numeric fields at Metadata - Variabelnivå!60/61 "
        "with shared spelling INVARN8 and key n:invarn8. A label guard identifies the eighth "
        "occurrence, but variable-wide membership expansion selects both again and remains "
        "ambiguous. Membership is deferred until separate final identities are curated."
    )
    return deferred, notes


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


def merge_memberships(path, additions, removals):
    """Replace only affected membership blocks; preserve all other authored text."""
    text = path.read_text() if path.exists() else ""
    starts = [match.start() for match in re.finditer(r"(?m)^\[\[statement\]\]", text)]
    prefix = text[: starts[0]] if starts else text
    blocks = [text[start:end] for start, end in zip(starts, [*starts[1:], len(text)])]
    kept = []
    merged = {}
    aliases = (
        {"lisa-disponibel-inkomst": "disponibel-inkomst"}
        if path.relative_to(ROOT).as_posix()
        == "curation/registers/scb/r34/curation.toml"
        else {}
    )
    for block in blocks:
        statement = tomllib.loads(block)["statement"][0]
        key = statement["subject"], next(iter(statement.get("fields", {})), None)
        if key[1] == "sets" and any(
            name in aliases for name in statement["fields"]["sets"]
        ):
            additions.setdefault(key, {"members": set(), "sources": set()})
        if statement["kind"] != "set" or (key not in additions and key not in removals):
            kept.append(block)
            continue
        if set(statement["fields"]) != {key[1]}:
            raise ValueError(
                f"{path}: combined membership/scalar setter requires review"
            )
        own = merged.setdefault(key, {"members": set(), "evidence": [], "caveat": []})
        old_members = {aliases.get(name, name) for name in statement["fields"][key[1]]}
        if removed := removals.get(key, set()) & old_members:
            if "D-9 membership only:" not in statement.get("evidence", ""):
                raise ValueError(
                    f"{path}: cannot remove membership without D-9 provenance"
                )
            old_members.difference_update(removed)
        own["members"].update(old_members)
        for field in ("evidence", "caveat"):
            value = statement.get(field)
            if (
                field == "evidence"
                and removed
                and not additions.get(key, {}).get("sources")
            ):
                value = "\n\n".join(
                    p
                    for p in value.split("\n\n")
                    if not p.startswith("D-9 membership only:")
                )
            if value and value not in own[field]:
                own[field].append(value)
    if kept and not kept[-1].endswith("\n\n"):
        kept[-1] += "\n"
    for key in sorted(additions.keys() | removals.keys()):
        addition = additions.get(key, {"members": set(), "sources": set()})
        own = merged.setdefault(key, {"members": set(), "evidence": [], "caveat": []})
        own["members"].update(addition["members"])
        if not own["members"]:
            continue
        if addition["sources"]:
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


def convert(*, frames, write_curation=False):
    ledgers = {}
    for path in sorted((ROOT / "curation/registers").glob("*/*/ids.toml")):
        data = tomllib.loads(path.read_text())
        provider, register = path.parts[-3:-1]
        public = f"{provider}/{data.get('public', register)}"
        if public in ledgers:
            raise ValueError(f"duplicate register public id: {public}")
        ledgers[public] = path.parent / "curation.toml", data.get("variables", {})
    deferred_addresses, address_notes = frame_deferrals(frames, ledgers)
    additions = collections.defaultdict(dict)
    removals = collections.defaultdict(lambda: collections.defaultdict(set))
    outcomes = []
    counts = collections.Counter()
    rows = list(source_rows())
    valid_members = collections.defaultdict(set)
    for _, field, target, member, _ in rows:
        if field == "set declaration":
            valid_members[target, member]
    for _, field, target, member, _ in rows:
        if field == "sets":
            public, _, variable = target.rpartition("/")
            if (
                public in ledgers
                and variable in ledgers[public][1]
                and target not in deferred_addresses
            ):
                valid_members[public, member].add(variable)
    # Existing curated memberships also count when the legacy group uses the same slug.
    for public, (path, variables) in ledgers.items():
        if not path.exists():
            continue
        for statement in tomllib.loads(path.read_text()).get("statement", []):
            subject = statement.get("subject", "")
            variable = subject.removeprefix("var:")
            if (
                statement["kind"] != "set"
                or not subject.startswith("var:")
                or variable not in variables
                or f"{public}/{variable}" in deferred_addresses
            ):
                continue
            for name in statement.get("fields", {}).get("sets", []):
                if public == "scb/lisa" and name == "lisa-disponibel-inkomst":
                    name = "disponibel-inkomst"
                if (public, name) in valid_members:
                    valid_members[public, name].add(variable)
    for source, field, target, member, metadata in rows:
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
        if field == "set declaration" and len(valid_members[target, member]) < 2:
            outcomes.append(
                (
                    "deferred",
                    source,
                    field,
                    target,
                    member,
                    f"fewer than two valid local ledger members after refusals ({len(valid_members[target, member])}); review in the next id sprint",
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
        if target in deferred_addresses:
            path, _ = ledgers[public]
            key = f"var:{variable}", field
            removals[path][key].add(member)
            outcomes.append(
                ("deferred", source, field, target, member, deferred_addresses[target])
            )
            continue
        if public not in ledgers or variable not in ledgers[public][1]:
            outcomes.append(
                (
                    "refused",
                    source,
                    field,
                    target,
                    member,
                    "no exact local variable ledger id; next id sprint; no id or delivery-column mapping inferred",
                )
            )
            continue
        path, _ = ledgers[public]
        key = f"var:{variable}", field
        if field == "sets" and len(valid_members[public, member]) < 2:
            removals[path][key].add(member)
            outcomes.append(
                (
                    "deferred",
                    source,
                    field,
                    target,
                    member,
                    "set has fewer than two valid local ledger members after refusals; review in the next id sprint",
                )
            )
            continue
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
    edits = {
        path: merge_memberships(path, additions[path], removals[path])
        for path in additions.keys() | removals.keys()
    }
    if write_curation:
        for path, text in edits.items():
            path.write_text(text)
    lines = [
        "# D-9 set/tag membership conversion",
        "",
        "Temporary conversion evidence; deleted with this converter at milestone F.",
        "",
        "Generated by `uv run --no-project scripts/rebuild_convert_sets_tags.py --frames FRAMES --write-curation`.",
        "",
        f"Source inventory: {counts['set declaration']} explicit sets, {counts['sets']} set memberships; {counts['tag declaration']} tags, {counts['tags']} variable memberships and {counts['register tags']} register memberships.",
        "Exact current local ledger ids are the conversion authority. Missing ids are refused for the next id sprint; delivery-column hints never supply identity. The five real-lock address refusals are rechecked against current source frames and deferred with named reasons below. Existing membership setters are consolidated per variable and field. Unrelated authored statements and all locks are preserved.",
        "",
        "This is static source accounting. Lock, curated build and verify were not run; placement and final resolver selection remain unverified. Tag search is not implemented. Presentation fields are dropped with reasons below; register memberships remain deferred.",
        "",
        "Sets with fewer than two distinct locally pinned members after refusals are deferred; their remaining memberships are not emitted. A local ledger id does not by itself prove final placement.",
        "",
        "| Field | Converted memberships | Refused memberships | Deferred memberships |",
        "| --- | ---: | ---: | ---: |",
    ]
    for field in ("sets", "tags", "register tags"):
        summary = collections.Counter(
            row[0]
            for row in outcomes
            if row[2] == field and not row[5].startswith("presentation metadata:")
        )
        lines.append(
            f"| {field} | {summary['converted']} | {summary['refused']} | {summary['deferred'] + summary['dropped']} |"
        )
    lines += [
        "",
        "## Current-frame deferrals",
        "",
        *[f"- {note}" for note in address_notes],
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
    parser.add_argument("--frames", type=Path, required=True)
    parser.add_argument("--write-curation", action="store_true")
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "scripts/rebuild_convert_sets_tags_report.md",
    )
    args = parser.parse_args()
    report, edits = convert(frames=args.frames, write_curation=args.write_curation)
    args.report.write_text(report)
    print(
        f"Accounted for set/tag sources; {len(edits)} register files {'written' if args.write_curation else 'proposed'}. Report: {args.report}"
    )


if __name__ == "__main__":
    main()
