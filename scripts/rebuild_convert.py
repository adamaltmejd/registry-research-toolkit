#!/usr/bin/env python3
"""One-shot curation converter/report; deleted at milestone F (REBUILD.md).

Extends the raw-TOML partition/owner census in scratchpad/fable/partitions.py.
The exact historical dry-run program is lost; its expected table is in REBUILD.md.
Reads only reg-build read JSONL, never the legacy builder's IR. Generated statements
are proposals: lock reviews today's premises, not the old builder's fingerprints.

Run reg-build read first, then:
  uv run --no-project scripts/rebuild_convert.py --frames DIR --out SCRATCH
  ... --lock --inputs DIR --assets DIR
The report output root must not exist. --write-curation replaces the checkout's
curation statements and locks directly; otherwise output stays in scratch.
Exit 1 when the requested lock fails, after writing the report.
"""

import argparse
import collections
import gc
import json
import re
import shutil
import subprocess
import time
import tomllib
from pathlib import Path

from rebuild_convert_cases import CaseSafety, audit_cached
from rebuild_convert_classifications import convert as classifications
from rebuild_convert_deferred import convert as deferred
from rebuild_convert_groups import normalize_sets
from rebuild_convert_identity import IdentityProjection
from rebuild_convert_selectors import collapse

ROOT = Path(__file__).resolve().parents[1]
# simplify: resolver finding codes are string literals; replace this inventory if codes become computed.
CURRENT_FINDING_CODES = {
    code
    for path in (ROOT / "crates/reg-resolve/src").rglob("*.rs")
    for code in re.findall(r'"([A-Z][0-9]+:[a-z][a-z-]+)"', path.read_text())
}
EXPECTED = {
    "No statement": 1461,
    "set variable": 97,
    "Merge": 55,
    "set": 145,
    "Regroup": 61,
}
ALIASES = {
    "name": "label",
    "data_type": "type",
    "is_sensitive": "sensitive",
    "is_identifier": "identifier",
    "representation": "format",
    "classification_declared": "classification",
    "data_warning": "notice",
    "measurement_unit": "unit",
    "column_name": "spelling",
}
FIELDS = {
    "label",
    "definition",
    "operational_definition",
    "description",
    "unit",
    "type",
    "length",
    "format",
    "spelling",
    "period",
    "value_set",
    "coded",
    "classification",
    "coverage",
    "notice",
    "sensitive",
    "identifier",
    "role",
    "temporality",
    "unit_type",
    "sets",
    "tags",
    "synonyms",
}


def fold(text):
    return text.lower()


def table_slug(text):
    # Address the resolver's unpinned tables (reg_resolve::slugify), not old ids.
    text = text.lower().translate(str.maketrans("åäáàöóéèü", "aaaaooeeu"))
    slug = re.sub("[^a-z0-9]+", "-", text).strip("-")
    return slug if slug and slug[0].isalpha() else "v" + ("-" + slug if slug else "")


def literal(value):
    if isinstance(value, dict):
        return (
            "{ "
            + ", ".join(f"{json.dumps(k)} = {literal(v)}" for k, v in value.items())
            + " }"
        )
    if isinstance(value, list):
        return "[" + ", ".join(map(literal, value)) + "]"
    return json.dumps(value, ensure_ascii=False)


def entries(path):
    """Raw array entries, retaining original authoring lines and excluding slug pins."""
    text = path.read_text()
    data = tomllib.loads(text)
    indices = collections.Counter()
    for line, content in enumerate(text.splitlines(), 1):
        match = re.fullmatch(r"\s*\[\[([\w.]+)\]\]\s*(?:#.*)?", content)
        if not match:
            continue
        kind = match[1]
        # Nested inline payloads belong to their outer entry.
        parts = kind.split(".")
        value = data
        for part in parts:
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(part, {})
        if not isinstance(value, list):
            continue
        index = indices[kind]
        indices[kind] += 1
        yield kind, line, value[index]


def round_summary(data):
    """Record the operator's decisions separately from remaining prerequisites."""
    resolved = {
        "classification clearing (52)": "new reader binds no source classification; dropped",
        "parallel metadata (592)": "211 case aliases need no statement; 236 co-delivered and 145 ordinary declarations use set sets, preserving each column's own source metadata; disagreements use F6/ambiguity",
        "operational_definition (191 previously deferred declarations)": "emit set operational_definition; d-set-fields supplies field application in the validation integration; missing endpoints or conflicting donors remain separately listed",
        "noncalendar placement (33)": "dropped with reason D-7",
    }
    targets = {
        (item["register"], item["variable"])
        for item in data["identity_deferred"]
        if item["reason"].startswith("D-12:")
    }
    data["round02_summary"] = {
        "resolved_operator_rulings": resolved,
        "changed_targets_without_local_ledger_id": len(targets),
    }
    text = ["## Open decisions", "", "Resolved by the operator in fix round 02:", ""]
    text.extend(f"- **{topic}:** {decision}." for topic, decision in resolved.items())
    text += [
        "",
        "Remaining genuinely open items:",
        "",
        "- Source-attribution provenance has no reviewed four-kind field mapping.",
        "- Ambiguous donor facts, unsupported release-name coverage, partial source windows, and indistinguishable case-only owners still require documentary selection.",
        "- Missing authored tables/releases and changed variable targets without local IDs remain prerequisites; no routing or ID is guessed. Noncalendar placement is already assigned to D-7.",
        "- Overlapping release assertions remain blocked by the current assertion-conflict policy.",
        "",
        f"Changed targets awaiting D-12: {len(targets)}. Register/variable names and original source lines are retained in report.json.",
        "",
        "The field application is supplied by d-set-fields; the model decision is resolved. The report records the exact runtime revision used for validation.",
        "",
        "| Category | Source declarations | Converted | Refused by observed lock | Dropped/no statement | Mapping |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    mappings = {
        "errata.delivered": "assert occurrence plus set operational_definition where uniquely stated; preserve source key and release guards",
        "representation.parallel": "case aliases: no statement; other declarations: set sets only; source metadata stays on each occurrence",
        "errata.version": "assert release where coverage and an existing table are known",
        "errata.occurrence_period": "set period for calendar coverage; noncalendar: D-7",
        "representation.delivery_metadata": "set description/label from agreeing pinned alternatives",
        "errata.classification_reference": "new reader binds no source classification; no statement",
    }
    for category, mapping in mappings.items():
        rows = [r for r in data["entries"] if r["category"] == category]
        counts = collections.Counter(r["outcome"] for r in rows)
        text.append(
            f"| `{category}` | {len(rows)} | {counts['converted']} | {counts['refused']} | {counts['dropped']} | {mapping} |"
        )
    text += [
        "",
        "Converted means emitted without an observed located refusal. If the whole-root lock fails before evaluation completes, other proposals remain uncertified; unreported errors are not assumed absent.",
        "",
    ]
    return "\n".join(text)


def remove_covered(proposals):
    """One authored correction can select nested source coverage windows."""
    kept = []
    spans = []
    for proposal in proposals:
        match = re.fullmatch(r"(.*?) @([^ ]+) where (.*)", proposal["subject"])
        if (
            proposal["kind"] != "set"
            or not match
            or ".." not in match[2]
            or "," in match[2]
        ):
            kept.append(proposal)
            spans.append(None)
            continue
        start, end = match[2].split("..")
        span = (match[1], match[3], start or "0001-01-01", end or "9999-12-31")
        payload = {k: v for k, v in proposal.items() if k != "subject"}
        covered = False
        for i in range(len(kept) - 1, -1, -1):
            other = spans[i]
            if (
                other is None
                or span[:2] != other[:2]
                or payload != {k: v for k, v in kept[i].items() if k != "subject"}
            ):
                continue
            if other[2] <= span[2] and span[3] <= other[3]:
                covered = True
                break
            if span[2] <= other[2] and other[3] <= span[3]:
                del kept[i]
                del spans[i]
        if not covered:
            kept.append(proposal)
            spans.append(span)
    return kept


class Converter:
    def __init__(self, frames, out):
        self.out = out
        self.records = []
        for path in sorted(frames.glob("*.jsonl")):
            with path.open() as stream:
                for line in stream:
                    record = json.loads(line)
                    if record["kind"] in {
                        "occurrence",
                        "table",
                        "register",
                        "release",
                        "value_set",
                    }:
                        self.records.append(record)
        self.ledgers = {}
        self.public = {}
        self.tables = {}
        for path in sorted((ROOT / "curation/registers").glob("*/*/ids.toml")):
            provider, register = path.parts[-3:-1]
            ledger = tomllib.loads(path.read_text())
            self.ledgers[provider, register] = ledger
            self.public[provider, ledger.get("public", register)] = register
            for table, slug in ledger.get("tables", {}).items():
                self.tables[provider, register, table] = slug
        self.rows = collections.defaultdict(list)
        self.releases = {}
        self.context_tables = {
            (r["provider"], r["register"], r["table"]): r
            for r in self.records
            if r["kind"] == "table"
        }
        self.context_registers = {
            (r["provider"], r["register"]): r
            for r in self.records
            if r["kind"] == "register"
        }
        self.value_sets = collections.defaultdict(set)
        for record in self.records:
            if record["kind"] == "table":
                self.tables.setdefault(
                    (record["provider"], record["register"], record["table"]),
                    table_slug(record["table"])
                    if record["table"]
                    else self.ledgers.get(
                        (record["provider"], record["register"]), {}
                    ).get("public", record["register"]),
                )
            if record["kind"] == "occurrence":
                self.tables.setdefault(
                    (record["provider"], record["register"], record["table"]),
                    table_slug(record["table"])
                    if record["table"]
                    else self.ledgers.get(
                        (record["provider"], record["register"]), {}
                    ).get("public", record["register"]),
                )
                self.rows[record["provider"], record["register"]].append(record)
            elif record["kind"] == "release":
                self.releases[
                    record["provider"],
                    record["register"],
                    record["table"],
                    record["release"],
                ] = record
            elif record["kind"] == "value_set":
                self.value_sets[
                    tuple(
                        sorted(
                            (item["code"], item["label"]) for item in record["items"]
                        )
                    )
                ].add(record["hash"])
        self.results = []
        self.statements = collections.defaultdict(dict)
        self.partitions = collections.Counter()
        self.case_only = []
        self.field_demand = collections.Counter()
        self.selector_members = {}
        self.selector_collapse = {}
        self.coordinate_spans = {}
        self.coordinate_labels = {}
        self.unscoped_year_coordinates = set()
        self.identity = IdentityProjection(self.records, self.ledgers)
        self.identity_audit = collections.Counter()
        self.identity_deferred = []
        self.grouping_audit = {}
        self.grouping_deferred = []
        self.case_safety = CaseSafety(self)
        self.current_kind = ""
        self.cached_audit = {}
        self.current_audit = {}
        self.native_registers = {}
        self.previous_cases = set()

    def normalize_identity(self):
        """Remove default restatements before checking local mint authority."""
        before = 0
        for owner, statements in self.statements.items():
            lookup = collections.defaultdict(list)
            for row in self.rows[owner]:
                try:
                    subject = self.selector(row)
                except ValueError:
                    continue
                lookup[subject].append(row)
                base = re.sub(r" and release = '[^']*'$", "", subject)
                if base != subject:
                    lookup[base].append(row)
            groups = collections.defaultdict(set)
            selected = {}
            for key, record in statements.items():
                statement = record["statement"]
                target = statement.get("fields", {}).get("variable")
                if target is None:
                    continue
                before += 1
                subject = statement["subject"]
                match = re.fullmatch(r"(.*) and type = '([^']*)'", subject)
                rows = lookup.get(match[1] if match else subject, [])
                if match:
                    rows = [r for r in rows if r.get("dtype") == match[2]]
                selected[key] = rows
                groups[target].update(id(row) for row in rows)
            for key, rows in selected.items():
                record = statements[key]
                target = record["statement"]["fields"]["variable"]
                target_status = self.identity.target_status(*owner, target)
                same_group = bool(rows) and all(
                    self.identity.group(row) is not None
                    and {id(r) for r in self.identity.group_members(row)}
                    == groups[target]
                    for row in rows
                )
                same_slug = bool(rows) and all(
                    self.identity.default_slug(row) == target for row in rows
                )
                reason = None
                if any(self.identity.status(r) == "gap_marker" for r in rows):
                    reason = "gap marker has no variable identity; retain its finding"
                    self.identity_audit["gap_marker"] += 1
                elif target != "none" and (
                    same_slug or (same_group and target_status != "local")
                ):
                    reason = "projected default identity already has the legacy outcome"
                    self.identity_audit["removed_default_restatements"] += 1
                elif target != "none":
                    status = target_status
                    if any(self.identity.status(r) == "unplaced" for r in rows):
                        reason = (
                            "D-7: identity assignment targets an unplaced occurrence"
                        )
                        self.identity_audit["unplaced"] += 1
                    elif status != "local":
                        reason = f"D-12: target has no local ledger id ({status})"
                        self.identity_audit[f"missing_local.{status}"] += 1
                    elif any(self.identity.status(r) == "gap_marker" for r in rows):
                        reason = (
                            "gap marker has no variable identity; retain its finding"
                        )
                        self.identity_audit["gap_marker"] += 1
                if reason:
                    self.identity_deferred.append(
                        {
                            "register": "/".join(owner),
                            "variable": target,
                            "sources": record["origins"],
                            "reason": reason,
                            "statements": 1,
                        }
                    )
                    del statements[key]
            # Partition and column_owner declarations can state the same effect.
            # A covering selector already carries the narrower one's entire claim.
            covers = collections.defaultdict(list)
            for key in sorted(selected, key=lambda key: -len(selected[key])):
                if key not in statements:
                    continue
                record = statements[key]
                target = record["statement"]["fields"]["variable"]
                members = {id(row) for row in selected[key]}
                covering = next(
                    (r for own, r in covers[target] if members and members <= own), None
                )
                if covering is None:
                    covers[target].append((members, record))
                    continue
                covering["origins"] = sorted(
                    set(covering["origins"] + record["origins"])
                )
                for field in ("evidence", "caveat"):
                    values = list(
                        dict.fromkeys(
                            str(r["statement"][field])
                            for r in (covering, record)
                            if r["statement"].get(field)
                        )
                    )
                    if values:
                        covering["statement"][field] = "\n\n".join(values)
                del statements[key]
                self.identity_audit["coalesced_duplicate_effects"] += 1
            while True:
                assignments = {
                    id(row): statements[key]["statement"]["fields"]["variable"]
                    for key, rows in selected.items()
                    if key in statements
                    for row in rows
                }
                orphaned = self.identity.orphaned_markers(*owner, assignments)
                concurrent = self.identity.concurrent_fold_assignments(
                    *owner, assignments
                )
                removed = False
                for key, rows in selected.items():
                    if key not in statements:
                        continue
                    details = [
                        detail for row in rows for detail in concurrent.get(id(row), [])
                    ]
                    if not details and not any(
                        self.identity.group(row) in orphaned for row in rows
                    ):
                        continue
                    record = statements.pop(key)
                    self.identity_deferred.append(
                        {
                            "register": "/".join(owner),
                            "variable": record["statement"]["fields"]["variable"],
                            "sources": record["origins"],
                            "reason": (
                                "D-11: identity assignment merges concurrent different "
                                "folded spellings in one table; retain separate variables"
                                if details
                                else "identity assignment would orphan a default gap marker; "
                                "retain its finding"
                            ),
                            "conflicts": list(
                                {
                                    json.dumps(detail, sort_keys=True): detail
                                    for detail in details
                                }.values()
                            ),
                            "statements": 1,
                        }
                    )
                    self.identity_audit[
                        "deferred_concurrent_folds" if details else "orphan_gap_marker"
                    ] += 1
                    removed = True
                if not removed:
                    break
        self.identity_audit["before"] = before
        self.identity_audit["after"] = sum(
            "variable" in r["statement"].get("fields", {})
            for records in self.statements.values()
            for r in records.values()
        )
        retained = collections.Counter(
            origin
            for records in self.statements.values()
            for record in records.values()
            for origin in set(record["origins"])
        )
        reasons = collections.defaultdict(set)
        for record in self.identity_deferred:
            for origin in record["sources"]:
                reasons[origin].add(record["reason"])
        for result in self.results:
            if result["source"] in reasons:
                result["identity_notes"] = sorted(reasons[result["source"]])
                if not retained[result["source"]]:
                    result["outcome"] = "dropped"
                    result["reason"] = "; ".join(result["identity_notes"])

    def span(self, row):
        if not row.get("release"):
            table = self.context_tables.get(
                (row["provider"], row["register"], row["table"]), {}
            )
            register = self.context_registers.get(
                (row["provider"], row["register"]), {}
            )
            return (
                row.get("from")
                or table.get("from")
                or register.get("from")
                or "0001-01-01",
                row.get("to") or table.get("to") or register.get("to") or "9999-12-31",
            )
        source = self.releases.get(
            (row["provider"], row["register"], row["table"], row.get("release")), row
        )
        return source.get("from"), source.get("to")

    def selector(self, row):
        table = self.tables.get((row["provider"], row["register"], row["table"]))
        if table is None:
            # Implicit tables have no ledger entry and use the empty handle.
            if row["table"] != "":
                raise ValueError("source table has no ledger slug")
            table = self.ledgers.get((row["provider"], row["register"]), {}).get(
                "public", row["register"]
            )
        if any(
            "'" in v or "\n" in v or "\r" in v
            for v in (row["source_key"], row.get("release") or "")
        ):
            raise ValueError("source guard cannot be expressed by address grammar")
        guards = [f"key = '{row['source_key']}'"]
        if row.get("release"):
            guards.append(f"release = '{row['release']}'")
        spelling = fold(row["column"]) or "<none>"
        if any(c.isspace() for c in spelling) or "@" in spelling:
            raise ValueError("source spelling cannot be expressed by address grammar")
        scope = ""
        if not row.get("release"):
            start, end = self.span(row)
            start = "" if start == "0001-01-01" else start
            end = "" if end == "9999-12-31" else end
            unsupported = any(
                value and not 1900 <= int(value[:4]) <= 2099 for value in (start, end)
            )
            if unsupported:
                owner = row["provider"], row["register"]
                if owner not in self.coordinate_spans:
                    peers = collections.defaultdict(set)
                    for other in self.rows[owner]:
                        peers[
                            other["table"], fold(other["column"]), other["source_key"]
                        ].add(self.span(other))
                    self.coordinate_spans[owner] = peers
                coordinate = row["table"], fold(row["column"]), row["source_key"]
                if len(self.coordinate_spans[owner][coordinate]) != 1:
                    raise ValueError(
                        "unsupported selector year outside 1900-2099; removing scope would widen source occurrences"
                    )
                # The key/table/fold guards select exactly the same whole sources.
                self.unscoped_year_coordinates.add((*owner, *coordinate))
            elif start or end:
                scope = f" @{start}..{end}"
        owner = row["provider"], row["register"]
        if owner not in self.coordinate_labels:
            labels = collections.defaultdict(set)
            for other in self.rows[owner]:
                base = other["table"], fold(other["column"]), other["source_key"]
                labels[(*base, None)].add(other.get("label"))
                if other.get("release"):
                    labels[(*base, other["release"])].add(other.get("label"))
            self.coordinate_labels[owner] = labels
        labels = self.coordinate_labels[owner][
            row["table"], fold(row["column"]), row["source_key"], row.get("release")
        ]
        if len(labels) > 1:
            label = row.get("label")
            if not label or any(c in label for c in ("'", "\n", "\r")):
                raise ValueError(
                    "ambiguous source label cannot be expressed by an exact guard"
                )
            guards.append(f"label = '{label}'")
        return f"{table}: {spelling}{scope} where " + " and ".join(guards)

    def add(self, at, provider, register, statement):
        grouping = statement["kind"] == "set" and "sets" in statement.get("fields", {})
        payload = {
            k: v
            for k, v in statement.items()
            if not grouping or k not in {"evidence", "caveat"}
        }
        key = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        record = self.statements[provider, register].setdefault(
            key, {"statement": statement, "origins": [], "contexts": {}}
        )
        record["origins"].append(at)
        if grouping:
            for field in ("evidence", "caveat"):
                if statement.get(field):
                    values = record["contexts"].setdefault(field, [])
                    if statement[field] not in values:
                        values.append(statement[field])
                    record["statement"][field] = "\n\n".join(values)

    def compact(self, proposals, provider, register):
        if all(
            p["kind"] == "set"
            and not (p.get("fields", {}).keys() & {"variable", "sets"})
            for p in proposals
        ):
            # Assertions add source rows before scalar application. A release guard
            # proved redundant against raw frames can select newly asserted rows.
            return remove_covered(proposals)
        # Remove release guards only when every source occurrence of the remaining
        # selector is selected. This keeps finite-edition corrections finite.
        owner = (provider, register)
        if owner not in self.selector_members:
            members = collections.defaultdict(set)
            for row in self.rows[owner]:
                try:
                    subject = self.selector(row)
                except ValueError:
                    continue
                base = re.sub(r" and release = '[^']*'$", "", subject)
                members[base].add(subject)
            self.selector_members[owner] = members
        groups = collections.defaultdict(list)
        for proposal in proposals:
            base = re.sub(r" and release = '[^']*'$", "", proposal["subject"])
            payload = {k: v for k, v in proposal.items() if k != "subject"}
            groups[base, json.dumps(payload, sort_keys=True)].append(proposal)
        result = []
        for (base, _), group in groups.items():
            if self.selector_members[owner].get(base) == {p["subject"] for p in group}:
                result.append(group[0] | {"subject": base})
            else:
                result.extend(group)
        return remove_covered(result)

    def match(self, provider, register, entry, pins, variants, check_case=True):
        rows = self.rows[provider, register]
        column = entry.get("column")
        variable = entry.get("variable", "")
        if column:
            rows = [r for r in rows if r["column"] == column]
        if variable:
            key = (
                "v" + variable.split(".")[1]
                if provider == "scb" and "." in variable
                else None
            )
            slug = variable.rsplit("/", 1)[-1]
            if key:
                rows = [r for r in rows if r["source_key"] == key]
            elif (
                provider == "sos"
                and column
                and "." in variable
                and variable.split(".", 1)[0]
                == self.native_registers.get((provider, register))
                and variable.split(".", 1)[1].lower() == column.lower()
            ):
                # The SOS reader keys literal named columns, not legacy numeric ids.
                rows = [r for r in rows if r["source_key"] == "n:" + column.lower()]
            elif variable in pins:
                anchors = (
                    self.ledgers.get((provider, register), {})
                    .get("variables", {})
                    .get(pins[variable], [])
                )
                if any(" # " in a or " @ " in a for a in anchors):
                    raise ValueError(
                        "qualified ledger endpoint needs reviewed key/day selection"
                    )
                rows = [
                    r
                    for r in rows
                    if any(
                        a.split(" # ")[0].split(" @ ")[0]
                        == (
                            f"{r['table']}: {r['column']}"
                            if r["table"]
                            else r["column"]
                        )
                        for a in anchors
                    )
                ]
            else:
                anchors = (
                    self.ledgers.get((provider, register), {})
                    .get("variables", {})
                    .get(slug, [])
                )
                if any(" # " in a or " @ " in a for a in anchors):
                    raise ValueError(
                        "qualified ledger endpoint needs reviewed key/day selection"
                    )
                rows = [
                    r
                    for r in rows
                    if any(
                        a.split(" # ")[0].split(" @ ")[0]
                        == (
                            f"{r['table']}: {r['column']}"
                            if r["table"]
                            else r["column"]
                        )
                        for a in anchors
                    )
                ]
        elif not column:
            raise ValueError("entry has no occurrence endpoint")
        variant = entry.get("variant")
        if entry.get("deldatamangd"):
            rows = [r for r in rows if r["table"] == entry["deldatamangd"]]
        if variant:
            slug = variants.get(
                variant, self.tables.get((provider, register, variant), variant)
            )
            rows = [
                r
                for r in rows
                if self.tables.get((provider, register, r["table"]), r["table"]) == slug
            ]
        editions = entry.get("editions") or (
            [entry["edition"]] if "edition" in entry else None
        )
        versions = entry.get("versions") or entry.get("source_editions")
        if editions:
            handles = {
                "r" + str(e) if provider == "scb" and str(e).isdigit() else str(e)
                for e in editions
            }
            rows = [r for r in rows if r.get("release") in handles]
        if versions:
            rows = [
                r
                for r in rows
                if self.releases.get(
                    (provider, register, r["table"], r.get("release")), {}
                ).get("name")
                in versions
            ]
        periods = entry.get("periods")
        if entry.get("valid_from") or entry.get("valid_to"):
            periods = [
                [
                    entry.get("valid_from", "0001-01-01"),
                    entry.get("valid_to", "9999-12-31"),
                ]
            ]
        if periods:
            # Never widen a partial legacy window to a whole source occurrence.
            selected = []
            for row in rows:
                start, end = self.span(row)
                if (
                    start
                    and end
                    and any(
                        max(a, start) <= min(b, end) and not (a <= start and end <= b)
                        for a, b in periods
                    )
                ):
                    raise ValueError("legacy period cuts a whole source occurrence")
                if start and end and any(a <= start and end <= b for a, b in periods):
                    selected.append(row)
            rows = selected
        expected_labels = {
            claim["value"]
            for claim in entry.get("expected_fields", [])
            if claim["name"] == "name" and claim["status"] == "value"
        }
        if expected_labels:
            rows = [row for row in rows if row.get("label") in expected_labels]
        if not rows:
            raise ValueError("no exact new-frame occurrence endpoint")
        if check_case:
            return self.case_safety.authorize(
                provider, register, self.current_kind, entry, rows
            )
        return rows

    def partition(self, entry, rows, at):
        # Reuses partitions.py's literal -> folded spelling -> owner census.
        columns = entry.get("columns", {})
        if not columns:
            raise ValueError("partition has no literal columns")
        owners = set(columns.values())
        groups = collections.defaultdict(set)
        for spelling, owner in columns.items():
            groups[fold(spelling)].add(owner)
        if len(groups) == 1 and len(owners) > 1:
            self.case_only.append(at)
            self.partitions["set variable"] += 1
            raise ValueError(
                "case-only split: folded spelling cannot distinguish owners"
            )
        active = [r for r in rows if r["column"] in columns and all(self.span(r))]
        overlapping = False
        together = False
        for i, left in enumerate(active):
            for right in active[i + 1 :]:
                if fold(left["column"]) == fold(right["column"]):
                    continue
                a, b = self.span(left), self.span(right)
                if max(a[0], b[0]) <= min(a[1], b[1]):
                    overlapping = True
                    together |= left["table"] == right["table"]
        if len(owners) == 1:
            result = "set" if together else "Merge" if overlapping else "No statement"
        elif not overlapping:
            result = "set variable"
        elif all(
            len({columns[c] for c in columns if fold(c) == group}) == 1
            for group in groups
        ) and len(owners) == len(groups):
            result = "No statement"
        else:
            result = "Regroup"
        self.partitions[result] += 1
        if any(len(group) > 1 for group in groups.values()):
            raise ValueError(
                "partition includes case-only owners beside other spellings; needs reviewed regrouping"
            )
        return result

    def convert_file(self, path):
        data = tomllib.loads(path.read_text())
        provider = path.parent.name
        public = data.get("register", {}).get("slug", path.stem)
        register = self.public.get((provider, public), public)
        self.native_registers[provider, register] = str(
            data.get("register", {}).get("native_id", "")
        )
        pins = dict(data.get("variable", {}))
        auto = path.with_name(path.stem + ".auto.toml")
        if auto.exists():
            pins = tomllib.loads(auto.read_text()).get("variable", {}) | pins
        pins = {k: v.get("slug") if isinstance(v, dict) else v for k, v in pins.items()}
        variants = {
            v["native_id"]: (
                self.tables.get((provider, register, v.get("display_group")))
                or (
                    table_slug(v["display_group"])
                    if v.get("display_group")
                    else v["slug"]
                )
            )
            for v in data.get("variant", [])
            if "native_id" in v and "slug" in v
        }
        variants.update(
            {
                v["slug"]: variants[v["native_id"]]
                for v in data.get("variant", [])
                if "native_id" in v and "slug" in v
            }
        )
        declarations = list(entries(path))
        self.case_safety.prepare(provider, register, declarations, pins, variants)
        for kind, line, entry in declarations:
            self.current_kind = kind
            # Count explicit legacy field requests even when endpoint conversion fails.
            if kind == "errata.field":
                self.field_demand[ALIASES.get(entry["field"], entry["field"])] += 1
            elif kind == "errata.flags":
                self.field_demand.update(
                    ALIASES[k] for k in ("is_sensitive", "is_identifier") if k in entry
                )
            elif kind in {
                "identity.column_owner",
                "identity.split",
                "identity.unassigned",
            }:
                self.field_demand["variable"] += 1
            elif kind == "errata.occurrence_period":
                self.field_demand["period"] += 1
            elif kind == "identity.route":
                self.field_demand["table"] += 1
            elif kind == "enrichment.description":
                self.field_demand["description"] += 1
            at = f"{path.relative_to(ROOT)}:{line}"
            self.current_source = at
            result = {
                "source": at,
                "category": kind,
                "provider": provider,
                "register": register,
                "column": entry.get("column"),
                "variable": entry.get("variable"),
            }
            proposals = []
            try:
                evidence = (
                    entry.get("evidence")
                    or entry.get("ref")
                    or entry.get("reason")
                    or entry.get("columns_ref")
                )
                caveat = entry.get("data_warning")
                if kind == "identity.partition":
                    rows = self.match(provider, register, entry, pins, variants)
                    outcome = self.partition(entry, rows, at)
                    result["partition"] = outcome
                    if outcome == "set":
                        # D11 forbids merging simultaneous spellings. Group for picking.
                        fields = {
                            "sets": [
                                f"legacy-{pins.get(entry['variable'], entry['variable'].replace('.', '-'))}"
                            ]
                        }
                        for row in rows:
                            if row["column"] in entry["columns"]:
                                proposals.append(
                                    {
                                        "kind": "set",
                                        "subject": self.selector(row),
                                        "fields": fields,
                                    }
                                )
                    else:
                        for row in rows:
                            owner = entry["columns"].get(row["column"])
                            if owner:
                                if owner not in pins:
                                    raise ValueError("partition owner has no slug pin")
                                proposals.append(
                                    {
                                        "kind": "set",
                                        "subject": self.selector(row),
                                        "fields": {"variable": pins[owner]},
                                    }
                                )
                elif kind == "errata.column":
                    variant = entry.get("variant", "")
                    table = variants.get(variant, variant)
                    if table not in {
                        slug
                        for (p, r, _), slug in self.tables.items()
                        if (p, r) == (provider, register)
                    }:
                        raise ValueError(
                            "asserted column needs a reviewed existing table"
                        )
                    fields = {
                        ALIASES.get(k, k): v
                        for k, v in entry.items()
                        if ALIASES.get(k, k) in FIELDS
                    }
                    # A warning is statement context, not an occurrence source field.
                    # It is retained in caveat below.
                    fields.pop("notice", None)
                    operational_definition = fields.pop("operational_definition", None)
                    if "classification" in fields:
                        raise ValueError(
                            "asserted column classification needs reviewed family/version mapping"
                        )
                    periods = entry.get("versions")
                    if periods and all(re.fullmatch(r"\d{4}", p) for p in periods):
                        scope = ",".join(periods)
                    elif re.fullmatch(r"\d{4}-\d{4}", entry.get("holdings_period", "")):
                        scope = entry["holdings_period"].replace("-", "..")
                    else:
                        raise ValueError(
                            "asserted column needs an explicit expressible period"
                        )
                    proposals.append(
                        {
                            "kind": "assert",
                            "subject": f"{table}: {entry['column']} @{scope}",
                            "fact": {"entity": "occurrence", "fields": fields},
                        }
                    )
                    if operational_definition is not None:
                        proposals.append(
                            {
                                "kind": "set",
                                "subject": proposals[-1]["subject"],
                                "fields": {
                                    "operational_definition": operational_definition
                                },
                            }
                        )
                elif kind == "identity.split":
                    if entry.get("by") != "data_type":
                        raise ValueError(
                            "identity split needs a reviewed non-type selector"
                        )
                    rows = self.match(provider, register, entry, pins, variants)
                    for part in entry["parts"]:
                        if part["owner"] not in pins:
                            raise ValueError("split owner has no slug pin")
                        matching = [
                            r for r in rows if r.get("dtype") == part["data_type"]
                        ]
                        if not matching:
                            raise ValueError("split type has no exact frame endpoint")
                        for row in matching:
                            subject = self.selector(row)
                            dtype = row["dtype"]
                            if "'" in dtype:
                                raise ValueError(
                                    "split type cannot be expressed by guard grammar"
                                )
                            proposals.append(
                                {
                                    "kind": "set",
                                    "subject": subject + f" and type = '{dtype}'",
                                    "fields": {"variable": pins[part["owner"]]},
                                }
                            )
                    evidence = evidence or "Legacy explicit data-type identity split"
                elif kind in {"coding.documented", "coding.extend", "coding.choice"}:
                    items = entry.get("members") or entry.get("list_members")
                    if not items:
                        raise ValueError(
                            "coding choice needs a reviewed selected source value-set hash"
                        )
                    wanted = sorted((str(c), str(label)) for c, label in items)
                    hashes = self.value_sets.get(tuple(wanted), set())
                    if len(hashes) != 1:
                        raise ValueError(
                            "code list has no unique existing frame value set; needs handwritten assertion"
                        )
                    for row in self.match(provider, register, entry, pins, variants):
                        proposals.append(
                            {
                                "kind": "set",
                                "subject": self.selector(row),
                                "fields": {"value_set": next(iter(hashes))},
                            }
                        )
                elif kind == "acknowledge":
                    if entry["code"] not in CURRENT_FINDING_CODES:
                        raise ValueError("legacy finding code; no new-model finding")
                    subject = entry.get("subject", "")
                    slug = subject.rsplit("/", 1)[-1]
                    if slug not in self.ledgers.get((provider, register), {}).get(
                        "variables", {}
                    ):
                        raise ValueError(
                            "legacy diagnostic subject has no public ledger endpoint"
                        )
                    proposals.append(
                        {
                            "kind": "ack",
                            "subject": f"var:{slug}",
                            "finding": entry["code"],
                            "severity": "review",
                            "evidence": evidence,
                        }
                    )
                elif kind == "code_label_pair":
                    code, label = entry["code"].split("/"), entry["label"].split("/")
                    if code[:2] != label[:2] or code[:2] != [provider, public]:
                        raise ValueError(
                            "link endpoints belong to a different register"
                        )
                    proposals.append(
                        {
                            "kind": "link",
                            "subject": f"var:{label[-1]}",
                            "relation": "label_of",
                            "object": entry["code"],
                        }
                    )
                    evidence = evidence or "Legacy code_label_pair declaration"
                elif kind == "identity.column_owner":
                    owner = entry["owner"]
                    if owner not in pins:
                        raise ValueError("column owner has no slug pin")
                    for row in self.match(provider, register, entry, pins, variants):
                        proposals.append(
                            {
                                "kind": "set",
                                "subject": self.selector(row),
                                "fields": {"variable": pins[owner]},
                            }
                        )
                elif kind == "identity.unassigned":
                    for row in self.match(provider, register, entry, pins, variants):
                        proposals.append(
                            {
                                "kind": "set",
                                "subject": self.selector(row),
                                "fields": {"variable": "none"},
                            }
                        )
                elif kind in {
                    "errata.field",
                    "errata.flags",
                    "errata.data_type",
                    "enrichment.description",
                    "coding.uncoded",
                    "coding.warning",
                }:
                    if kind == "errata.field":
                        if entry.get("clear"):
                            raise ValueError("four-kind fields cannot clear a value")
                        name = ALIASES.get(entry["field"], entry["field"])
                        fields = {name: entry["value"]}
                    elif kind == "errata.flags":
                        fields = {
                            ALIASES[k]: entry[k]
                            for k in ("is_sensitive", "is_identifier")
                            if k in entry
                        }
                    elif kind == "errata.data_type":
                        fields = {"type": entry["data_type"]}
                    elif kind == "enrichment.description":
                        fields = {"description": entry["description"]}
                        evidence = entry.get("provenance")
                    elif kind == "coding.uncoded":
                        # coded=false does not erase source evidence or invent a value set.
                        fields = {"coded": False}
                    else:
                        fields = {"notice": entry["data_warning"]}
                    if not fields or not fields.keys() <= FIELDS:
                        raise ValueError("legacy field has no four-kind field")
                    if "classification" in fields and not str(
                        fields["classification"]
                    ).startswith("class/"):
                        raise ValueError(
                            "classification correction needs reviewed family/version mapping"
                        )
                    for row in self.match(provider, register, entry, pins, variants):
                        proposals.append(
                            {
                                "kind": "set",
                                "subject": self.selector(row),
                                "fields": fields,
                            }
                        )
                elif kind == "variant":
                    raise ValueError(
                        "table identity already in ledger; panel metadata has no four-kind field"
                    )
                else:
                    proposals = deferred(
                        self, provider, register, kind, entry, pins, variants
                    )
                    if proposals is None:
                        raise ValueError(
                            f"{kind}: requires reviewed translation of legacy payload/guards"
                        )
                if not proposals:
                    if kind == "representation.parallel" and entry.get("case_aliases"):
                        result.update(
                            outcome="dropped",
                            reason="case aliases covered by folded spellings; no statement",
                            statements=0,
                        )
                        self.results.append(result)
                        continue
                    raise ValueError("no expressible selected occurrence")
                if not evidence:
                    raise ValueError("legacy declaration supplies no evidence")
                proposals = self.compact(proposals, provider, register)
                for statement in proposals:
                    statement.setdefault("evidence", evidence)
                    if caveat:
                        statement["caveat"] = caveat
                    self.add(at, provider, register, statement)
                result["outcome"] = "converted"
                result["statements"] = len(proposals)
            except ValueError as error:
                result.update(outcome="dropped", reason=str(error))
            self.results.append(result)

    def write(self, write_curation=False):
        self.curation = ROOT / "curation" if write_curation else self.out / "curation"
        if not write_curation:
            shutil.copytree(ROOT / "curation", self.curation)
        for path in self.curation.rglob("curation.lock"):
            path.unlink()
        for path in self.curation.rglob("curation.toml"):
            path.unlink()
        self.counts = collections.Counter()
        self.locations = {}
        for (provider, register), records in sorted(self.statements.items()):
            if not records:
                continue
            path = self.curation / "registers" / provider / register / "curation.toml"
            path.parent.mkdir(parents=True, exist_ok=True)
            text = ""
            next_line = 1
            for key, record in sorted(records.items()):
                statement = dict(record["statement"])
                statement["evidence"] = "\n".join(sorted(set(record["origins"]))) + (
                    "\n\n" + statement["evidence"] if statement.get("evidence") else ""
                )
                text += "\n[[statement]]\n"
                line = next_line + 1
                self.locations[str(path.resolve()), line] = record
                relative = path.relative_to(ROOT if write_curation else self.out)
                record["location"] = f"{relative}:{line}"
                body = (
                    "\n".join(f"{k} = {literal(v)}" for k, v in statement.items())
                    + "\n"
                )
                text += body
                next_line += 2 + body.count("\n")
                kind = statement["kind"]
                self.counts[kind] += 1
                for field in statement.get("fields", {}):
                    self.counts[f"{kind}.fields.{field}"] += 1
                for field in statement.get("fact", {}).get("fields", {}):
                    self.counts[f"assert.fields.{field}"] += 1
                if kind == "assert":
                    self.counts[f"assert.entity.{statement['fact']['entity']}"] += 1
                if kind == "link":
                    self.counts[f"link.relation.{statement['relation']}"] += 1
                if kind == "ack":
                    self.counts[f"ack.finding.{statement['finding']}"] += 1
            path.write_text(text)

    def report(self, lock):
        refused = set()
        errors = collections.Counter()
        located = []
        notices = []
        refusal_lines = []
        for diagnostic in lock["refusals"]:
            match = re.match(
                r"(?:reg-build: )?(.*curation\.toml):(\d+): (.*)", diagnostic
            )
            if match:
                path, line, error = match.groups()
                record = self.locations.get((str(Path(path).resolve()), int(line)), {})
                if error.startswith("redundant-set:"):
                    notices.append(
                        {
                            "location": record.get("location", f"{path}:{line}"),
                            "message": error,
                            "sources": record.get("origins", []),
                        }
                    )
                    continue
                refused.update(record.get("origins", []))
                category = re.sub(
                    r"overlapping overrides with .*", "overlapping overrides", error
                )
                category = re.sub(
                    r"^unknown field (`[^`]+`), expected.*",
                    r"unknown field \1",
                    category,
                )
                category = re.sub(
                    r'^variable "[^"]+" is not a ledger slug$',
                    "variable is not a ledger slug",
                    category,
                )
                category = re.sub(
                    r'^set variable would orphan gap marker for "[^"]+";',
                    "set variable would orphan gap marker;",
                    category,
                )
                errors[category] += 1
                located.append(
                    {
                        "location": record.get("location", f"{path}:{line}"),
                        "error": error,
                        "sources": record.get("origins", []),
                    }
                )
            refusal_lines.append(diagnostic)
        for entry in self.results:
            if entry["source"] in refused:
                entry["outcome"] = "refused"
            elif entry["outcome"] == "converted":
                entry["lock_status"] = (
                    "accepted"
                    if lock["status"] == "accepted"
                    else "not certified: whole-root lock failed or was not run"
                )
        lock["grouped_errors"] = dict(errors)
        lock["located_refusals"] = located
        lock["stderr_lines"] = lock["refusals"]
        lock["refusals"] = refusal_lines
        lock["located_notices"] = notices
        dropped = collections.Counter(
            r["reason"] for r in self.results if r["outcome"] == "dropped"
        )
        outcomes = collections.Counter(r["outcome"] for r in self.results)
        data = {
            "source_entries": len(self.results),
            "outcomes": dict(outcomes),
            "statement_counts": dict(sorted(self.counts.items())),
            "selector_collapse": self.selector_collapse,
            "identity_audit": dict(self.identity_audit),
            "grouping_audit": getattr(self, "grouping_audit", {}),
            "grouping_deferred": getattr(self, "grouping_deferred", []),
            "unsupported_year_coordinates_safely_unscoped": len(
                self.unscoped_year_coordinates
            ),
            "identity_deferred": self.identity_deferred,
            "case_audit": dict(self.case_safety.counts),
            "case_conflicts": self.case_safety.conflicts,
            "previous_proposal_audit": self.cached_audit,
            "current_proposal_audit": getattr(self, "current_audit", {}),
            "previous_case_drops": dict(
                collections.Counter(
                    ("still dropped: " + r["reason"])
                    if r["outcome"] == "dropped"
                    else "emitted"
                    for r in self.results
                    if r["source"] in self.previous_cases
                )
            ),
            "explicit_legacy_set_field_requests": dict(
                sorted(self.field_demand.items())
            ),
            "dropped_reasons": dict(sorted(dropped.items())),
            "partitions_expected": EXPECTED,
            "partitions_observed": dict(self.partitions),
            "case_only": self.case_only,
            "generated": [
                {
                    "location": record["location"],
                    "sources": record["origins"],
                    "kind": record["statement"]["kind"],
                }
                for records in self.statements.values()
                for record in records.values()
            ],
            "lock": lock,
            "entries": self.results,
            "uncertified_converted_entries": sum(
                r.get("lock_status", "").startswith("not certified")
                for r in self.results
            ),
        }
        summary = round_summary(data)
        (self.out / "report.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        )
        text = [
            "# One-shot curation conversion report",
            "",
            "Proposals only. Lock records current premises; old fingerprints have not been certified.",
            "",
            f"Scoped declarations: {len(self.results)} = {sum(r['category'] not in ('variant', 'classification') for r in self.results)} register curation entries + {sum(r['category'] == 'variant' for r in self.results)} table declarations + {sum(r['category'] == 'classification' for r in self.results)} classification declarations/bindings. Slug pins and inline payload rows are not additional statements.",
            "",
            "| Outcome | Entries |",
            "| --- | ---: |",
        ]
        text += [f"| {k} | {v} |" for k, v in sorted(outcomes.items())]
        text += ["", "| Statement / field / subtype | Count |", "| --- | ---: |"]
        text += [f"| `{k}` | {v} |" for k, v in sorted(self.counts.items())]
        text += [
            "",
            "Explicit legacy set-field requests, including entries whose selectors or payloads still need review (counts are source entries, not expanded statements):",
            "",
            "| Field | Entries |",
            "| --- | ---: |",
        ]
        text += [
            f"| `{field}` | {count} |"
            for field, count in sorted(self.field_demand.items())
        ]
        text += [
            "",
            "## D-3b field scope",
            "",
            "Counts below include mechanically mapped deferred categories. These are proposals, not lock-certified applications.",
            "",
            "| Set field | Proposed statements |",
            "| --- | ---: |",
            *[
                f"| `{key.removeprefix('set.fields.')}` | {value} |"
                for key, value in sorted(self.counts.items())
                if key.startswith("set.fields.")
            ],
            "",
            "The counts include translated parallel grouping (`sets`) and delivered donor wording (`operational_definition`). Field application is tested with the d-set-fields integration. Calendar corrections use `period`; the 33 noncalendar declarations stay dropped as D-7. Classification clearing and parallel metadata semantics are resolved below.",
            "",
            "## Identity and case audit",
            "",
            f"Historical set variable: 12,151 statements from 6,088 owner entries. Current proposed set variable: {self.counts['set.fields.variable']}. The current pass also projects partitions previously skipped by the approximate census; its pre-filter population differs from the historical one.",
            "",
            f"Projection audit: `{json.dumps(dict(self.identity_audit), sort_keys=True)}`.",
            "",
            f"Case safety: `{json.dumps(dict(self.case_safety.counts), sort_keys=True)}`. Remaining cases are listed in report.json.",
            "",
            f"Cached proposal audit: `{json.dumps(self.cached_audit.get('counts', {}), sort_keys=True)}`. This is a static reconstruction, not a new real-frame lock.",
            "",
            f"Missing ledger causes: `{json.dumps(self.cached_audit.get('missing_ledger_causes', {}), sort_keys=True)}`. A same-named id in another register is not authority to borrow it; changed groups without local ids wait for D-12.",
        ]
        text += [
            "",
            "| Partition outcome | Historical | Observed from frames | Four-kind translation |",
            "| --- | ---: | ---: | --- |",
        ]
        translations = {
            "No statement": "skip: projected default identity (not resolver-certified)",
            "set variable": "set variable (case-only splits dropped)",
            "Merge": "set variable onto one ledger id",
            "set": "set sets (simultaneous columns stay distinct)",
            "Regroup": "set variable per curated owner",
        }
        text += [
            f"| {k} | {v} | {self.partitions[k]} | {translations[k]} |"
            for k, v in EXPECTED.items()
        ]
        text += [
            "",
            "The historical program/output are missing. This extends the recovered partitions.py census; observed counts use current source periods, without edition shadowing. They are not a resolver equivalence proof.",
            "",
            "## Dropped reasons",
            "",
            "| Reason | Entries |",
            "| --- | ---: |",
        ]
        text += [f"| {k} | {v} |" for k, v in sorted(dropped.items())]
        text += ["", "## Case-only splits", ""] + [f"- {at}" for at in self.case_only]
        text += [
            "",
            "## Lock",
            "",
            f"Status: {lock['status']}. See report.json for located refusals and their source origins.",
            "",
            "| Lock error | Located refusals |",
            "| --- | ---: |",
            *[
                f"| {error} | {count} |"
                for error, count in sorted(lock["grouped_errors"].items())
            ],
            "",
            "## Statements A deferred",
            "",
            "The converter covers none of these A comparison repairs; they stay hand-written:",
            "",
            "- EKB_AR/EKB_MANAD coverage: source-bound corrections are absent from supported legacy fields; hand-written set coverage.",
            "- LOVA/DORS/LMED/LSS undeclared tables: hand-written assert table or set table; no guessed routing.",
            "- LMED ATC/VARUNR and LVM BESDATUM identifier flags: no explicit legacy errata.flags or errata.field repair targets these columns; reader flags are not inverted by inference.",
            "- BU personnummerkvalitet/vartbarn, LMED kon/transtyp, LVM avslut, SOL pomvtrygg code lists: hand-written assert/set after documentary endpoint review.",
            "- LVM besdatumn: the legacy BESDATUM/BESDATUMN identity.split entries at sos/lvm.toml:173 and :198 use non-type selectors and are dropped; a reviewed set variable or second ledger anchor stays hand-written.",
            "",
        ]
        (self.out / "report.md").write_text("\n".join(text) + summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frames", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--lock", action="store_true")
    parser.add_argument("--write-curation", action="store_true")
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--assets", type=Path)
    parser.add_argument("--previous-proposals", type=Path)
    args = parser.parse_args()
    if args.write_curation and args.previous_proposals:
        parser.error("--previous-proposals requires scratch curation output")
    args.out = args.out.resolve()
    if args.out.exists() or ROOT / "curation" in (args.out, *args.out.parents):
        parser.error("--out must be a new scratch directory outside curation/")
    if not list(args.frames.glob("*.jsonl")):
        parser.error("--frames contains no reg-build read JSONL")
    args.out.mkdir(parents=True)
    converter = Converter(args.frames, args.out)
    if args.previous_proposals:
        converter.cached_audit = audit_cached(converter, args.previous_proposals)
        previous = json.loads((args.previous_proposals / "report.json").read_text())
        converter.previous_cases = {
            r["source"]
            for r in previous["entries"]
            if r.get("reason")
            == "literal-only column selector cannot distinguish case-folded source rows"
        }
    for provider in ("scb", "fk", "sos"):
        for path in sorted(
            (ROOT / "reg_meta_build/curation/registers" / provider).glob("*.toml")
        ):
            if not path.name.endswith(".auto.toml"):
                converter.convert_file(path)
    for provider, register, path, line, statement, reason in classifications(
        ROOT, register_public=converter.public
    ):
        at = f"{path}:{line}"
        result = {
            "source": at,
            "category": "classification",
            "outcome": "converted" if statement else "dropped",
        }
        if statement:
            converter.add(at, provider, register, statement)
        else:
            result["reason"] = reason
        converter.results.append(result)
    converter.normalize_identity()
    normalize_sets(converter)
    collapse(converter)
    converter.write(args.write_curation)
    lock = {"status": "not run", "refusals": []}
    converter.report(lock)
    if args.previous_proposals:
        converter.current_audit = audit_cached(converter, args.out)
    if args.lock:
        if not args.inputs:
            parser.error("--lock requires --inputs")
        command = [
            str(ROOT / "target/release/reg-build"),
            "lock",
            "--inputs",
            str(args.inputs),
            "--curation",
            str(converter.curation),
        ]
        if args.assets:
            command += ["--assets", str(args.assets)]
        converter.records.clear()
        converter.rows.clear()
        converter.releases.clear()
        converter.value_sets.clear()
        converter.context_tables.clear()
        converter.context_registers.clear()
        gc.collect()
        started = time.monotonic()
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        (args.out / "lock.log").write_text(result.stdout + result.stderr)
        lock = {
            "status": "accepted" if result.returncode == 0 else "refused",
            "exit_code": result.returncode,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "refusals": result.stderr.splitlines(),
        }
    converter.report(lock)
    print(
        json.dumps(
            {
                "source_entries": len(converter.results),
                "statement_counts": converter.counts,
                "lock": lock["status"],
            },
            sort_keys=True,
        )
    )
    if lock["status"] == "refused":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
