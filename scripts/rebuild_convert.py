#!/usr/bin/env python3
"""One-shot curation converter/report; deleted at milestone F (REBUILD.md).

Extends the raw-TOML partition/owner census in scratchpad/fable/partitions.py.
The exact historical dry-run program is lost; its expected table is in REBUILD.md.
Reads only reg-build read JSONL, never the legacy builder's IR. Generated statements
are proposals: lock reviews today's premises, not the old builder's fingerprints.

Run reg-build read first, then:
  uv run --no-project scripts/rebuild_convert.py --frames DIR --out SCRATCH
  ... --lock --inputs DIR --assets DIR
The output root must not exist. No generated curation is committed.
"""

import argparse
import collections
import gc
import json
import re
import shutil
import subprocess
import tomllib
from pathlib import Path

from rebuild_convert_classifications import convert as classifications

ROOT = Path(__file__).resolve().parents[1]
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
            if start or end:
                scope = f" @{start}..{end}"
        return f"{table}: {spelling}{scope} where " + " and ".join(guards)

    def add(self, at, provider, register, statement):
        key = json.dumps(statement, sort_keys=True, ensure_ascii=False)
        self.statements[provider, register].setdefault(
            key, {"statement": statement, "origins": []}
        )["origins"].append(at)

    def compact(self, proposals, provider, register):
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
        return result

    def match(self, provider, register, entry, pins, variants):
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
            slug = variants.get(variant, variant)
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
        if not rows:
            raise ValueError("no exact new-frame occurrence endpoint")
        if column and any(
            other["column"] != column
            and fold(other["column"]) == fold(column)
            and any(
                (other["table"], other["source_key"])
                == (row["table"], row["source_key"])
                for row in rows
            )
            for other in self.rows[provider, register]
        ):
            raise ValueError(
                "literal-only column selector cannot distinguish case-folded source rows"
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
        for kind, line, entry in entries(path):
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
                    if outcome == "No statement":
                        raise ValueError(
                            "dry-run projection agrees; resolver identity not certified"
                        )
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
                    raise ValueError(
                        f"{kind}: requires reviewed translation of legacy payload/guards"
                    )
                if not proposals:
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

    def write(self):
        shutil.copytree(ROOT / "curation", self.out / "curation")
        for path in (self.out / "curation").rglob("curation.lock"):
            path.unlink()
        for path in (self.out / "curation").rglob("curation.toml"):
            path.unlink()
        self.counts = collections.Counter()
        self.locations = {}
        for (provider, register), records in sorted(self.statements.items()):
            path = (
                self.out / "curation/registers" / provider / register / "curation.toml"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            text = "# One-shot conversion proposals; deleted at milestone F (REBUILD.md).\n"
            next_line = 2
            for key, record in sorted(records.items()):
                statement = record["statement"]
                text += "\n[[statement]]\n"
                line = next_line + 1
                self.locations[str(path.resolve()), line] = record
                record["location"] = f"{path.relative_to(self.out)}:{line}"
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
        for diagnostic in lock["refusals"]:
            match = re.match(
                r"(?:reg-build: )?(.*curation\.toml):(\d+): (.*)", diagnostic
            )
            if match:
                path, line, error = match.groups()
                record = self.locations.get((str(Path(path).resolve()), int(line)), {})
                refused.update(record.get("origins", []))
                category = re.sub(
                    r"overlapping overrides with .*", "overlapping overrides", error
                )
                errors[category] += 1
                located.append(
                    {
                        "location": record.get("location", f"{path}:{line}"),
                        "error": error,
                        "sources": record.get("origins", []),
                    }
                )
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
        dropped = collections.Counter(
            r["reason"] for r in self.results if r["outcome"] == "dropped"
        )
        outcomes = collections.Counter(r["outcome"] for r in self.results)
        data = {
            "source_entries": len(self.results),
            "outcomes": dict(outcomes),
            "statement_counts": dict(sorted(self.counts.items())),
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
        }
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
            "The proposal field counts cover translated statements only. They are not a complete D-3b scope while expressible legacy categories remain deferred below.",
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
        (self.out / "report.md").write_text("\n".join(text))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frames", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--lock", action="store_true")
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--assets", type=Path)
    args = parser.parse_args()
    args.out = args.out.resolve()
    if args.out.exists() or ROOT / "curation" in (args.out, *args.out.parents):
        parser.error("--out must be a new scratch directory outside curation/")
    if not list(args.frames.glob("*.jsonl")):
        parser.error("--frames contains no reg-build read JSONL")
    args.out.mkdir(parents=True)
    converter = Converter(args.frames, args.out)
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
    converter.write()
    lock = {"status": "not run", "refusals": []}
    converter.report(lock)
    if args.lock:
        if not args.inputs:
            parser.error("--lock requires --inputs")
        command = [
            str(ROOT / "target/release/reg-build"),
            "lock",
            "--inputs",
            str(args.inputs),
            "--curation",
            str(args.out / "curation"),
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
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        (args.out / "lock.log").write_text(result.stdout + result.stderr)
        lock = {
            "status": "accepted" if result.returncode == 0 else "refused",
            "exit_code": result.returncode,
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


if __name__ == "__main__":
    main()
