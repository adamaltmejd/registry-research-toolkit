"""One-shot case-selector audit; deleted at milestone F (REBUILD.md)."""

import calendar
import collections
import datetime as dt
import json
import re

ALIASES = {
    "name": "label",
    "data_type": "type",
    "is_sensitive": "sensitive",
    "is_identifier": "identifier",
    "measurement_unit": "unit",
    "column_name": "spelling",
}
SOURCE_FIELDS = {"type": "dtype", "spelling": "column"}


def period_intervals(scope):
    """Calendar scopes as inclusive ISO intervals; refuse unfamiliar period syntax."""

    def endpoint(text, end):
        if not text:
            return "9999-12-31" if end else "0001-01-01"
        if re.fullmatch(r"\d{4}", text):
            return text + ("-12-31" if end else "-01-01")
        if re.fullmatch(r"\d{4}-\d{2}", text):
            year, month = map(int, text.split("-"))
            return f"{text}-{calendar.monthrange(year, month)[1] if end else 1:02}"
        return dt.date.fromisoformat(text).isoformat()

    if not scope:
        return [("0001-01-01", "9999-12-31")]
    result = []
    for piece in scope.split(","):
        bounds = piece.split("..")
        if len(bounds) == 1:
            bounds *= 2
        if len(bounds) != 2:
            raise ValueError("unfamiliar period syntax")
        start, end = endpoint(bounds[0], False), endpoint(bounds[1], True)
        if start > end:
            raise ValueError("reversed period")
        result.append((start, end))
    return result


def parse_selector(subject):
    """The converter's literal occurrence addresses, never a partial guard parse."""
    match = re.fullmatch(r"([^:]+): (.*?)(?: @([^ ]+))?(?: where (.*))?", subject)
    if not match:
        return None
    table, column, scope, source_guards = match.groups()
    if not column or any(c.isspace() for c in column) or "@" in column:
        return None
    guards = {}
    for guard in source_guards.split(" and ") if source_guards else []:
        match = re.fullmatch(r"(key|release|type|label|unit) = '([^']*)'", guard)
        if not match or match[1] in guards:
            return None
        guards[match[1]] = match[2]
    try:
        return table, column.lower(), period_intervals(scope), guards
    except ValueError:
        return None


def value_key(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def outcomes(kind, entry, pins):
    """Only explicit legacy outcomes; missing claims retain their source value."""
    if kind == "identity.column_owner":
        return {"variable": pins.get(entry["owner"], entry["owner"])}
    if kind == "representation.parallel":
        return {
            "sets": [
                "legacy-"
                + pins.get(entry["variable"], entry["variable"].replace(".", "-"))
            ]
        }
    if kind == "identity.unassigned":
        return {"variable": "none"}
    if kind == "errata.field" and not entry.get("clear"):
        return {ALIASES.get(entry["field"], entry["field"]): entry["value"]}
    if kind == "errata.flags":
        return {
            ALIASES[key]: entry[key]
            for key in ("is_sensitive", "is_identifier")
            if key in entry
        }
    if kind == "coding.uncoded":
        return {"coded": False}
    if kind == "coding.documented":
        items = entry.get("members") or entry.get("list_members")
        if items:
            return {"legacy_code_list": sorted((str(c), str(v)) for c, v in items)}
    return {}


class CaseSafety:
    def __init__(self, converter):
        self.converter = converter
        self.claims = collections.defaultdict(lambda: collections.defaultdict(set))
        self.counts = collections.Counter()
        self.conflicts = []

    @staticmethod
    def identity(row):
        # A selector folds only the literal spelling; other source coordinates stay.
        return (
            row["provider"],
            row["register"],
            row["table"],
            row["source_key"],
            row.get("release"),
            row["column"],
            row.get("from"),
            row.get("to"),
            row.get("dtype"),
        )

    def prepare(self, provider, register, declarations, pins, variants):
        self.pins = pins
        for kind, _line, entry in declarations:
            if kind == "representation.parallel":
                if entry.get("case_aliases"):
                    continue
                for column in entry["columns"]:
                    endpoint = {k: entry[k] for k in ("variable", "variant")}
                    endpoint.update(
                        {
                            k: column[k]
                            for k in (
                                "column",
                                "source_editions",
                                "valid_from",
                                "valid_to",
                            )
                        }
                    )
                    try:
                        rows = self.converter.match(
                            provider,
                            register,
                            endpoint,
                            pins,
                            variants,
                            check_case=False,
                        )
                    except ValueError:
                        continue
                    for row in rows:
                        self.claims[self.identity(row)]["sets"].add(
                            value_key(outcomes(kind, entry, pins)["sets"])
                        )
                continue
            if kind == "identity.partition":
                for column, owner in entry.get("columns", {}).items():
                    selected = entry | {"column": column}
                    try:
                        rows = self.converter.match(
                            provider,
                            register,
                            selected,
                            pins,
                            variants,
                            check_case=False,
                        )
                    except ValueError:
                        continue
                    for row in rows:
                        self.claims[self.identity(row)]["variable"].add(
                            value_key(pins.get(owner, owner))
                        )
                continue
            fields = outcomes(kind, entry, pins)
            if not fields:
                continue
            try:
                rows = self.converter.match(
                    provider,
                    register,
                    entry,
                    pins,
                    variants,
                    check_case=False,
                )
            except ValueError:
                continue
            for row in rows:
                for field, value in fields.items():
                    self.claims[self.identity(row)][field].add(value_key(value))

    def default(self, row, field):
        if field == "variable":
            return self.converter.identity.default_slug(row)
        return row.get(SOURCE_FIELDS.get(field, field))

    def authorize(self, provider, register, kind, entry, rows):
        if not entry.get("column"):
            return rows
        selected = {self.identity(row) for row in rows}
        subjects = {self.converter.selector(row) for row in rows}
        coordinates = {(row["table"], row["source_key"]) for row in rows}
        variants = [
            row
            for row in self.converter.rows[provider, register]
            if self.identity(row) not in selected
            and row["column"] != entry["column"]
            and row["column"].lower() == entry["column"].lower()
            and (row["table"], row["source_key"]) in coordinates
        ]
        siblings = []
        for row in variants:
            try:
                subject = self.converter.selector(row)
            except ValueError:
                continue
            if subject in subjects:
                siblings.append(row)
        if not siblings:
            if variants:
                self.counts["distinct_source_coordinates"] += 1
            return rows
        wanted = {
            field: {value_key(value)}
            for field, value in outcomes(kind, entry, self.pins).items()
        }
        safe = bool(wanted)
        for row in siblings:
            claims = self.claims[self.identity(row)]
            for field, values in wanted.items():
                actual = claims.get(field)
                if not actual:
                    default = self.default(row, field)
                    actual = {value_key(default)} if default is not None else set()
                safe &= actual == values
        if not safe:
            self.counts["different_or_unproven_outcomes"] += 1
            self.conflicts.append(
                {
                    "source": getattr(self.converter, "current_source", None),
                    "provider": provider,
                    "register": register,
                    "variable": entry.get("variable"),
                    "columns": sorted({row["column"] for row in rows + siblings}),
                }
            )
            raise ValueError(
                "case-folded variants have different or unproven legacy outcomes"
            )
        self.counts["same_outcome_folded_selector"] += 1
        return rows + siblings


def audit_cached(converter, root):
    """Static reclassification of cached proposals, never a substitute for lock."""
    import tomllib

    counts = collections.Counter()
    cause_counts = collections.Counter()
    overlaps = []
    unplaced = []
    missing = []
    cached_report = json.loads((root / "report.json").read_text())
    origins = {item["location"]: item["sources"] for item in cached_report["generated"]}
    category = {item["source"]: item["category"] for item in cached_report["entries"]}
    for path in sorted(root.glob("curation/registers/*/*/curation.toml")):
        provider, register = path.parts[-3:-1]
        index = collections.defaultdict(list)
        for row in converter.rows[provider, register]:
            index[
                converter.tables[provider, register, row["table"]],
                row["column"].lower() or "<none>",
            ].append(row)
        parsed = tomllib.loads(path.read_text()).get("statement", [])
        lines = [
            number
            for number, line in enumerate(path.read_text().splitlines(), 1)
            if line == "[[statement]]"
        ]
        candidates = []
        for statement, line in zip(parsed, lines, strict=True):
            address = parse_selector(statement["subject"])
            if address is None or statement["kind"] not in {"set", "assert"}:
                continue
            table, column, scope, guards = address
            location = f"{path.relative_to(root)}:{line}"
            sources = origins.get(location, [])
            rows = [
                row
                for row in index[table, column]
                if ("key" not in guards or guards["key"] == row["source_key"])
                and ("release" not in guards or guards["release"] == row.get("release"))
                and ("type" not in guards or guards["type"] == row.get("dtype"))
            ]
            fields = statement.get(
                "fields", statement.get("fact", {}).get("fields", {})
            )
            target = fields.get("variable")
            reason = None
            if target is not None:
                if any(not row["column"] for row in rows):
                    reason = "gap marker"
                elif target != "none" and any(
                    converter.identity.status(row) == "unplaced" for row in rows
                ):
                    reason = "unplaced occurrence"
                elif target != "none" and target not in converter.ledgers.get(
                    (provider, register), {}
                ).get("variables", {}):
                    reason = "missing local ledger"
            if reason:
                counts[reason] += 1
                item = {"location": location, "sources": sources, "target": target}
                if reason == "missing local ledger":
                    elsewhere = any(
                        target in ledger.get("variables", {})
                        for owner, ledger in converter.ledgers.items()
                        if owner != (provider, register)
                    )
                    item["cause"] = (
                        "slug exists only in another register"
                        if elsewhere
                        else "no ledger id anywhere"
                    )
                    cause_counts[item["cause"]] += 1
                    missing.append(item)
                elif reason == "unplaced occurrence":
                    item["categories"] = sorted(
                        {category[source] for source in sources}
                    )
                    unplaced.append(item)
                continue
            selected = set()
            for row in rows:
                period = converter.identity.period(row)
                if not period:
                    continue
                for start, end in scope:
                    start, end = max(start, period[0]), min(end, period[1])
                    if start <= end:
                        identity = (
                            row["provider"],
                            row["register"],
                            row["table"],
                            row.get("release"),
                            row["column"],
                            row["source_key"],
                            converter.span(row),
                        )
                        for locator in [row["locator"], *row.get("also", [])]:
                            selected.add(((identity, value_key(locator)), start, end))
            candidates.append((statement, address, selected, location, sources))
        by_row_field = collections.defaultdict(list)
        pairs = set()
        assertions = []
        for i, (statement, address, selected, _location, _sources) in enumerate(
            candidates
        ):
            fields = statement.get(
                "fields", statement.get("fact", {}).get("fields", {})
            )
            for identity, start, end in selected:
                for field in fields:
                    for j, other_start, other_end in by_row_field[identity, field]:
                        if max(start, other_start) <= min(end, other_end):
                            pairs.add((j, i))
                    by_row_field[identity, field].append((i, start, end))
            if statement["kind"] == "assert":
                table, column, scope, _guards = address
                for j in assertions:
                    other = candidates[j][1]
                    if (table, column) == other[:2] and any(
                        max(a, c) <= min(b, d) for a, b in scope for c, d in other[2]
                    ):
                        pairs.add((j, i))
                assertions.append(i)
        for a, b in sorted(pairs):
            left, right = candidates[a], candidates[b]
            left_fields = left[0].get(
                "fields", left[0].get("fact", {}).get("fields", {})
            )
            right_fields = right[0].get(
                "fields", right[0].get("fact", {}).get("fields", {})
            )
            shared = left_fields.keys() & right_fields.keys()
            same = all(left_fields[field] == right_fields[field] for field in shared)
            overlaps.append(
                {
                    "locations": [left[3], right[3]],
                    "sources": sorted(set(left[4] + right[4])),
                    "fields": sorted(shared),
                    "cause": "converter duplicate effect"
                    if same
                    else "different legacy outcomes",
                }
            )
    counts["overlap pairs"] = len(overlaps)
    counts["overlap directed diagnostics"] = 2 * len(overlaps)
    return {
        "method": "static cached-proposal audit; no real-frame lock run",
        "counts": dict(counts),
        "missing_ledger_causes": dict(cause_counts),
        "missing_ledger": missing,
        "unplaced": unplaced,
        "overlaps": overlaps,
    }
