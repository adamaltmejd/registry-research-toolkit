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


class UnexpressibleSelector(ValueError):
    """Source evidence exists but cannot be represented by address grammar."""


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
            subject = self.converter.selector(row)
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
