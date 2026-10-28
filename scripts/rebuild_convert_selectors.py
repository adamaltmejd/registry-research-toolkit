"""One-shot exact occurrence selector collapse; deleted at milestone F (REBUILD.md)."""

import collections
import datetime as dt
import json
import re

from rebuild_convert_cases import parse_selector, period_intervals, value_key

OPEN = ("0001-01-01", "9999-12-31")
GUARD_FIELDS = {"key": "source_key", "type": "dtype"}


def merge_periods(periods):
    result = []
    for start, end in sorted(periods):
        if result and (
            dt.date.fromisoformat(start).toordinal()
            <= dt.date.fromisoformat(result[-1][1]).toordinal() + 1
        ):
            result[-1] = result[-1][0], max(end, result[-1][1])
        else:
            result.append((start, end))
    return result


def render(table, spelling, scope, guards):
    def interval(start, end):
        if start.endswith("-01-01") and end.endswith("-12-31"):
            start, end = start[:4], end[:4]
        start = "" if start in {OPEN[0], "0001"} else start
        end = "" if end in {OPEN[1], "9999"} else end
        return start if start == end else f"{start}..{end}"

    subject = f"{table}: {spelling}"
    if scope != [OPEN]:
        subject += " @" + ",".join(interval(*span) for span in scope)
    if guards:
        subject += " where " + " and ".join(
            f"{field} = '{value}'" for field, value in guards.items()
        )
    return subject


class Snapshot:
    """The source snapshot after constructors, before scalar/identity setters."""

    def __init__(self, converter, owner, statements):
        self.rows = []
        self.index = collections.defaultdict(list)
        self.problem = None
        self.period_coordinates = set()
        register_bounds = converter.context_registers.get(owner, {})
        defaults = {}
        for (pid, rid, handle), source in converter.context_tables.items():
            if (pid, rid) == owner:
                defaults[converter.tables[pid, rid, handle]] = (
                    source.get("from") or register_bounds.get("from") or OPEN[0],
                    source.get("to") or register_bounds.get("to") or OPEN[1],
                )
        releases = {
            (converter.tables[provider, register, table], release): (
                (row["from"], row["to"]) if row.get("from") and row.get("to") else None
            )
            for (provider, register, table, release), row in converter.releases.items()
            if (provider, register) == owner
            and (provider, register, table) in converter.tables
        }
        constructors = [
            record["statement"]
            for record in statements.values()
            if record["statement"]["kind"] == "assert"
        ]
        for statement in constructors:
            if statement["fact"]["entity"] == "table":
                self.problem = "asserted tables require their own coverage snapshot"
            if statement["fact"]["entity"] != "release":
                continue
            match = re.fullmatch(r"release:([^/]+)/(.+)", statement["subject"])
            if not match:
                self.problem = "unfamiliar asserted release address"
                continue
            periods = period_intervals(statement["fact"]["period"])
            if len(periods) != 1:
                self.problem = "noncontiguous asserted release"
                continue
            releases[match[1], match[2]] = periods[0]
        for row in converter.rows[owner]:
            table = converter.tables[*owner, row["table"]]
            period = (
                releases.get((table, row["release"]))
                if row.get("release")
                else converter.span(row)
            )
            self.add(table, row, period)
        for number, statement in enumerate(constructors):
            fact = statement["fact"]
            if fact["entity"] != "occurrence":
                continue
            address = parse_selector(statement["subject"])
            if address is None:
                self.problem = "unfamiliar asserted occurrence address"
                continue
            table, spelling, scope, guards = address
            row = {GUARD_FIELDS.get(k, k): v for k, v in guards.items()}
            row.setdefault("source_key", f"stmt:assertion-{number}")
            row["column"] = "" if spelling == "<none>" else spelling
            for field, value in fact["fields"].items():
                if field != "period":
                    row[{"spelling": "column", "type": "dtype"}.get(field, field)] = (
                        value
                    )
            if row.get("release"):
                periods = [releases.get((table, row["release"]))]
            elif fact["fields"].get("period"):
                periods = period_intervals(fact["fields"]["period"])
            else:
                periods = (
                    scope
                    if " @" in statement["subject"]
                    else [
                        defaults.get(
                            table,
                            (
                                register_bounds.get("from") or OPEN[0],
                                register_bounds.get("to") or OPEN[1],
                            ),
                        )
                    ]
                )
            for period in periods:
                self.add(table, row, period)
        for record in statements.values():
            statement = record["statement"]
            fields = statement.get("fields", {})
            if "coverage" in fields:
                self.problem = (
                    "coverage changes require their own source phase snapshot"
                )
            if "period" in fields:
                address = parse_selector(statement["subject"])
                if address is None or any(c in address[1] for c in "{},"):
                    self.problem = "nonliteral period correction address"
                else:
                    self.period_coordinates.add(address[:2])
        if any(
            "table" in r["statement"].get("fields", {}) for r in statements.values()
        ):
            self.problem = "table moves require their own source phase snapshot"

    def add(self, table, row, period):
        self.index[table, row["column"].lower() or "<none>"].append(len(self.rows))
        self.rows.append((row, period))

    def select(self, address):
        table, spelling, scope, guards = address
        selected = set()
        whole = True
        for number in self.index[table, spelling]:
            row, period = self.rows[number]
            if any(row.get(GUARD_FIELDS.get(k, k)) != v for k, v in guards.items()):
                continue
            if scope == [OPEN]:
                selected.add(number)
                continue
            if period is None:
                continue
            overlap = merge_periods(
                (max(a, period[0]), min(b, period[1]))
                for a, b in scope
                if max(a, period[0]) <= min(b, period[1])
            )
            if overlap:
                selected.add(number)
                whole &= overlap == [period]
        return selected, whole

    def ambiguous(self, selected):
        # simplify: pairwise within one spelling; index if a selector spans thousands of rows.
        rows = [self.rows[number] for number in sorted(selected)]
        for i, (left, a) in enumerate(rows):
            for right, b in rows[i + 1 :]:
                a, b = a or OPEN, b or OPEN
                if max(a[0], b[0]) <= min(a[1], b[1]) and (
                    left.get("source_key") != right.get("source_key")
                    or left.get("label") != right.get("label")
                ):
                    return True
        return False

    def candidate(self, address, wanted):
        table, spelling, _scope, source_guards = address
        guards = {k: v for k, v in source_guards.items() if k != "release"}
        scopes = [[OPEN]]
        periods = [self.rows[n][1] for n in wanted]
        dated = all(period is not None for period in periods)
        supported = dated and all(
            value in OPEN or 1900 <= int(value[:4]) <= 2099
            for period in periods
            for value in period
        )
        if supported:
            scopes.append(merge_periods(periods))
        if self.ambiguous(wanted):
            return None, "overlapping editions disagree in source key or label"
        reason = (
            "undated editions cannot be selected by a calendar period"
            if not dated
            else "edition dates lie outside the calendar selector grammar"
        )
        for scope in scopes:
            candidate = table, spelling, scope, guards
            selected, whole = self.select(candidate)
            if selected == wanted and whole:
                return candidate, None
            if scope != [OPEN]:
                reason = (
                    "period would cut a source occurrence"
                    if selected == wanted and not whole
                    else "other editions overlap the selected calendar periods"
                )
        return None, reason


def collapse(converter, *, context=None):
    counts = collections.Counter()
    remaining = collections.Counter()
    for owner, statements in converter.statements.items():
        counts["statements_before"] += len(statements)
        counts["release_guards_before"] += sum(
            "release = '" in r["statement"]["subject"] for r in statements.values()
        )
        # Existing constructors affect shorter selectors. On converter replay an
        # emitted assertion also appears among the new proposals; its evidence
        # prefix differs, so identify that duplicate by the asserted fact shape.
        snapshot_records = {}
        for record in [*(context or {}).get(owner, {}).values(), *statements.values()]:
            shape = {
                k: v
                for k, v in record["statement"].items()
                if k not in {"evidence", "caveat"}
            }
            snapshot_records[value_key(shape)] = record
        snapshot = Snapshot(converter, owner, snapshot_records)
        groups = collections.defaultdict(list)
        retained = []
        for record in statements.values():
            statement = record["statement"]
            address = parse_selector(statement["subject"])
            if (
                statement["kind"] != "set"
                or address is None
                or snapshot.problem
                or address[:2] in snapshot.period_coordinates
            ):
                retained.append(record)
                if "release = '" in statement["subject"]:
                    record["selector_reason"] = (
                        "assertion constructor"
                        if statement["kind"] == "assert"
                        else snapshot.problem
                        or (
                            "period correction coordinate"
                            if address and address[:2] in snapshot.period_coordinates
                            else "unfamiliar selector syntax"
                        )
                    )
                continue
            table, spelling, _scope, guards = address
            payload = {k: v for k, v in statement.items() if k != "subject"}
            key = (
                tuple(sorted(set(record["origins"]))),
                value_key(payload),
                table,
                spelling,
                tuple((k, v) for k, v in guards.items() if k != "release"),
            )
            groups[key].append((record, address))
        for group in groups.values():
            wanted = set()
            for _record, address in group:
                members, whole = snapshot.select(address)
                if not whole:
                    raise ValueError("original converter selector cuts an occurrence")
                wanted.update(members)
            candidate, reason = (
                snapshot.candidate(group[0][1], wanted)
                if wanted
                else (None, "no source members")
            )
            batches = []
            if candidate:
                batches.append((group, candidate, wanted, None))
            else:
                expressible = []
                expressible_members = set()
                for item in group:
                    members = snapshot.select(item[1])[0]
                    individual, why = (
                        snapshot.candidate(item[1], members)
                        if members
                        else (None, reason)
                    )
                    if individual:
                        expressible.append(item)
                        expressible_members.update(members)
                    else:
                        batches.append(([item], None, members, why))
                if expressible:
                    combined, why = snapshot.candidate(
                        expressible[0][1], expressible_members
                    )
                    if combined is None:
                        raise ValueError(
                            "individually exact period selectors did not combine"
                        )
                    batches.append((expressible, combined, expressible_members, why))
            for batch, candidate, members, reason in batches:
                record, address = batch[0]
                selected = candidate or address
                if "key" in selected[3]:
                    without_key = (
                        *selected[:3],
                        {k: v for k, v in selected[3].items() if k != "key"},
                    )
                    actual, whole = snapshot.select(without_key)
                    if actual == members and whole and not snapshot.ambiguous(members):
                        selected = without_key
                        counts["key_guards_removed"] += 1
                statement = record["statement"] | {
                    "subject": record["statement"]["subject"]
                    if selected == address
                    else render(*selected)
                }
                record = record | {"statement": statement}
                retained.append(record)
                if "release" in selected[3]:
                    record["selector_reason"] = (
                        reason or "release guard required for exact membership"
                    )
        rewritten = {}
        for record in retained:
            key = json.dumps(record["statement"], sort_keys=True, ensure_ascii=False)
            if key in rewritten:
                rewritten[key]["origins"] = sorted(
                    set(rewritten[key]["origins"] + record["origins"])
                )
            else:
                rewritten[key] = record
        converter.statements[owner] = rewritten
        counts["statements_after"] += len(rewritten)
        counts["release_guards_after"] += sum(
            "release = '" in r["statement"]["subject"] for r in rewritten.values()
        )
        remaining.update(
            record["selector_reason"]
            for record in rewritten.values()
            if "release = '" in record["statement"]["subject"]
        )
    converter.selector_collapse = dict(counts) | {
        "remaining_release_reasons": dict(sorted(remaining.items()))
    }
    return converter.selector_collapse


def bundle_classifications(converter):
    """Use literal spelling unions only when every other selector/payload fact agrees.

    Run last: Snapshot models one spelling at a time. A literal union with one
    shared period and guards is exactly the union of the individually proved rows.
    """
    before = after = lists = 0
    for owner, statements in converter.statements.items():
        groups = collections.defaultdict(list)
        retained = []
        before += len(statements)
        for record in statements.values():
            statement = record["statement"]
            address = parse_selector(statement["subject"])
            if (
                statement["kind"] != "set"
                or set(statement.get("fields", {})) != {"classification"}
                or address is None
                or not re.fullmatch(r"[\w.-]+", address[1])
            ):
                retained.append(record)
                continue
            table, spelling, scope, guards = address
            payload = {k: v for k, v in statement.items() if k != "subject"}
            key = (
                table,
                tuple(scope),
                tuple(sorted(guards.items())),
                tuple(sorted(set(record["origins"]))),
                value_key(payload),
            )
            groups[key].append((record, spelling, address))
        for group in groups.values():
            record, _, address = group[0]
            spellings = sorted({spelling for _, spelling, _ in group})
            if len(spellings) > 1:
                table, _, scope, guards = address
                statement = record["statement"] | {
                    "subject": render(
                        table, "{" + ",".join(spellings) + "}", scope, guards
                    )
                }
                record = record | {"statement": statement}
                lists += 1
            retained.append(record)
        converter.statements[owner] = {
            json.dumps(record["statement"], sort_keys=True, ensure_ascii=False): record
            for record in retained
        }
        after += len(converter.statements[owner])
    converter.selector_collapse.update(
        bundle_before=before, bundle_after=after, literal_spelling_lists=lists
    )
