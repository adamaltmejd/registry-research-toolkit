"""One-shot default-identity projection; deleted at milestone F (REBUILD.md).

Mirrors reg-resolve's period, edition, identity and ledger steps for report-only
noise removal. It does not apply statements or certify resolver equivalence.
"""

import collections
import datetime as dt

OPEN = ("0001-01-01", "9999-12-31")


def _anchor(text):
    day = key = None
    if " @ " in text:
        text, day = text.rsplit(" @ ", 1)
        day = day.strip()
        dt.date.fromisoformat(day)
    if " # " in text:
        text, key = text.rsplit(" # ", 1)
        key = key.strip()
    table, spelling = text.rsplit(": ", 1) if ": " in text else ("", text)
    return table.strip(), spelling.strip().lower(), key, day


class IdentityProjection:
    """Project only pinned default slugs, preserving unminted identities as None."""

    def __init__(self, records, ledgers):
        self.ledgers = ledgers
        self.slugs = {}
        self.statuses = {}
        self.periods = {}
        self.groups = {}
        self.members = {}
        self.provider_slugs = collections.defaultdict(set)
        for (provider, register), ledger in ledgers.items():
            for slug in ledger.get("variables", {}):
                self.provider_slugs[provider, slug].add(register)
        registers = collections.defaultdict(list)
        for row in records:
            if row["kind"] in {"register", "table", "release", "occurrence"}:
                registers[row["provider"], row["register"]].append(row)
        for at, rows in registers.items():
            self._project(rows, ledgers.get(at, {}))

    def default_slug(self, row):
        return self.slugs.get(id(row))

    def status(self, row):
        return self.statuses[id(row)]

    def period(self, row):
        return self.periods.get(id(row))

    def group(self, row):
        return self.groups.get(id(row))

    def group_members(self, row):
        """All placed rows of this default identity, including shadowed rows."""
        group = self.group(row)
        return self.members.get((row["provider"], row["register"], group), ())

    def target_status(self, provider, register, slug):
        if slug in self.ledgers.get((provider, register), {}).get("variables", {}):
            return "local"
        if self.provider_slugs.get((provider, slug)):
            return "only-other-register"
        return "absent-everywhere"

    def _project(self, records, ledger):
        register = next((r for r in records if r["kind"] == "register"), {})
        tables = {r["table"]: r for r in records if r["kind"] == "table"}
        releases = {
            (r["table"], r["release"]): r for r in records if r["kind"] == "release"
        }
        editions = collections.defaultdict(list)
        rank = {"final": 0, "unstated": 1, "preliminary": 2}
        primary = {}
        for at, release in releases.items():
            if release.get("from") and release.get("to"):
                editions[
                    at[0], release["from"], release["to"], release.get("qualifier")
                ].append(at)
        for members in editions.values():
            best = min(
                members,
                key=lambda at: (rank.get(releases[at].get("edition"), 2), at[1]),
            )
            for at in members:
                primary[at] = best

        rows = [r for r in records if r["kind"] == "occurrence"]
        for row in rows:
            rid = id(row)
            table = tables.get(row["table"], {})
            release_at = row["table"], row.get("release")
            if row.get("release"):
                if release_at not in releases:
                    raise ValueError(
                        "identity projection: occurrence release is missing"
                    )
                if release_at not in primary:
                    self.statuses[rid] = "unplaced"
                    continue
                source = releases[release_at]
            else:
                source = row
            stated = source.get("from") or OPEN[0], source.get("to") or OPEN[1]
            if stated[0] > stated[1]:
                raise ValueError("identity projection: reversed occurrence period")
            limit = (
                table.get("from") or register.get("from") or OPEN[0],
                table.get("to") or register.get("to") or OPEN[1],
            )
            clipped = max(stated[0], limit[0]), min(stated[1], limit[1])
            status = "used" if clipped[0] <= clipped[1] else "rejected"
            self.periods[rid] = clipped if status == "used" else stated
            self.statuses[rid] = "gap_marker" if not row["column"] else status

        primary_keys = {
            (r["table"], r["release"], r["source_key"])
            for r in rows
            if self.status(r) == "used" and r.get("release")
        }
        for row in rows:
            at = row["table"], row.get("release")
            best = primary.get(at)
            if (
                self.status(row) == "used"
                and best is not None
                and best != at
                and (*best, row["source_key"]) in primary_keys
            ):
                self.statuses[id(row)] = "shadowed"

        by_key = collections.defaultdict(lambda: collections.defaultdict(list))
        for row in rows:
            if self.status(row) == "used":
                by_key[row["source_key"]][row["column"].lower()].append(
                    self.period(row)
                )
        split = set()
        for key, folds in by_key.items():
            events = sorted(
                (lo, hi, fold) for fold, spans in folds.items() for lo, hi in spans
            )
            ends = {}
            for lo, hi, fold in events:
                if any(other != fold and end >= lo for other, end in ends.items()):
                    split.add(key)
                    break
                ends[fold] = max(hi, ends.get(fold, hi))

        members = collections.defaultdict(list)
        for row in rows:
            if self.status(row) in {"used", "shadowed", "rejected"}:
                key = row["source_key"]
                group = key, row["column"].lower() if key in split else ""
                members[group].append(row)
        members = {
            group: own
            for group, own in members.items()
            if any(self.status(r) == "used" for r in own)
        }
        anchors = collections.defaultdict(list)
        for group, own in members.items():
            at = own[0]["provider"], own[0]["register"], group
            self.members[at] = tuple(own)
            for row in own:
                self.groups[id(row)] = group
                anchors[row["table"], row["column"].lower()].append((group, row))
        pinned = {}
        for slug, texts in sorted(ledger.get("variables", {}).items()):
            targets = set()
            for text in texts:
                table, spelling, key, day = _anchor(text)
                for group, row in anchors.get((table, spelling), []):
                    lo, hi = self.period(row)
                    if (key is None or key == row["source_key"]) and (
                        day is None or lo <= day <= hi
                    ):
                        targets.add(group)
            target = next((g for g in sorted(targets) if g not in pinned), None)
            if target is not None:
                pinned[target] = slug
        for row in rows:
            group = self.groups.get(id(row))
            if group is not None:
                self.slugs[id(row)] = pinned.get(group)
            elif self.status(row) in {"used", "shadowed", "rejected"}:
                self.statuses[id(row)] = "withheld"
