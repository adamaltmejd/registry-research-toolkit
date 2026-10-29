"""One-shot built-catalog lookup; deleted at milestone F (REBUILD.md).

The published resolver output is the identity authority. Source locators join reader
frames to catalog occurrences; this module never repeats resolver identity rules.
New assignments are certified by reg-build lock, not predicted here.
"""

import collections
import datetime as dt
import sqlite3


def iso_day(day):
    return (dt.date(1970, 1, 1) + dt.timedelta(days=day)).isoformat()


class CatalogIdentity:
    """Read current identities and placement from an immutable catalog."""

    def __init__(self, records, ledgers, catalog):
        self.ledgers = ledgers
        self.tables = {}
        self.releases = {}
        self.matches = {}
        self.provider_slugs = collections.defaultdict(set)
        for (provider, register), ledger in ledgers.items():
            for slug in ledger.get("variables", {}):
                self.provider_slugs[provider, slug].add(register)
        connection = sqlite3.connect(catalog.resolve().as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            table_rows = list(
                connection.execute("""
                SELECT t.id, t.pid, t.implicit, x.body AS name
                FROM tbl t JOIN text x ON x.id = t.name_text
            """)
            )
            table_ids = {r["pid"]: r["id"] for r in table_rows}
            names = collections.defaultdict(list)
            for row in table_rows:
                names[row["name"]].append(row["pid"])
            by_locator = collections.defaultdict(dict)
            for row in connection.execute("""
                SELECT ol.locator, o.id, o.source_key, o.status, o.start, o.end,
                       v.pid AS variable, t.pid AS table_pid
                FROM occurrence o JOIN occurrence_locator ol ON ol.occurrence_id = o.id
                JOIN tbl t ON t.id = o.table_id
                LEFT JOIN variable v ON v.id = o.variable_id
            """):
                by_locator[row["locator"], row["source_key"]][row["id"]] = dict(row)
            for row in records:
                if row["kind"] != "occurrence":
                    continue
                matches = {}
                for locator in [row["locator"], *row.get("also", [])]:
                    matches.update(
                        by_locator.get(
                            (f"{locator['input']}:{locator['at']}", row["source_key"]),
                            {},
                        )
                    )
                self.matches[id(row)] = tuple(matches.values())
                pids = {r["table_pid"] for r in matches.values()}
                if len(pids) == 1:
                    self._table(row, pids.pop())
            for row in records:
                if row["kind"] not in {"table", "occurrence"}:
                    continue
                provider, register = row["provider"], row["register"]
                key = provider, register, row["table"]
                if key in self.tables:
                    continue
                ledger = ledgers.get((provider, register), {})
                public = ledger.get("public", register)
                prefix = f"{provider}/{public}"
                slug = ledger.get("tables", {}).get(row["table"])
                pid = (
                    f"{prefix}/{slug}" if slug else prefix if not row["table"] else None
                )
                if pid in table_ids:
                    self._table(row, pid)
                    continue
                matches = [p for p in names[row["table"]] if p.startswith(prefix + "/")]
                if len(matches) == 1:
                    self._table(row, matches[0])
            # The LISA attachment reconstructs authored constructors whose locators
            # are deliberately absent from occurrence_locator. Match their exact
            # published coordinates; never synthesize their identity.
            authored = collections.defaultdict(list)
            for result in connection.execute("""
                SELECT o.id, o.source_key, o.status, o.start, o.end,
                       v.pid AS variable, t.pid AS table_pid, s.exact AS spelling,
                       r.source_key AS release
                FROM occurrence o JOIN tbl t ON t.id = o.table_id
                LEFT JOIN variable v ON v.id = o.variable_id
                LEFT JOIN spelling s ON s.id = o.spelling_id
                LEFT JOIN release r ON r.id = o.release_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM occurrence_locator ol WHERE ol.occurrence_id = o.id
                )
            """):
                authored[
                    result["table_pid"],
                    result["source_key"],
                    result["spelling"],
                    result["release"],
                ].append(dict(result))
            for row in records:
                if row["kind"] != "occurrence" or self.matches[id(row)]:
                    continue
                provider, register = row["provider"], row["register"]
                public = ledgers.get((provider, register), {}).get("public", register)
                slug = self.tables.get((provider, register, row["table"]))
                pid = f"{provider}/{public}" + (
                    f"/{slug}" if slug and slug != public else ""
                )
                candidates = authored.get(
                    (pid, row["source_key"], row["column"] or None, row.get("release")),
                    [],
                )
                self.matches[id(row)] = tuple(
                    r
                    for r in candidates
                    if (
                        r["start"] is None
                        or (
                            row.get("from") == iso_day(r["start"])
                            and row.get("to") == iso_day(r["end"] - 1)
                        )
                    )
                )
            handles = collections.defaultdict(list)
            for key, slug in self.tables.items():
                provider, register, _ = key
                public = ledgers.get((provider, register), {}).get("public", register)
                prefix = f"{provider}/{public}"
                pid = prefix if slug == public else f"{prefix}/{slug}"
                if pid in table_ids:
                    handles[table_ids[pid]].append(key)
            for row in connection.execute("""
                SELECT r.table_id, r.source_key, r.start, r.end, x.body AS name,
                       r.edition, r.qualifier
                FROM release r JOIN text x ON x.id = r.name_text
            """):
                for key in handles[row["table_id"]]:
                    self.releases[*key, row["source_key"]] = {
                        "kind": "release",
                        "provider": key[0],
                        "register": key[1],
                        "table": key[2],
                        "release": row["source_key"],
                        "name": row["name"],
                        "from": iso_day(row["start"])
                        if row["start"] is not None
                        else None,
                        "to": iso_day(row["end"] - 1)
                        if row["end"] is not None
                        else None,
                        "edition": row["edition"],
                        "qualifier": row["qualifier"],
                    }
        finally:
            connection.close()
        self.members = collections.defaultdict(list)
        for row in records:
            if row["kind"] == "occurrence":
                group = self.group(row)
                if group is not None:
                    self.members[group].append(row)

    def _table(self, row, pid):
        provider, register = row["provider"], row["register"]
        public = self.ledgers.get((provider, register), {}).get("public", register)
        prefix = f"{provider}/{public}"
        if pid == prefix:
            slug = public
        elif pid.startswith(prefix + "/"):
            slug = pid[len(prefix) + 1 :]
        else:
            # Cross-register routing needs reviewed curation, never a guessed handle.
            return
        self.tables[provider, register, row["table"]] = slug

    def group(self, row):
        pids = {r["variable"] for r in self.matches[id(row)]}
        return pids.pop() if len(pids) == 1 and None not in pids else None

    def group_members(self, row):
        return self.members.get(self.group(row), ())

    def default_slug(self, row):
        group = self.group(row)
        return group.rsplit("/", 1)[1] if group is not None else None

    def status(self, row):
        statuses = {r["status"] for r in self.matches[id(row)]}
        return (
            statuses.pop()
            if len(statuses) == 1
            else "multiple"
            if statuses
            else "absent"
        )

    def period(self, row):
        spans = {(r["start"], r["end"]) for r in self.matches[id(row)]}
        if len(spans) == 1:
            start, end = spans.pop()
            if start is not None:
                return iso_day(start), iso_day(end - 1)
        return None

    def target_status(self, provider, register, slug):
        if slug in self.ledgers.get((provider, register), {}).get("variables", {}):
            return "local"
        return (
            "only-other-register"
            if self.provider_slugs.get((provider, slug))
            else "absent-everywhere"
        )
