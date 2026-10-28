"""One-shot variable grouping normalization; deleted at milestone F (REBUILD.md)."""

import collections
import json
import re


def normalize_sets(converter):
    """Emit one positive membership list per known final local variable.

    The resolver expands occurrence selectors for `sets` to complete variables. Keep
    that expansion explicit rather than emitting overlapping per-release statements.
    Unminted identities stay deferred; this projection is not permission to mint ids.
    """
    counts = collections.Counter()
    dropped = []
    for owner, statements in converter.statements.items():
        grouping = {
            key: record
            for key, record in statements.items()
            if record["statement"]["kind"] == "set"
            and set(record["statement"].get("fields", {})) == {"sets"}
        }
        counts["before"] += len(grouping)
        lookup = collections.defaultdict(list)
        for row in converter.rows[owner]:
            try:
                subject = converter.selector(row)
            except ValueError:
                continue
            lookup[subject].append(row)
            base = re.sub(r" and release = '[^']*'$", "", subject)
            if base != subject:
                lookup[base].append(row)

        def selected(subject, lookup=lookup):
            match = re.fullmatch(r"(.*) and type = '([^']*)'", subject)
            rows = lookup.get(match[1] if match else subject, [])
            if match:
                rows = [row for row in rows if row.get("dtype") == match[2]]
            return rows

        assignments = collections.defaultdict(set)
        for record in statements.values():
            statement = record["statement"]
            target = statement.get("fields", {}).get("variable")
            if target is not None:
                for row in selected(statement["subject"]):
                    assignments[id(row)].add(target)
        ledger = converter.ledgers.get(owner, {}).get("variables", {})
        folds = collections.defaultdict(set)
        for row in converter.rows[owner]:
            if row["column"]:
                folds[row["source_key"]].add(row["column"].lower())
        asserted_keys = set()
        for record in statements.values():
            statement = record["statement"]
            if (
                statement["kind"] != "assert"
                or statement.get("fact", {}).get("entity") != "occurrence"
            ):
                continue
            subject = statement["subject"]
            key = re.search(r"(?:where | and )key = '([^']*)'", subject)
            if key is None:
                continue
            asserted_keys.add(key[1])
            spelling = subject.split(": ", 1)[-1].split(" @", 1)[0]
            spelling = spelling.split(" where ", 1)[0]
            folds[key[1]].add(spelling.lower())
        effective = {}
        used = set()
        for row in converter.rows[owner]:
            if converter.identity.status(row) not in {"used", "shadowed", "rejected"}:
                continue
            targets = assignments.get(id(row), set())
            if len(targets) > 1:
                continue
            target = (
                next(iter(targets)) if targets else converter.identity.default_slug(row)
            )
            if target in ledger:
                effective[id(row)] = target
                if converter.identity.status(row) == "used":
                    used.add(target)
        provider, register = owner
        public = converter.ledgers.get(owner, {}).get("public", register)

        def endpoint_exists(address, used=used, provider=provider, public=public):
            if address.startswith("var:"):
                return address.removeprefix("var:") in used
            parts = address.split("/")
            if len(parts) == 3 and ":" not in parts[0]:
                return parts[:2] == [provider, public] and parts[2] in used
            # Other source addresses are checked by the located lock boundary.
            return True

        for key, record in list(statements.items()):
            statement = record["statement"]
            kind = statement["kind"]
            if kind not in {"ack", "link"}:
                continue
            endpoints = [statement["subject"]]
            if kind == "link":
                endpoints.append(statement["object"])
            if all(endpoint_exists(address) for address in endpoints):
                continue
            dropped.append(
                {
                    "register": "/".join(owner),
                    "subject": statement["subject"],
                    "sources": sorted(set(record["origins"])),
                    "reason": (
                        "legacy diagnostic/link endpoint has no placed final local variable; "
                        "reviewed current finding/link required"
                    ),
                    "statements": 1,
                    "kind": kind,
                }
            )
            counts[f"dropped_endpoint.{kind}"] += 1
            del statements[key]
        combined = {}
        for key, record in grouping.items():
            statement = record["statement"]
            subject = statement["subject"]
            if subject.startswith("var:"):
                target = subject.removeprefix("var:")
                rows = [
                    row
                    for row in converter.rows[owner]
                    if effective.get(id(row)) == target
                ]
                targets = {target} if target in used else set()
            else:
                rows = selected(subject)
                unknown = any(
                    converter.identity.status(row) in {"used", "shadowed", "rejected"}
                    and id(row) not in effective
                    and assignments.get(id(row)) != {"none"}
                    for row in rows
                )
                targets = (
                    set()
                    if unknown
                    else {
                        effective[id(row)]
                        for row in rows
                        if id(row) in effective and effective[id(row)] in used
                    }
                )
            unstable = any(
                row["source_key"] in asserted_keys
                and len(folds[row["source_key"]]) > 1
                and converter.identity.status(row) in {"used", "shadowed", "rejected"}
                and not (
                    len(assignments.get(id(row), set())) == 1
                    and next(iter(assignments[id(row)])) in ledger
                )
                for row in rows
            )
            if unstable:
                dropped.append(
                    {
                        "register": "/".join(owner),
                        "subject": subject,
                        "sources": sorted(set(record["origins"])),
                        "reason": (
                            "grouping identity may change after asserted occurrences; "
                            "reviewed final membership required"
                        ),
                        "statements": 1,
                    }
                )
                counts["deferred_after_asserted_occurrences"] += 1
                del statements[key]
                continue
            if not targets:
                dropped.append(
                    {
                        "register": "/".join(owner),
                        "subject": subject,
                        "sources": sorted(set(record["origins"])),
                        "reason": (
                            "D-12: grouping has no proved placed final variable with a local ledger id"
                        ),
                        "statements": 1,
                    }
                )
                counts["deferred_without_known_local_variable"] += 1
                del statements[key]
                continue
            for target in sorted(targets):
                own = combined.setdefault(
                    target,
                    {"sets": set(), "origins": set(), "evidence": [], "caveat": []},
                )
                own["sets"].update(statement["fields"]["sets"])
                own["origins"].update(record["origins"])
                for field in ("evidence", "caveat"):
                    value = statement.get(field)
                    if value and value not in own[field]:
                        own[field].append(value)
            del statements[key]
        for target, own in sorted(combined.items()):
            statement = {
                "kind": "set",
                "subject": f"var:{target}",
                "fields": {"sets": sorted(own["sets"])},
                "evidence": "\n\n".join(own["evidence"]),
            }
            if own["caveat"]:
                statement["caveat"] = "\n\n".join(own["caveat"])
            key = json.dumps(
                {
                    k: value
                    for k, value in statement.items()
                    if k not in {"evidence", "caveat"}
                },
                sort_keys=True,
                ensure_ascii=False,
            )
            statements[key] = {
                "statement": statement,
                "origins": sorted(own["origins"]),
                "contexts": {field: own[field] for field in ("evidence", "caveat")},
            }
            counts["after"] += 1
    retained = collections.Counter(
        origin
        for statements in converter.statements.values()
        for record in statements.values()
        for origin in set(record["origins"])
    )
    reasons = collections.defaultdict(set)
    for record in dropped:
        for origin in record["sources"]:
            reasons[origin].add(record["reason"])
    for result in converter.results:
        if result["source"] in reasons:
            result["grouping_notes"] = sorted(reasons[result["source"]])
            if not retained[result["source"]]:
                result["outcome"] = "dropped"
                result["reason"] = "; ".join(result["grouping_notes"])
    converter.grouping_audit = dict(counts)
    converter.grouping_deferred = dropped
