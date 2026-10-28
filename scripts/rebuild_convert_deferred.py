"""One-shot deferred conversions; deleted at milestone F (REBUILD.md)."""

import re
from datetime import date

D3B_SCOPE = {
    "errata.occurrence_period": ["period"],
    "representation.delivery_metadata": ["description", "label"],
    "errata.delivered": ["operational_definition"],
    "errata.version": [],
    "representation.parallel": ["sets"],
    "errata.classification_reference": [],
}


def period(scope):
    """Translate stated calendar intervals without inventing noncalendar coverage."""
    if scope.get("kind") != "intervals" or not scope.get("intervals"):
        raise ValueError("D-7")
    tokens = []
    for interval in scope["intervals"]:
        start, end = interval.get("start", ""), interval.get("end", "")
        # The frame sentinels mean open time, not literal period-grammar years.
        start = "" if start == "0001-01-01" else start
        end = "" if end == "9999-12-31" else end
        for value in (start, end):
            if not value:
                continue
            if not re.fullmatch(r"[0-9]{4}(?:-[0-9]{2}-[0-9]{2})?", value):
                raise ValueError(
                    "occurrence coverage has an unsupported calendar bound"
                )
            if not 1900 <= int(value[:4]) <= 2099:
                raise ValueError(
                    "period year outside 1900..2099 has no faithful grammar expression"
                )
            if len(value) > 4:
                try:
                    date.fromisoformat(value)
                except ValueError as error:
                    raise ValueError(
                        "occurrence coverage has an invalid calendar day"
                    ) from error
        if not start and not end:
            raise ValueError("fully open coverage has no period-grammar expression")
        lower = start + "-01-01" if len(start) == 4 else start
        upper = end + "-12-31" if len(end) == 4 else end
        if lower and upper and lower > upper:
            raise ValueError("occurrence coverage bounds are reversed")
        tokens.append(start if start == end else f"{start}..{end}")
    return ",".join(tokens)


def table(converter, provider, register, variant, variants):
    slug = variants.get(
        variant, converter.tables.get((provider, register, variant), variant)
    )
    matches = {
        handle
        for (p, r, handle), public in converter.tables.items()
        if (p, r, public) == (provider, register, slug)
    }
    if len(matches) != 1:
        raise ValueError(
            "assertion needs one existing table; table declaration requires review"
        )
    return matches.pop(), slug


def convert(converter, provider, register, kind, entry, pins, variants):
    """Return proposals; None means this helper does not own the category."""
    if kind == "errata.occurrence_period":
        replacement = entry["edition_period_scope"]
        if replacement["kind"] == "not_applicable":
            replacement = entry["edition_scope"]
        value = period(replacement)
        if "," in value:
            raise ValueError(
                "noncontiguous occurrence coverage cannot be expressed by set period"
            )
        rows = converter.match(provider, register, entry, pins, variants)
        interval = replacement["intervals"][0]
        start, end = interval.get("start", ""), interval.get("end", "")
        start = (start + "-01-01" if len(start) == 4 else start) or "0001-01-01"
        end = (end + "-12-31" if len(end) == 4 else end) or "9999-12-31"
        for row in rows:
            parent = converter.context_tables.get(
                (provider, register, row["table"]), {}
            )
            register_bounds = converter.context_registers.get((provider, register), {})
            lower = parent.get("from") or register_bounds.get("from")
            upper = parent.get("to") or register_bounds.get("to")
            if (lower and start < lower) or (upper and end > upper):
                raise ValueError(
                    "occurrence period requires an explicit parent coverage correction; no scope is shrunk"
                )
        subjects = [converter.selector(row) for row in rows]
        proposals = []
        for row, subject in zip(rows, subjects, strict=True):
            labels = {
                peer.get("label")
                for peer, other in zip(rows, subjects, strict=True)
                if other == subject
            }
            if len(labels) > 1:
                label = row.get("label")
                if not label or any(char in label for char in "'\n\r"):
                    raise ValueError(
                        "period correction needs a distinguishable literal label guard"
                    )
                subject += f" and label = '{label}'"
            proposals.append(
                {"kind": "set", "subject": subject, "fields": {"period": value}}
            )
        return proposals
    if kind == "errata.classification_reference":
        raise ValueError("new reader binds no source classification")
    if kind == "representation.parallel":
        if entry.get("case_aliases"):
            return []
        group = "legacy-" + pins.get(
            entry["variable"], entry["variable"].replace(".", "-")
        )
        proposals = []
        for column in entry["columns"]:
            endpoint = {
                "variable": entry["variable"],
                "variant": entry["variant"],
                "column": column["column"],
                "source_editions": column["source_editions"],
                "valid_from": column["valid_from"],
                "valid_to": column["valid_to"],
            }
            try:
                rows = converter.match(provider, register, endpoint, pins, variants)
            except ValueError as error:
                if str(error) != "no exact new-frame occurrence endpoint":
                    raise
                unbounded = {
                    k: v
                    for k, v in endpoint.items()
                    if k not in {"valid_from", "valid_to"}
                }
                try:
                    converter.match(provider, register, unbounded, pins, variants)
                except ValueError:
                    raise ValueError(
                        "parallel declared occurrence absent from new frames; reviewed assertion required"
                    ) from error
                raise ValueError(
                    "D-7: parallel legacy window disagrees with new-reader release placement"
                ) from error
            for row in rows:
                proposals.append(
                    {
                        "kind": "set",
                        "subject": converter.selector(row),
                        "fields": {"sets": [group]},
                    }
                )
        return proposals
    if kind == "representation.delivery_metadata":
        proposals = []
        for record in entry["records"]:
            coordinates = dict(
                token.split(":", 1) for token in record["ref"]["semantic_record_key"]
            )
            fields = {}
            for field in entry["fields"]:
                values = {
                    claim["value"]
                    for alternative in record["alternatives"]
                    for claim in alternative["fields"]
                    if claim["name"] == field and claim["status"] == "value"
                }
                if len(values) != 1:
                    raise ValueError(
                        "delivery metadata alternatives disagree or lack the requested value"
                    )
                fields["label" if field == "name" else field] = values.pop()
            columns = {
                claim["value"]
                for alternative in record["alternatives"]
                for claim in alternative["fields"]
                if claim["name"] == "column_name" and claim["status"] == "value"
            }
            if len(columns) != 1:
                raise ValueError("delivery metadata needs one pinned literal column")
            endpoint = {
                "column": columns.pop(),
                "deldatamangd": coordinates["deldatamangd"],
            }
            rows = converter.match(provider, register, endpoint, pins, variants)
            for row in rows:
                windows = [c for c in entry["columns"] if c["column"] == row["column"]]
                start, end = converter.span(row)
                if (
                    not start
                    or not end
                    or not any(
                        c["valid_from"] <= start and end <= c["valid_to"]
                        for c in windows
                    )
                ):
                    raise ValueError(
                        "delivery metadata window does not cover a whole source occurrence"
                    )
                proposals.append(
                    {
                        "kind": "set",
                        "subject": converter.selector(row),
                        "fields": fields,
                    }
                )
        return proposals
    if kind == "errata.version":
        _, slug = table(converter, provider, register, entry["variant"], variants)
        name = entry["name"]
        if re.fullmatch(r"\d{4}", name):
            value = period(
                {"kind": "intervals", "intervals": [{"start": name, "end": name}]}
            )
        elif re.fullmatch(r"\d{4} - \d{4}", name):
            start, end = name.split(" - ")
            value = period(
                {"kind": "intervals", "intervals": [{"start": start, "end": end}]}
            )
        else:
            raise ValueError(
                "release name needs reviewed coverage interpretation; do not infer coverage from its token shape"
            )
        return [
            {
                "kind": "assert",
                "subject": f"release:{slug}/{name}",
                "fact": {"entity": "release", "name": name, "period": value},
            }
        ]
    if kind == "errata.delivered":
        return delivered(converter, provider, register, entry, variants)
    return None


def delivered(converter, provider, register, entry, variants):
    handle, slug = table(converter, provider, register, entry["variant"], variants)
    donor = handle
    if entry.get("donor_variant"):
        donor, _ = table(
            converter, provider, register, entry["donor_variant"], variants
        )
    rows = [
        r
        for r in converter.rows[provider, register]
        if r["table"] == donor and r["column"].lower() == entry["column"].lower()
    ]
    if entry.get("native_variable_id") is not None:
        rows = [r for r in rows if r["source_key"] == f"v{entry['native_variable_id']}"]
    if not rows:
        raise ValueError(
            "delivered occurrence has no exact new-frame donor; identity/facts require D-12 review"
        )
    keys = {row["source_key"] for row in rows}
    if len(keys) != 1:
        raise ValueError(
            "delivered donor has multiple source keys; identity requires review"
        )
    source_key = keys.pop()
    operational_definitions = {row.get("operational_definition") for row in rows}
    if len(operational_definitions) != 1:
        raise ValueError(
            "delivered donor facts disagree in operational_definition; reviewed edition-specific mapping required"
        )
    operational_definition = operational_definitions.pop()
    if "'" in source_key:
        raise ValueError("delivered donor key cannot be expressed by address guard")
    # A curator's delivery has no inherited unit. Coding is separately opted into.
    names = {
        "label": "label",
        "definition": "definition",
        "dtype": "type",
        "length": "length",
        "sensitive": "sensitive",
        "identifier": "identifier",
    }
    proposals = []
    asserted_periods = []
    for version in entry["versions"]:
        releases = [
            r
            for (p, g, t, _), r in converter.releases.items()
            if (p, g, t) == (provider, register, handle) and r["name"] == version
        ]
        if (
            len(releases) != 1
            or not releases[0].get("from")
            or not releases[0].get("to")
        ):
            raise ValueError(
                "delivered occurrence needs one dated existing release; convert the missing release first"
            )
        release = releases[0]
        bounds = release["from"], release["to"]
        if any(max(bounds[0], a) <= min(bounds[1], b) for a, b in asserted_periods):
            raise ValueError(
                "overlapping delivered releases need an assertion-conflict design decision"
            )
        asserted_periods.append(bounds)
        if any(
            row["table"] == handle
            and row["column"].lower() == entry["column"].lower()
            and row.get("release") == release["release"]
            for row in converter.rows[provider, register]
        ):
            raise ValueError(
                "delivery already documented in new frames; any metadata correction needs explicit set review"
            )
        fields = {}
        for source, target in names.items():
            values = {r.get(source) for r in rows}
            if len(values) != 1:
                raise ValueError(
                    f"delivered donor facts disagree in {target}; reviewed edition-specific mapping required"
                )
            value = values.pop()
            if value is not None:
                fields[target] = value
        if entry.get("data_type"):
            fields["type"] = entry["data_type"]
        if entry.get("measurement_unit"):
            fields["unit"] = entry["measurement_unit"]
        if entry.get("donor_coding"):
            coded_rows = [
                r
                for r in rows
                if converter.releases.get(
                    (provider, register, donor, r.get("release")), {}
                ).get("name")
                == version
            ]
            if not coded_rows or any(r.get("population_valuesets") for r in coded_rows):
                raise ValueError(
                    "donor coding needs exact edition-specific non-population evidence"
                )
            for source, target in (("valueset", "value_set"), ("coded", "coded")):
                values = {r.get(source) for r in coded_rows}
                if len(values) != 1:
                    raise ValueError("donor coding differs within the named edition")
                value = values.pop()
                if value is not None:
                    fields[target] = value
        # Assertions create source-shaped rows; an existing ledger anchor must name them.
        anchor = f"{handle}: {entry['column']}" if handle else entry["column"]
        owners = [
            owner
            for owner, anchors in converter.ledgers[provider, register]
            .get("variables", {})
            .items()
            if any(a.lower() == anchor.lower() for a in anchors)
        ]
        if len(owners) != 1:
            raise ValueError(
                "D-12: delivered occurrence has no unique existing unqualified ledger anchor"
            )
        scope = period(
            {
                "kind": "intervals",
                "intervals": [{"start": release["from"], "end": release["to"]}],
            }
        )
        if "'" in release["release"]:
            raise ValueError("delivered release cannot be expressed by address guard")
        proposals.append(
            {
                "kind": "assert",
                "subject": (
                    f"{slug}: {entry['column']} @{scope} where key = '{source_key}'"
                    f" and release = '{release['release']}'"
                ),
                "fact": {"entity": "occurrence", "fields": fields},
            }
        )
        if operational_definition:
            # d-set-fields will add the stated field; keep it out of today's closed assert schema.
            proposals.append(
                {
                    "kind": "set",
                    "subject": proposals[-1]["subject"],
                    "fields": {"operational_definition": operational_definition},
                }
            )
    return proposals
