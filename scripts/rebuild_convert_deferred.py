"""One-shot deferred conversions; deleted at milestone F (REBUILD.md)."""

import re

D3B_SCOPE = {
    "errata.occurrence_period": ["period"],
    "representation.delivery_metadata": ["description", "label"],
    "errata.delivered": [],
    "errata.version": [],
    "representation.parallel": [],
    "errata.classification_reference": [],
}


def period(scope):
    """Translate stated calendar intervals without inventing noncalendar coverage."""
    if scope.get("kind") != "intervals" or not scope.get("intervals"):
        raise ValueError("D-7: noncalendar occurrence coverage needs a design decision")
    tokens = []
    for interval in scope["intervals"]:
        start, end = interval.get("start", ""), interval.get("end", "")
        if not all(
            not x or re.fullmatch(r"\d{4}(?:-\d{2}-\d{2})?", x) for x in (start, end)
        ):
            raise ValueError("occurrence coverage has an unsupported calendar bound")
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
        rows = converter.match(provider, register, entry, pins, variants)
        return [
            {
                "kind": "set",
                "subject": converter.selector(row),
                "fields": {"period": value},
            }
            for row in rows
        ]
    if kind == "errata.classification_reference":
        raise ValueError(
            "classification clearing needs a design decision; no replacement family is declared"
        )
    if kind == "representation.parallel":
        raise ValueError(
            "parallel metadata sharing/preservation needs reviewed per-field translation; grouping alone is not equivalent"
        )
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
            value = name
        elif re.fullmatch(r"\d{4} - \d{4}", name):
            value = name.replace(" - ", "..")
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
    if any(row.get("operational_definition") for row in rows):
        raise ValueError(
            "delivered donor operational_definition has no four-kind field"
        )
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
        scope = f"{release['from']}..{release['to']}"
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
    return proposals
