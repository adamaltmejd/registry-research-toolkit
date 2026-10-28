"""D-9 classification-only dispatch; deleted with the converter at milestone F."""

import codecs
import collections
import csv
import io
import json
import tomllib
import unicodedata
import zipfile

from rebuild_convert_classifications import fqid, inventory, load_books


def label_rows(converter, books, assets):
    """Join exact legacy descriptors through original CSV coordinates, never hashes.

    The reader pools names by content hash. Equal code lists can have distinct
    descriptor names, so a hash is not evidence that an occurrence used a label.
    """

    def normalized(value):
        return " ".join(unicodedata.normalize("NFC", value).split())

    # Match reg-readers' CP1252 decoding with the five CP850 remnant bytes.
    def decode_remnant(error: UnicodeError):
        if not isinstance(error, UnicodeDecodeError):
            raise error
        return error.object[error.start : error.end].decode("cp850"), error.end

    codecs.register_error("scb_cp850", decode_remnant)
    wanted = {
        normalized(label)
        for book in books
        for label in book["data"].get("binding", {}).get("value_set_labels", [])
    }
    by_cvid = collections.defaultdict(set)
    with zipfile.ZipFile(assets / "scb-raw-2026-03/current-scb-inputs.zip") as archive:
        with archive.open("Vardemangder.csv") as stream:
            for row in csv.DictReader(
                io.TextIOWrapper(stream, encoding="cp1252", errors="scb_cp850"),
                delimiter="|",
            ):
                label = normalized(row["Värdemängdsversion"])
                by_cvid[row["CVID"].strip()].add(label)
        coordinates = {}
        with archive.open("Registerinformation.csv") as stream:
            for number, row in enumerate(
                csv.DictReader(
                    io.TextIOWrapper(stream, encoding="cp1252", errors="scb_cp850"),
                    delimiter="|",
                ),
                2,
            ):
                if labels := by_cvid.get(row["CVID"].strip(), set()) & wanted:
                    coordinates[f"row={number}"] = (
                        labels,
                        len(by_cvid[row["CVID"].strip()]) != 1,
                    )
    rows = collections.defaultdict(list)
    ambiguous = collections.defaultdict(list)
    for values in converter.rows.values():
        for row in values:
            labels = set()
            mixed = False
            for locator in [row["locator"], *row.get("also", [])]:
                if locator["input"].endswith("/Registerinformation.csv"):
                    found, mixed_cvid = coordinates.get(locator["at"], ((), False))
                    labels.update(found)
                    mixed |= mixed_cvid
            for label in labels:
                if mixed or not (
                    row.get("valueset") or row.get("population_valuesets")
                ):
                    ambiguous[label].append(row)
                else:
                    rows[label].append(row)
    converter.classification_source_audit = {
        "matched_cvids": sum(bool(names & wanted) for names in by_cvid.values()),
        "mixed_label_cvids": sum(
            bool(names & wanted) and len(names) != 1 for names in by_cvid.values()
        ),
        "rule": "only single-label CVIDs bind: expiry cannot change the label, including the reader's all-expired fallback",
    }
    return rows, ambiguous


def convert_timeline(converter, provider, register, at, entry, books):
    """Bind only whole occurrences covered by a documented timeline edition."""
    aliases = {
        alias
        for book in books
        if book["definition"].get("family") == entry["family"]
        for alias in book["definition"].get("family_aliases", [])
    }
    editions = {book["definition"]["slug"]: fqid(book["definition"]) for book in books}
    refused, statements = [], 0
    for row in converter.rows[provider, register]:
        if row.get("code_system") not in aliases:
            continue
        start, end = converter.span(row)
        covering = [
            slug
            for slug, lower, upper in entry["editions"]
            if lower <= start and end <= upper
        ]
        if len(covering) != 1:
            refused.append({"locator": row["locator"], "period": [start, end]})
            continue
        converter.add(
            at,
            provider,
            register,
            {
                "kind": "set",
                "subject": converter.selector(row),
                "fields": {"classification": editions[covering[0]]},
                "evidence": entry["evidence"],
            },
        )
        statements += 1
    return {
        "source": at,
        "category": "classification_timeline",
        "outcome": "refused" if refused or not statements else "converted",
        "statements": statements,
        "reason": "timeline windows cut whole source occurrences; no source period was split or truncated"
        if refused
        else "no matching source family declaration"
        if not statements
        else "whole occurrences selected",
        "editions": [
            [editions[slug], lower, upper] for slug, lower, upper in entry["editions"]
        ],
        "refused_occurrences": refused,
    }


def convert(converter, write_curation, assets):
    # Import at execution time: the shared driver owns serialization and reporting.
    from rebuild_convert import ALIASES, FIELDS, ROOT, entries, literal

    books = load_books(ROOT)
    versions = {b["definition"]["short_name"]: fqid(b["definition"]) for b in books}
    inventory_entries, outcomes = inventory(ROOT)
    converter.results.extend(outcomes)
    labels, ambiguous = label_rows(converter, books, assets)
    claims = {}
    book_bindings = {}
    candidates = collections.defaultdict(set)
    for book in books:
        version = fqid(book["definition"])
        for label in book["data"].get("binding", {}).get("value_set_labels", []):
            for row in labels[label]:
                candidates[id(row)].add(version)

    def emit(at, category, rows, version, evidence, **extra):
        result = dict(source=at, category=category, **extra)
        proposals = []
        try:
            if not rows and not extra.get("covered_by_book_binding"):
                raise ValueError("no exact new-frame occurrence endpoint")
            for row in rows:
                owner = row["provider"], row["register"]
                subject = converter.selector(row)
                key = (*owner, subject)
                previous = claims.get(key)
                if previous and previous != version:
                    raise ValueError(
                        f"conflicting exact classification claims: {previous} and {version}"
                    )
                proposals.append((owner, subject))
            for owner, subject in proposals:
                claims[(*owner, subject)] = version
                converter.add(
                    at,
                    *owner,
                    {
                        "kind": "set",
                        "subject": subject,
                        "fields": {"classification": version},
                        "evidence": evidence,
                    },
                )
            result.update(outcome="converted", statements=len(set(proposals)))
        except ValueError as error:
            result.update(outcome="refused", reason=str(error), statements=0)
        converter.results.append(result)

    for book in books:
        definition = book["definition"]
        version = fqid(definition)
        source = book["source"]
        for binding, line in zip(book["bindings"], book["binding_lines"], strict=True):
            provider, public, variable = binding["variable"].split("/")
            register = converter.public.get((provider, public), public)
            at = f"{source}:{line}"
            if variable not in converter.ledgers.get((provider, register), {}).get(
                "variables", {}
            ):
                converter.results.append(
                    {
                        "source": at,
                        "category": "classification-book-binding",
                        "outcome": "refused",
                        "reason": "explicit variable absent from local ledger",
                    }
                )
                continue
            statement: dict = {
                "kind": "set",
                "subject": f"var:{variable}",
                "fields": {"classification": version},
                "evidence": binding.get("note") or source,
            }
            book_bindings[provider, register, variable] = statement
            converter.add(at, provider, register, statement)
            converter.results.append(
                {
                    "source": at,
                    "category": "classification-book-binding",
                    "outcome": "converted",
                    "statements": 1,
                }
            )
    for book in books:
        definition = book["definition"]
        version = fqid(definition)
        source = book["source"]
        label_lines = book["text"].splitlines()
        for label in book["data"].get("binding", {}).get("value_set_labels", []):
            line = next(
                i
                for i, text in enumerate(label_lines, 1)
                if json.dumps(label, ensure_ascii=False) in text
            )
            matched = labels[label] + ambiguous[label]
            rows, covered = [], []
            for row in labels[label]:
                slug = converter.identity.default_slug(row)
                if slug is None and converter.identity.status(row) == "gap_marker":
                    # The projection attaches a marker only when its source key
                    # has one placed default identity, as the resolver does.
                    markers = [
                        name
                        for group, name in converter.identity.marker_groups[
                            row["provider"], row["register"]
                        ].items()
                        if group[0] == row["source_key"]
                    ]
                    if len(markers) == 1:
                        slug = markers[0]
                book_statement = book_bindings.get(
                    (row["provider"], row["register"], slug)
                )
                if (
                    book_statement
                    and book_statement["fields"]["classification"] == version
                ):
                    # Checked against the resolved baseline for all 136 nonempty
                    # occurrences of the nine declared targets (D-9 conversion).
                    converter.add(
                        f"{source}:{line}",
                        row["provider"],
                        row["register"],
                        book_statement
                        | {"evidence": f"Legacy exact value-set label: {label}"},
                    )
                    covered.append(row)
                elif len(candidates[id(row)]) == 1:
                    rows.append(row)
            emit(
                f"{source}:{line}",
                "classification-label-rule",
                rows,
                version,
                f"Legacy exact value-set label: {label}",
                label=label,
                matched_occurrences=len(matched),
                covered_by_book_binding=len(covered),
                refused_occurrences=[
                    r["locator"]
                    for r in matched
                    if r not in covered
                    and (len(candidates[id(r)]) != 1 or r in ambiguous[label])
                ],
            )
            if len(rows) + len(covered) != len(matched):
                converter.results[-1].update(
                    outcome="refused",
                    reason="source population rows name different exact classification versions; ambiguous whole occurrences left unbound",
                )

    for path in sorted((ROOT / "reg_meta_build/curation/providers").glob("*.toml")):
        data = tomllib.loads(path.read_text())
        # Walk nested arrays alongside their original authoring headers.
        lines = [
            i
            for i, line in enumerate(path.read_text().splitlines(), 1)
            if line == "[[register.variable]]"
        ]
        variables = [
            (register, variable)
            for register in data.get("register", [])
            for variable in register.get("variable", [])
        ]
        for (register, variable), line in zip(variables, lines, strict=True):
            if not variable.get("classification"):
                continue
            provider = path.stem
            rows = [
                r
                for r in converter.rows[provider, register["key"]]
                if r["column"] == variable["column"]
                and ("variants" not in variable or r["table"] in variable["variants"])
            ]
            emit(
                f"{path.relative_to(ROOT)}:{line}",
                "classification-provider",
                rows,
                versions[variable["classification"]],
                f"Explicit provider declaration: {variable['classification']}",
            )

    for path in sorted((ROOT / "reg_meta_build/curation/registers").glob("*/*.toml")):
        if path.name.endswith(".auto.toml"):
            continue
        data = tomllib.loads(path.read_text())
        provider = path.parent.name
        public = data["register"]["slug"]
        register = converter.public.get((provider, public), public)
        variants = {
            v["native_id"]: v["slug"]
            for v in data.get("variant", [])
            if "native_id" in v and "slug" in v
        }
        for kind, line, entry in entries(path):
            at = f"{path.relative_to(ROOT)}:{line}"
            if kind == "errata.classification_reference":
                converter.results.append(
                    {
                        "source": at,
                        "category": kind,
                        "outcome": "dropped",
                        "reason": "new readers bind no source classification; clearing has no effect",
                    }
                )
            elif kind == "errata.column" and "classification" in entry:
                table = variants.get(entry["variant"], entry["variant"])
                scope = ",".join(entry["versions"])
                subject = f"{table}: {entry['column']} @{scope}"
                fields = {
                    ALIASES.get(k, k): v
                    for k, v in entry.items()
                    if ALIASES.get(k, k) in FIELDS and k != "classification"
                }
                converter.add(
                    at,
                    provider,
                    register,
                    {
                        "kind": "assert",
                        "subject": subject,
                        "fact": {"entity": "occurrence", "fields": fields},
                        "evidence": entry["evidence"],
                    },
                )
                converter.add(
                    at,
                    provider,
                    register,
                    {
                        "kind": "set",
                        "subject": subject,
                        "fields": {"classification": versions[entry["classification"]]},
                        "evidence": entry["evidence"],
                    },
                )
                converter.results.append(
                    {
                        "source": at,
                        "category": "classification-erratum",
                        "outcome": "converted",
                        "statements": 2,
                    }
                )
            elif kind == "coding.sentinel" and "classification" in entry:
                converter.results.append(
                    {
                        "source": at,
                        "category": "classification-scoped-sentinel",
                        "outcome": "dropped",
                        "reason": "classification-scoped sentinels remain deferred; not a binding",
                    }
                )
            elif kind == "classification_timeline":
                converter.results.append(
                    convert_timeline(converter, provider, register, at, entry, books)
                )

    for name in ("sni69-fob1990-5.csv", "sni92-rams1999-5.csv"):
        converter.results.append(
            {
                "source": f"inputs:catalog/source/classifications/{name}:1",
                "category": "classification-extra-value-set",
                "outcome": "dropped",
                "reason": "no book declaration; preserved by reader as ordinary value set, not inventoried as classification",
            }
        )

    converter.write(write_curation, preserve=True)
    inventory_text = (
        "# Converted classification inventory; book periods describe existence only.\n"
    )
    for entry in inventory_entries:
        inventory_text += (
            "\n[[classification]]\n"
            + "\n".join(f"{k} = {literal(v)}" for k, v in entry.items())
            + "\n"
        )
    (converter.curation / "classifications.toml").write_text(inventory_text)
    converter.classification_conversion = {
        "versions": len(inventory_entries),
        "families": len({e["family"] for e in inventory_entries}),
        "successions": sum(len(e.get("succeeds", [])) for e in inventory_entries),
        "source_labels": converter.classification_source_audit,
    }
