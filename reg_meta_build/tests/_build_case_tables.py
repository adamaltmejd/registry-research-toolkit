"""The named projections a `cases/build/` `expected.json` reads.

Each table is one row function or SQL query over the build result, the report ledger
or the built artifact; `cases/build/README.md` documents the fields.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from _build_case_curation import RECORD_FILTERS
from reg_meta_build.db import stored_data_warnings

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from _build_case_sources import PreparedSet


def _locator(detail: str) -> str | None:
    return detail.split(":", 1)[0] if detail.startswith("curation/") else None


@dataclass(frozen=True)
class Outcome:
    result: dict
    events: list[dict]
    db: Path
    built: PreparedSet

    def table(self, name: str) -> list[dict]:
        return _TABLES[name](self)

    def _sql(self, sql: str) -> list[dict]:
        with closing(sqlite3.connect(self.db)) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(sql)]


def _refs(refs) -> list[str]:
    """Source record refs as `<source>#<semantic key parts joined by />`."""
    return [
        f"{ref['source']}#{'/'.join(ref['semantic_record_key'])}" for ref in refs or ()
    ]


def _issue_row(event: dict) -> dict:
    return {
        "code": event["code"],
        "severity": event["severity"],
        "subject": str(event["subject"]),
        "case_id": event.get("case_id"),
        "locator": _locator(str(event.get("detail") or "")),
        "detail": event.get("detail"),
        "acknowledged_by": event.get("acknowledged_by"),
        "valid_from": event.get("valid_from"),
        "valid_to": event.get("valid_to"),
        "fields": event.get("fields") or [],
        "withheld_output": event.get("withheld_output") or [],
    }


def _issues(outcome: Outcome) -> list[dict]:
    return [_issue_row(event) for event in outcome.events if event["kind"] == "issue"]


def _issue_refs(outcome: Outcome) -> list[dict]:
    """One row per source record an issue cites: the issue's fields, then the ref,
    as its parts and in the `_refs` string form other tables' `refs` use."""
    return [
        {
            **_issue_row(event),
            "source": ref["source"],
            "key": ref["semantic_record_key"],
            "ref": _refs([ref])[0],
        }
        for event in outcome.events
        if event["kind"] == "issue"
        for ref in event.get("refs") or ()
    ]


def _source_issues(outcome: Outcome) -> list[dict]:
    """Support- and value-source issues: the source evidence behind ledger issues."""
    return [
        {
            "kind": event["kind"],
            "severity": event.get("severity"),
            "descriptor_key": event.get("descriptor_key"),
            "physical_associations": event.get("physical_associations"),
            "refs": _refs(event.get("refs")),
        }
        for event in outcome.events
        if event["kind"] in {"support_source_issue", "value_source_issue"}
    ]


def _dispositions(outcome: Outcome) -> list[tuple[dict, list[str]]]:
    """Each ledger disposition of a prepared source record occurrence as a `uses`
    row, with the curation case ids the disposition names."""
    by_id = {
        event["record_id"]: event["dispositions"]
        for event in outcome.events
        if event["kind"] == "source_occurrence"
    }
    rows = []
    for record in outcome.built.records():
        fields = {
            name: value.value
            if (value := getattr(record.fields, name)) is not None
            else None
            for name in ("column_name", "data_type", "name", "description")
        }
        for disposition in by_id.get(record.record_id, ()):
            row = {
                "source": record.source,
                "native_variable": RECORD_FILTERS["variable"](record),
                "key": record.locators[0].semantic_record_key[-1],
                **fields,
                "use": disposition["use"],
                "variable": disposition["variable"],
            }
            rows.append((row, disposition["cases"]))
    return rows


def _case_uses(outcome: Outcome) -> list[dict]:
    """One row per curation case a source record's ledger disposition names."""
    return [
        {"case_id": case_id, **{key: row[key] for key in _CASE_USE_FIELDS}}
        for row, cases in _dispositions(outcome)
        for case_id in cases
    ]


_CASE_USE_FIELDS = ("source", "key", "use", "variable")


def _variables(outcome: Outcome) -> list[dict]:
    """Each built variable's distinct delivery columns; one null row when it has none."""
    rows = outcome._sql(
        "SELECT DISTINCT r.slug AS register, v.slug AS variable, "
        "s.delivery_column_name AS column, v.provider_key, v.name, v.definition, "
        "v.description, v.measurement_unit, v.operational_definition, "
        "v.is_identifier, v.is_sensitive, v.deprecated, "
        "sr.slug AS source_register, v.source_label, v.source_register_text "
        "FROM variable v JOIN register r USING (register_id) "
        "LEFT JOIN register sr ON sr.register_id = v.source_register_id "
        "LEFT JOIN variable_state s USING (variable_id)"
    )
    named = {
        (row["register"], row["variable"]) for row in rows if row["column"] is not None
    }
    return [
        row
        for row in rows
        if row["column"] is not None or (row["register"], row["variable"]) not in named
    ]


def _concept_groups(outcome: Outcome) -> list[dict]:
    """Each concept group as the sorted slugs of its member variables."""
    groups: dict[int, list[str]] = {}
    for row in outcome._sql(
        "SELECT gv.group_id, v.slug FROM concept_group_variable gv "
        "JOIN variable v USING (variable_id)"
    ):
        groups.setdefault(row["group_id"], []).append(row["slug"])
    return [{"variables": sorted(slugs)} for slugs in groups.values()]


def _group_members(outcome: Outcome) -> list[dict]:
    """Each concept-group member with its literal column and its facets."""
    facets: dict[int, list[list]] = {}
    for row in outcome._sql(
        "SELECT member_id, axis, value, label FROM concept_group_variable_facet"
    ):
        facets.setdefault(row["member_id"], []).append(
            [row["axis"], row["value"], row["label"]]
        )
    rows = outcome._sql(
        "SELECT m.member_id, g.group_key, r.slug AS register, "
        "v.slug AS variable, m.delivery_column_name AS column "
        "FROM concept_group_variable m JOIN concept_group g USING (group_id) "
        "JOIN variable v USING (variable_id) "
        "JOIN register r ON r.register_id = v.register_id"
    )
    for row in rows:
        row["facets"] = sorted(facets.get(row.pop("member_id"), []))
    return rows


def _editions(outcome: Outcome) -> list[dict]:
    """Each register version with its prose, populations and object types."""
    populations: dict[int, list[list]] = {}
    for row in outcome._sql(
        "SELECT regver_id, name, definition, comment, date_range FROM population"
    ):
        populations.setdefault(row.pop("regver_id"), []).append(list(row.values()))
    objects: dict[int, list[list]] = {}
    for row in outcome._sql("SELECT regver_id, name, definition FROM object_type"):
        objects.setdefault(row.pop("regver_id"), []).append(list(row.values()))
    rows = outcome._sql(
        "SELECT e.regver_id, r.slug AS register, rv.slug AS variant, "
        "e.registerversionnamn AS name, "
        "e.registerversionbeskrivning AS description, "
        "e.registerversionmatinformation AS measurement_information, "
        "e.registerversion_docstaus AS documentation_status, "
        "e.registerversion_forstagodkannandedatum AS first_approved_at, "
        "e.registerversion_senastgodkanddatum AS last_approved_at "
        "FROM register_version e JOIN register_variant rv USING "
        "(register_variant_id) JOIN register r USING (register_id)"
    )
    for row in rows:
        edition = row.pop("regver_id")
        row["populations"] = sorted_rows(populations.get(edition, []))
        row["object_types"] = sorted_rows(objects.get(edition, []))
    return rows


def _value_sets(outcome: Outcome) -> list[dict]:
    """Each stored value set: its id, its sorted members and the sorted FQIDs of the
    variables whose states or alias windows carry it."""
    sets: dict[int, dict] = {}
    for row in outcome._sql(
        "SELECT value_set_id, code, label FROM value_set "
        "LEFT JOIN value_set_member USING (value_set_id) "
        "LEFT JOIN value_code USING (code_id)"
    ):
        entry = sets.setdefault(
            row["value_set_id"],
            {"id": str(row["value_set_id"]), "members": [], "variables": set()},
        )
        if row["code"] is not None:
            entry["members"].append([row["code"], row["label"]])
    for row in outcome._sql(
        "SELECT u.value_set_id, p.slug || '/' || r.slug || '/' || v.slug AS fqid "
        "FROM (SELECT variable_id, value_set_id FROM variable_state "
        "UNION SELECT variable_id, value_set_id FROM variable_alias_window) u "
        "JOIN variable v USING (variable_id) "
        "JOIN register r ON r.register_id = v.register_id "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "WHERE u.value_set_id IS NOT NULL"
    ):
        sets[row["value_set_id"]]["variables"].add(row["fqid"])
    return [
        {
            **entry,
            "members": sorted(entry["members"]),
            "variables": sorted(entry["variables"]),
        }
        for entry in sets.values()
    ]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _warning_rows(outcome: Outcome, warnings: list[dict]) -> list[dict]:
    """Warnings in the `DataWarning` shape, projected with their coordinates as slugs
    and `detail_hash_of` naming what `diagnostic_detail_sha256` hashes."""
    # A warning copies its diagnostic's refs, so `refs` stands in for the subject the
    # warning does not carry: two issues sharing a code and detail stay apart.
    issue_hashes = {
        (
            event["code"],
            _sha256(event.get("detail") or ""),
            tuple(_refs(event.get("refs"))),
        )
        for event in outcome.events
        if event["kind"] == "issue"
    }
    rows = []
    for payload in warnings:
        variable = payload.get("variable_fqid")
        digest = payload["diagnostic_detail_sha256"]
        rows.append(
            {
                "register": payload["register_fqid"].split("/")[-1],
                "variable": variable.split("/")[-1] if variable else None,
                "variant": payload.get("variant"),
                "column": payload.get("delivery_column_name"),
                "valid_from": payload.get("valid_from"),
                "valid_to": payload.get("valid_to"),
                "code": payload["code"],
                "severity": payload["severity"],
                "detail": payload["detail"],
                "summary": payload.get("summary"),
                "detail_hash_of": "issue"
                if (payload["code"], digest, tuple(_refs(payload.get("refs"))))
                in issue_hashes
                else "detail"
                if _sha256(payload["detail"]) == digest
                else None,
                "fields": payload.get("fields"),
                "refs": _refs(payload.get("refs")),
                "withheld_output": payload.get("withheld_output"),
                "acknowledged_by": payload.get("acknowledged_by"),
                "case_id": payload.get("case_id"),
                "warning_id": payload["warning_id"],
            }
        )
    return rows


def _warnings(outcome: Outcome) -> list[dict]:
    with closing(sqlite3.connect(outcome.db)) as conn:
        stored = stored_data_warnings(conn)
    return _warning_rows(outcome, [w.model_dump(mode="json") for w in stored])


def _report_warnings(outcome: Outcome) -> list[dict]:
    return _warning_rows(
        outcome,
        [event for event in outcome.events if event["kind"] == "data_warning"],
    )


def _alias_windows(outcome: Outcome) -> list[dict]:
    """Each alias window of a delivery column, with its per-column facts, the
    members of its per-column value set and the books bound to it."""
    key = "w.variable_id, w.register_variant_id, w.delivery_column_name, w.valid_from"
    codes: dict[tuple, list[list[str]]] = {}
    for row in outcome._sql(
        f"SELECT {key}, c.code, c.label FROM variable_alias_window w "
        "JOIN value_set_member m ON m.value_set_id = w.value_set_id "
        "JOIN value_code c ON c.code_id = m.code_id"
    ):
        codes.setdefault(tuple(row.values())[:4], []).append(
            [row["code"], row["label"]]
        )
    books: dict[tuple, list[str]] = {}
    for row in outcome._sql(
        f"SELECT {key}, c.slug FROM alias_window_classification w "
        "JOIN classification c ON c.id = w.classification_id"
    ):
        books.setdefault(tuple(row.values())[:4], []).append(row["slug"])
    rows = []
    for row in outcome._sql(
        f"SELECT {key}, r.slug AS register, v.slug AS variable, rv.slug AS variant, "
        "w.delivery_column_name AS column, w.valid_to, w.column_metadata, "
        "w.provenance, w.coding_metadata, w.data_type, w.data_length, w.definition, "
        "w.measurement_unit, w.name, w.description, w.operational_definition, "
        "w.source_register_text FROM variable_alias_window w JOIN variable v USING (variable_id) "
        "JOIN register r ON r.register_id = v.register_id "
        "JOIN register_variant rv ON rv.register_variant_id = w.register_variant_id"
    ):
        ident = tuple(row.values())[:4]
        row = {
            k: v
            for k, v in row.items()
            if k not in {"variable_id", "register_variant_id", "delivery_column_name"}
        }
        rows.append(
            {
                **row,
                "codes": sorted(codes.get(ident, [])),
                "classifications": sorted(books.get(ident, [])),
            }
        )
    return rows


_ALIAS_CONFORMANCE_SQL = (
    "SELECT r.slug AS register, v.slug AS variable, "
    "w.delivery_column_name AS column, w.valid_from, w.valid_to, "
    "c.slug AS classification, a.conformance "
    "FROM alias_window_classification a "
    "JOIN variable_alias_window w USING "
    "(variable_id, register_variant_id, delivery_column_name, valid_from) "
    "JOIN classification c ON c.id = a.classification_id "
    "JOIN variable v ON v.variable_id = a.variable_id "
    "JOIN register r ON r.register_id = v.register_id "
    "WHERE a.conformance IS NOT NULL"
)


def _alias_conformance(outcome: Outcome) -> list[tuple[dict, dict]]:
    """Each alias window's book binding with its stored conformance evidence."""
    return [
        (
            {key: row[key] for key in row.keys() - {"conformance"}},
            json.loads(row["conformance"]),
        )
        for row in outcome._sql(_ALIAS_CONFORMANCE_SQL)
    ]


def _alias_extensions(evidence: dict) -> list[tuple[str, str, str]]:
    return [
        (code, label, kind)
        for kind, key in (
            ("nonstandard", "nonconforming_members"),
            ("sentinel", "sentinel_members"),
        )
        for code, label in evidence[key]
    ]


def _conformance(outcome: Outcome) -> list[dict]:
    """Each state's and alias window's conformance to one bound book.

    An alias window stores its evidence as JSON; its counts are read the way the
    reader counts them (distinct checked codes, distinct codes outside the book).
    """
    rows = [
        {**row, "window": "state"}
        for row in outcome._sql(
            "SELECT r.slug AS register, v.slug AS variable, "
            "s.delivery_column_name AS column, s.valid_from, s.valid_to, "
            "c.slug AS classification, cc.status, cc.checked_code_count AS checked, "
            "cc.matched_code_count AS matched, "
            "cc.nonconforming_code_count AS nonconforming, cc.overlap "
            "FROM classification_conformance cc "
            "JOIN classification c ON c.id = cc.declared_classification_id "
            "JOIN variable_state s ON s.state_id = cc.state_id "
            "JOIN variable v ON v.variable_id = s.variable_id "
            "JOIN register r ON r.register_id = v.register_id"
        )
    ]
    for row, evidence in _alias_conformance(outcome):
        checked = len(set(evidence["checked_codes"]))
        outside = len({code for code, _, _ in _alias_extensions(evidence)})
        rows.append(
            {
                **row,
                "window": "alias",
                "status": evidence["status"],
                "checked": checked,
                "matched": checked - outside,
                "nonconforming": outside,
                "overlap": (checked - outside) / checked if checked else 1.0,
            }
        )
    return rows


def _conformance_codes(outcome: Outcome) -> list[dict]:
    """Each source member a state's or alias window's book conformance records
    outside the book.

    A scoped sentinel certificate is projected as its window only; its fingerprints
    restate the code under test. An alias window stores no sentinel meaning.
    """
    rows = [
        {
            **{key: row[key] for key in row.keys() - {"scoped_sentinels"}},
            "window": "state",
            "scoped_windows": [
                [certificate["valid_from"], certificate["valid_to"]]
                for certificate in json.loads(row["scoped_sentinels"])
            ],
        }
        for row in outcome._sql(
            "SELECT r.slug AS register, v.slug AS variable, "
            "s.delivery_column_name AS column, s.valid_from, s.valid_to, "
            "c.slug AS classification, vc.code, vc.label, cc.member_kind, "
            "cc.sentinel_meaning, cc.scoped_sentinels "
            "FROM classification_conformance_code cc "
            "JOIN classification c ON c.id = cc.declared_classification_id "
            "JOIN value_code vc ON vc.code_id = cc.code_id "
            "JOIN variable_state s ON s.state_id = cc.state_id "
            "JOIN variable v ON v.variable_id = s.variable_id "
            "JOIN register r ON r.register_id = v.register_id"
        )
    ]
    for row, evidence in _alias_conformance(outcome):
        rows.extend(
            {
                **row,
                "window": "alias",
                "code": code,
                "label": label,
                "member_kind": kind,
                "sentinel_meaning": None,
                "scoped_windows": [
                    [certificate["valid_from"], certificate["valid_to"]]
                    for certificate in evidence["scoped_sentinels"]
                    if [code, label] in certificate["members"]
                ]
                if kind == "sentinel"
                else [],
            }
            for code, label, kind in _alias_extensions(evidence)
        )
    return rows


_STATE_JOIN = (
    "FROM variable_state s JOIN variable v USING (variable_id) "
    "JOIN register r ON r.register_id = v.register_id "
    "JOIN register_variant rv ON rv.register_variant_id = s.register_variant_id "
)
# Each state's id and coordinates; the lineage tables join it once per edge end.
_STATE_COORDINATES = (
    "SELECT s.state_id, r.slug AS register, v.slug AS variable, rv.slug AS variant, "
    "s.delivery_column_name AS column, s.valid_from, s.valid_to " + _STATE_JOIN
)
# A succession endpoint's register or variable FQID, from a `{side}_provider`,
# `{side}_register` (and `{side}_variable`) column triple.
_REGISTER = "{0}_provider || '/' || {0}_register"
_VARIABLE = "{0}_provider || '/' || {0}_register || '/' || {0}_variable"
_SUCCESSION_FACTS = "effective_year, note, beskrivning AS description"
# The register grain stores no `note`; the variant grain stores endpoints only.
_REGISTER_SUCCESSION_FACTS = "effective_year, NULL AS note, beskrivning AS description"
_TABLES: dict[str, Callable[[Outcome], list[dict]]] = {
    "issues": _issues,
    "issue_refs": _issue_refs,
    "cases": lambda o: [
        {
            "case_id": event["case_id"],
            "status": event["status"],
            "reason": event.get("reason"),
        }
        for event in o.events
        if event["kind"] == "case"
    ],
    "uses": lambda o: [row for row, _ in _dispositions(o)],
    "case_uses": _case_uses,
    "states": lambda o: o._sql(
        "SELECT r.slug AS register, v.slug AS variable, rv.slug AS variant, "
        "s.delivery_column_name AS column, s.valid_from, s.valid_to, s.data_type, "
        "v.name, s.name AS state_name, s.provenance, s.pooled, s.data_length, "
        "s.definition, s.measurement_unit, s.description, s.operational_definition, "
        "s.source_register_text, s.value_set_version_label " + _STATE_JOIN
    ),
    "state_codes": lambda o: o._sql(
        "SELECT r.slug AS register, v.slug AS variable, rv.slug AS variant, "
        "s.delivery_column_name AS column, s.valid_from, s.valid_to, c.code, c.label "
        + _STATE_JOIN
        + "LEFT JOIN value_set_member m ON m.value_set_id = s.value_set_id "
        "LEFT JOIN value_code c ON c.code_id = m.code_id"
    ),
    "variables": _variables,
    "variants": lambda o: o._sql(
        "SELECT r.slug AS register, rv.slug AS variant, rv.name, "
        "rv.panel_entity_key, rv.panel_time_key, rv.panel_time_grain, "
        "rv.description, rv.display_group "
        "FROM register_variant rv JOIN register r USING (register_id)"
    ),
    "registers": lambda o: o._sql(
        "SELECT slug AS register, name, purpose FROM register"
    ),
    "editions": _editions,
    # One row per tag member as its FQID; a tag without members is one null row.
    "tags": lambda o: o._sql(
        "SELECT t.slug, CASE WHEN m.variable_id IS NOT NULL THEN "
        "vp.slug || '/' || vr.slug || '/' || v.slug "
        "WHEN m.register_id IS NOT NULL THEN mp.slug || '/' || mr.slug END "
        "AS member FROM tag t LEFT JOIN tag_member m USING (tag_id) "
        "LEFT JOIN variable v ON v.variable_id = m.variable_id "
        "LEFT JOIN register vr ON vr.register_id = v.register_id "
        "LEFT JOIN provider vp ON vp.provider_id = vr.provider_id "
        "LEFT JOIN register mr ON mr.register_id = m.register_id "
        "LEFT JOIN provider mp ON mp.provider_id = mr.provider_id"
    ),
    "aliases": lambda o: o._sql(
        "SELECT r.slug AS register, v.slug AS variable, rv.slug AS variant, "
        "a.delivery_column_name AS column FROM variable_alias a "
        "JOIN variable v USING (variable_id) "
        "JOIN register r ON r.register_id = v.register_id "
        "JOIN register_variant rv ON rv.register_variant_id = a.register_variant_id"
    ),
    "alias_windows": _alias_windows,
    "concept_groups": _concept_groups,
    "group_axes": lambda o: o._sql(
        "SELECT g.group_key, a.axis, a.ordinal, a.label FROM concept_group_axis a "
        "JOIN concept_group g USING (group_id)"
    ),
    "group_labels": lambda o: o._sql(
        "SELECT group_key, label, source FROM concept_group"
    ),
    "group_members": _group_members,
    "warnings": _warnings,
    "report_warnings": _report_warnings,
    "search_pins": lambda o: o._sql(
        "SELECT key AS query, type, position, entity FROM search_pin"
    ),
    # The unfolded text `variable_fts` indexes, one row per built variable.
    "search_text": lambda o: [
        {**row, "delivery_column_names": json.loads(row["delivery_column_names"])}
        for row in o._sql(
            "SELECT r.slug AS register, v.slug AS variable, t.name, t.definition, "
            "t.description, t.delivery_column_names FROM variable_search_text t "
            "JOIN variable v USING (variable_id) "
            "JOIN register r ON r.register_id = v.register_id"
        )
    ],
    "manifest": lambda o: o._sql("SELECT key, value FROM import_manifest"),
    "edges": lambda o: o._sql(
        "SELECT 'same_as' AS type, "
        "a_provider || '/' || a_register || '/' || a_variable AS a, "
        "b_provider || '/' || b_register || '/' || b_variable AS b "
        "FROM variable_same_as UNION ALL SELECT 'replaced_by', "
        "predecessor_provider || '/' || predecessor_register || '/' "
        "|| predecessor_variable, "
        "successor_provider || '/' || successor_register || '/' || successor_variable "
        "FROM variable_replaced_by"
    ),
    # Every replaced_by edge below the classification grain, in one row shape.
    "successions": lambda o: o._sql(
        "SELECT 'register' AS grain, "
        f"{_REGISTER.format('predecessor')} AS predecessor, "
        f"{_REGISTER.format('successor')} AS successor, NULL AS predecessor_variant, "
        "NULL AS successor_variant, NULL AS predecessor_column, "
        "NULL AS successor_column, NULL AS variant, "
        f"{_REGISTER_SUCCESSION_FACTS} FROM register_replaced_by "
        "UNION ALL SELECT 'variable', "
        f"{_VARIABLE.format('predecessor')}, {_VARIABLE.format('successor')}, "
        f"NULL, NULL, NULL, NULL, NULL, {_SUCCESSION_FACTS} FROM variable_replaced_by "
        f"UNION ALL SELECT 'variant', {_REGISTER.format('predecessor')}, "
        f"{_REGISTER.format('successor')}, predecessor_variant, successor_variant, "
        "NULL, NULL, NULL, NULL, NULL, NULL FROM variant_replaced_by "
        f"UNION ALL SELECT 'representation', {_VARIABLE.format('predecessor')}, "
        f"{_VARIABLE.format('successor')}, NULL, NULL, predecessor_column, "
        f"successor_column, variant, {_SUCCESSION_FACTS} "
        "FROM representation_replaced_by"
    ),
    "classification_derivations": lambda o: o._sql(
        "SELECT derived_slug AS derived, source_slug AS source, note "
        "FROM classification_derived_from"
    ),
    "lineage": lambda o: o._sql(
        "SELECT c.register, c.variable, c.variant, c.column, "
        "s.register AS source_register, s.variable AS source_variable, "
        "s.variant AS source_variant, s.column AS source_column, "
        "l.valid_from, l.valid_to FROM variable_state_lineage l "
        f"JOIN ({_STATE_COORDINATES}) c ON c.state_id = l.consumer_state_id "
        f"JOIN ({_STATE_COORDINATES}) s ON s.state_id = l.source_state_id"
    ),
    "lineage_warnings": lambda o: o._sql(
        "SELECT c.register, c.variable, c.variant, c.column, c.valid_from, "
        "c.valid_to, w.warning_kind AS kind, w.message "
        "FROM variable_state_lineage_warning w "
        f"JOIN ({_STATE_COORDINATES}) c ON c.state_id = w.consumer_state_id"
    ),
    "state_classifications": lambda o: o._sql(
        "SELECT r.slug AS register, v.slug AS variable, "
        "s.delivery_column_name AS column, s.valid_from, s.valid_to, "
        "c.slug AS classification, sc.provenance "
        "FROM state_classification sc "
        "JOIN classification c ON c.id = sc.classification_id "
        "JOIN variable_state s ON s.state_id = sc.state_id "
        "JOIN variable v ON v.variable_id = s.variable_id "
        "JOIN register r ON r.register_id = v.register_id"
    ),
    "conformance": _conformance,
    "conformance_codes": _conformance_codes,
    # The reader's code-search index: each code a variable carries through a
    # state's or an alias window's value set, with the code's variable count.
    "code_index": lambda o: o._sql(
        "SELECT r.slug AS register, v.slug AS variable, vc.code, vc.label, "
        "vc.mapping_count FROM code_variable_map m "
        "JOIN value_code vc USING (code_id) JOIN variable v USING (variable_id) "
        "JOIN register r ON r.register_id = v.register_id"
    ),
    "value_sets": _value_sets,
    "classifications": lambda o: o._sql(
        "SELECT c.slug, c.short_name, c.name, c.name_en, c.publisher, c.valid_from, "
        "c.valid_to, c.description, c.url, c.code_count, c.valid_code_count, "
        "p.slug AS supersedes FROM classification c "
        "LEFT JOIN classification p ON p.id = c.supersedes_id"
    ),
    "classification_successions": lambda o: o._sql(
        "SELECT predecessor_slug AS predecessor, successor_slug AS successor, "
        "effective_year, note FROM classification_replaced_by"
    ),
    "classification_codes": lambda o: o._sql(
        "SELECT c.slug, v.code, v.label, cc.level, cc.is_valid, p.code AS parent "
        "FROM classification_code cc "
        "JOIN classification c ON c.id = cc.classification_id "
        "JOIN value_code v ON v.code_id = cc.code_id "
        "LEFT JOIN value_code p ON p.code_id = cc.parent_code_id"
    ),
    "relationships": lambda o: o._sql(
        "SELECT kind, binding_status, source_dataset, v.slug AS owner "
        "FROM source_relationship r "
        "LEFT JOIN variable v ON v.variable_id = r.owner_variable_id"
    ),
    "evidence": lambda o: [
        {"kind": event["evidence_kind"], "disposition": event["disposition"]}
        for event in o.events
        if event["kind"] == "prepared_evidence"
    ],
    "source_issues": _source_issues,
}


def sorted_rows(rows: list[list]) -> list[list]:
    return sorted(rows, key=lambda row: json.dumps(row, ensure_ascii=False))
