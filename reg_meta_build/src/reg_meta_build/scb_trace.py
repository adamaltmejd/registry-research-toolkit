"""Bounded diagnostic tracing of selected SCB catalog-version members.

The trace observes the current builder; it is not a source-of-truth or
contribution ledger.  It deliberately lives outside the catalog database and
retains only selected members plus the groups they intersect.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from reg_meta.errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Mapping, Sequence


_STATE_FIELDS = (
    "variable_id",
    "register_variant_id",
    "valid_from",
    "valid_to",
    "data_type",
    "data_length",
    "delivery_column_name",
    "source_register_text",
    "operational_definition",
    "provenance",
    "value_set_id",
    "value_set_version_label",
    "classification_id",
)


def _sortable_gkey(gkey: tuple[Any, ...]) -> tuple[str, ...]:
    return tuple("" if value is None else str(value) for value in gkey)


def _state_payload(row: Sequence[Any]) -> dict[str, Any]:
    payload = dict(zip(_STATE_FIELDS, row[1:14], strict=True))
    classification_id = payload["classification_id"]
    payload["state_id"] = row[0]
    payload["classification"] = (
        None
        if classification_id is None
        else {
            "id": classification_id,
            "short_name": row[14],
            "slug": row[15],
            "name": row[16],
        }
    )
    return payload


class ScbTraceCollector:
    """Collect one finite member→group→state diagnostic during a build."""

    def __init__(self, selected_cvids: tuple[int, ...], *, code_commit: str) -> None:
        self.selected_cvids = selected_cvids
        self.code_commit = code_commit
        self._intake_present: set[int] = set()
        self._cvid_gkey: dict[int, tuple[Any, ...]] = {}
        self._groups: dict[tuple[Any, ...], dict[str, Any]] = {}
        self._pending: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self._handles: list[dict[str, Any]] = []
        self._retired_before_overlap: set[int] = set()
        self._pre_overlap_checked = False
        self._finalized = False

    def record_intake(self, known_cvids: set[int]) -> None:
        self._intake_present = set(self.selected_cvids) & known_cvids

    def capture_groups(
        self,
        rows: Sequence[sqlite3.Row],
        groups: Mapping[tuple[Any, ...], Any],
        cvid_gkey: Mapping[int, tuple[Any, ...]],
        *,
        edition_claims: Callable[[int, str | None], Sequence[tuple[int, str, str]]],
    ) -> None:
        """Snapshot current post-enrichment staging before triage mutates it."""
        self._cvid_gkey = {
            cvid: gkey
            for cvid, gkey in cvid_gkey.items()
            if cvid in self.selected_cvids
        }
        selected_gkeys = set(self._cvid_gkey.values())
        ordered_gkeys = sorted(selected_gkeys, key=_sortable_gkey)
        group_ids = {
            gkey: f"group-{index:04d}"
            for index, gkey in enumerate(ordered_gkeys, start=1)
        }

        rows_by_group: dict[tuple[Any, ...], list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            gkey = cvid_gkey.get(row["cvid"])
            if gkey in selected_gkeys:
                rows_by_group[gkey].append(row)

        for gkey in ordered_gkeys:
            grp = groups[gkey]
            member_rows: dict[int, list[sqlite3.Row]] = defaultdict(list)
            for row in rows_by_group[gkey]:
                member_rows[row["cvid"]].append(row)
            members = []
            for cvid in sorted(member_rows):
                cvid_rows = member_rows[cvid]
                row = cvid_rows[0]
                claims = edition_claims(row["register_id"], row["registerversionnamn"])
                members.append(
                    {
                        "cvid": cvid,
                        "selected": cvid in self.selected_cvids,
                        "native_coordinates": {
                            "register_id": row["register_id"],
                            "register_variant_id": row["register_variant_id"],
                            "variable_id": row["var_id"],
                            "edition_id": row["regver_id"],
                            "edition_name": row["registerversionnamn"],
                        },
                        "edition_claim_limits": [
                            {"year": year, "valid_from": lo, "valid_to": hi}
                            for year, lo, hi in claims
                        ],
                        "staged_payload": {
                            "data_type": row["data_type"],
                            "data_length": row["data_length"],
                            "value_set_id": row["value_set_id"],
                            "value_set_version_label": row["value_set_version_label"],
                            "value_set_grain": row["grain"],
                            "operational_definition": row["operational_definition"],
                            "source_register_text": row["source_register_text"],
                            "provenance": row["provenance"],
                            "projected_owner_id": row["preset_variable_id"],
                            "aliases": sorted(
                                {
                                    item["delivery_column_name"]
                                    for item in cvid_rows
                                    if item["delivery_column_name"] is not None
                                }
                            ),
                        },
                    }
                )
            self._groups[gkey] = {
                "group_id": group_ids[gkey],
                "staging_membership": members,
                "staged_group": {
                    "grouping_key": {
                        "register_id": gkey[0],
                        "register_variant_id": gkey[1],
                        "variable_id": gkey[2],
                        "shape_data_type": gkey[3],
                        "shape_data_length": gkey[4],
                        "value_set_id": gkey[5],
                        "value_set_version_label": gkey[6],
                        "value_set_grain": gkey[7],
                        "column_component": gkey[8],
                        "source_register_text": gkey[9],
                        "projected_owner_id": gkey[10],
                    },
                    "resolved_payload": {
                        "data_type": grp.data_type,
                        "data_length": grp.data_length,
                        "delivery_column_name": grp.latest_alias,
                        "operational_definition": (
                            None
                            if grp.operational_definition_conflict
                            else grp.operational_definition
                        ),
                        "source_register_text": grp.source_register_text,
                        "value_set_id": grp.value_set_id,
                        "value_set_version_label": grp.value_set_version_label,
                    },
                    "edition_ids": sorted(grp.regvers),
                    "claim_limits": [
                        {
                            "year": year,
                            "valid_from": claim.lo,
                            "valid_to": claim.hi,
                            "authority": claim.authority,
                            "approval": claim.approval,
                        }
                        for year, claim in sorted(grp.claims.items())
                    ],
                    "unika": {
                        "matched": grp.unika_matched,
                        "first_year": grp.unika_min,
                        "last_year": grp.unika_max,
                        "open_top": grp.unika_has_open_top,
                    },
                },
                "assignment": None,
                "emissions": [],
            }

    def capture_assignments(
        self,
        assignments: Mapping[tuple[Any, ...], int | None],
        dropped: set[tuple[Any, ...]],
        labels: Mapping[tuple[Any, ...], str],
        clamped_to: Mapping[tuple[Any, ...], str],
    ) -> None:
        for gkey, group in self._groups.items():
            group["assignment"] = {
                "owner_variable_id": assignments.get(gkey),
                "dropped": gkey in dropped,
                "emitted_label_override": labels.get(gkey),
                "valid_to_clamp": clamped_to.get(gkey),
            }

    def record_emission(
        self, gkey: tuple[Any, ...], emitted_row: tuple[Any, ...]
    ) -> None:
        if gkey not in self._groups:
            return
        payload = dict(zip(_STATE_FIELDS[:-1], emitted_row, strict=True))
        payload["classification_id"] = None
        self._pending.append((gkey, payload))

    def bind_emissions(self, conn: sqlite3.Connection) -> None:
        """Bind actual batch rows to their inserted state IDs by the unique key."""
        for gkey, payload in self._pending:
            rows = conn.execute(
                "SELECT state_id FROM variable_state "
                "WHERE variable_id = ? AND register_variant_id = ? "
                "AND valid_from = ? AND value_set_version_label = ?",
                (
                    payload["variable_id"],
                    payload["register_variant_id"],
                    payload["valid_from"],
                    payload["value_set_version_label"],
                ),
            ).fetchall()
            if len(rows) != 1:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="scb_trace_emission_binding_failed",
                    error_class="diagnostic",
                    message=(
                        "SCB trace could not bind an emitted row to exactly one "
                        "state_id using idx_variable_state_unique."
                    ),
                    remediation=(
                        "This is a diagnostic bookkeeping error, not a formation "
                        "finding. Disable tracing or fix the trace hook before "
                        "interpreting the build."
                    ),
                )
            handle = {
                "group_id": self._groups[gkey]["group_id"],
                "state_id": rows[0][0],
                "emitted_payload": payload,
                "final_fate": None,
            }
            self._handles.append(handle)
            self._groups[gkey]["emissions"].append(handle)
        self._pending.clear()

    def mark_pre_overlap_liveness(self, conn: sqlite3.Connection) -> None:
        if self._pre_overlap_checked:
            return
        for handle in self._handles:
            state_id = handle["state_id"]
            if (
                conn.execute(
                    "SELECT 1 FROM variable_state WHERE state_id = ?", (state_id,)
                ).fetchone()
                is None
            ):
                self._retired_before_overlap.add(state_id)
        self._pre_overlap_checked = True

    @staticmethod
    def _read_state(conn: sqlite3.Connection, state_id: int) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT vs.state_id, vs.variable_id, vs.register_variant_id, "
            "vs.valid_from, vs.valid_to, vs.data_type, vs.data_length, "
            "vs.delivery_column_name, vs.source_register_text, "
            "vs.operational_definition, vs.provenance, vs.value_set_id, "
            "vs.value_set_version_label, vs.classification_id, "
            "c.short_name, c.slug, c.name "
            "FROM variable_state vs "
            "LEFT JOIN classification c ON c.id = vs.classification_id "
            "WHERE vs.state_id = ?",
            (state_id,),
        ).fetchone()
        return None if row is None else _state_payload(row)

    def capture_final(self, conn: sqlite3.Connection) -> None:
        if self._finalized:
            return
        self.mark_pre_overlap_liveness(conn)
        for handle in self._handles:
            state_id = handle["state_id"]
            current = self._read_state(conn, state_id)
            if state_id in self._retired_before_overlap:
                handle["final_fate"] = {
                    "status": "missing",
                    "final_state": None,
                    "changes": None,
                    "downstream_explanation": "unknown",
                    "missing_before_curated_overlap_resolution": True,
                    "same_numeric_id_context": (
                        None
                        if current is None
                        else {"context_only": True, "state": current}
                    ),
                }
                continue
            if current is None:
                handle["final_fate"] = {
                    "status": "missing",
                    "final_state": None,
                    "changes": None,
                    "downstream_explanation": "unknown",
                    "missing_before_curated_overlap_resolution": False,
                    "same_numeric_id_context": None,
                }
                continue
            emitted = handle["emitted_payload"]
            changes = {
                field: {"emitted": emitted[field], "final": current[field]}
                for field in _STATE_FIELDS
                if emitted[field] != current[field]
            }
            handle["final_fate"] = {
                "status": "modified" if changes else "unchanged",
                "final_state": current,
                "changes": changes,
                "downstream_explanation": None,
                "missing_before_curated_overlap_resolution": False,
                "same_numeric_id_context": None,
            }
        self._finalized = True

    def _owner_context(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        owner_ids = sorted(
            {
                assignment["owner_variable_id"]
                for group in self._groups.values()
                if (assignment := group["assignment"]) is not None
                and assignment["owner_variable_id"] is not None
            }
        )
        traced_handles = {handle["state_id"] for handle in self._handles}
        owners = []
        for owner_id in owner_ids:
            variable = conn.execute(
                "SELECT variable_id, register_id, provider_key, slug, name "
                "FROM variable WHERE variable_id = ?",
                (owner_id,),
            ).fetchone()
            states = []
            for (state_id,) in conn.execute(
                "SELECT state_id FROM variable_state WHERE variable_id = ? "
                "ORDER BY state_id",
                (owner_id,),
            ):
                state = self._read_state(conn, state_id)
                assert state is not None
                states.append(
                    {
                        "context_only": (
                            state_id not in traced_handles
                            or state_id in self._retired_before_overlap
                        ),
                        "state": state,
                    }
                )
            aliases = [
                {
                    "register_variant_id": row[0],
                    "delivery_column_name": row[1],
                }
                for row in conn.execute(
                    "SELECT register_variant_id, delivery_column_name "
                    "FROM variable_alias WHERE variable_id = ? "
                    "ORDER BY register_variant_id, delivery_column_name",
                    (owner_id,),
                )
            ]
            alias_windows = [
                {
                    "register_variant_id": row[0],
                    "delivery_column_name": row[1],
                    "valid_from": row[2],
                    "valid_to": row[3],
                    "provenance": row[4],
                }
                for row in conn.execute(
                    "SELECT register_variant_id, delivery_column_name, "
                    "valid_from, valid_to, provenance "
                    "FROM variable_alias_window WHERE variable_id = ? "
                    "ORDER BY register_variant_id, delivery_column_name, valid_from",
                    (owner_id,),
                )
            ]
            owners.append(
                {
                    "variable_id": owner_id,
                    "exists": variable is not None,
                    "variable": (
                        None
                        if variable is None
                        else {
                            "variable_id": variable[0],
                            "register_id": variable[1],
                            "provider_key": variable[2],
                            "slug": variable[3],
                            "name": variable[4],
                        }
                    ),
                    "final_states": states,
                    "final_aliases": aliases,
                    "final_alias_windows": alias_windows,
                }
            )
        return owners

    def report(self, conn: sqlite3.Connection) -> dict[str, Any]:
        self.capture_final(conn)
        selected = []
        for cvid in self.selected_cvids:
            if cvid not in self._intake_present:
                selected.append(
                    {
                        "cvid": cvid,
                        "outcome": "absent",
                        "present_at_intake": False,
                        "staged_group_id": None,
                        "owner_variable_id": None,
                        "dropped": False,
                        "emitted_state_ids": [],
                    }
                )
                continue
            gkey = self._cvid_gkey.get(cvid)
            if gkey is None:
                selected.append(
                    {
                        "cvid": cvid,
                        "outcome": "staging_unavailable",
                        "present_at_intake": True,
                        "staged_group_id": None,
                        "owner_variable_id": None,
                        "dropped": False,
                        "emitted_state_ids": [],
                    }
                )
                continue
            group = self._groups[gkey]
            assignment = group["assignment"] or {
                "owner_variable_id": None,
                "dropped": False,
            }
            emissions = group["emissions"]
            outcome = (
                "unassigned"
                if assignment["owner_variable_id"] is None
                else "dropped"
                if assignment["dropped"]
                else "emitted"
                if emissions
                else "not_emitted"
            )
            selected.append(
                {
                    "cvid": cvid,
                    "outcome": outcome,
                    "present_at_intake": True,
                    "staged_group_id": group["group_id"],
                    "owner_variable_id": assignment["owner_variable_id"],
                    "dropped": assignment["dropped"],
                    "emitted_state_ids": [item["state_id"] for item in emissions],
                }
            )

        return {
            "format": "scb-member-formation-trace",
            "selection": {"native_cvids": list(self.selected_cvids)},
            "evidence_layers": {
                "raw_observations": {
                    "included": False,
                    "comparison": (
                        "Use inspect-source-records with each member's native "
                        "coordinates and the exact code/input pins in this report."
                    ),
                },
                "staged": (
                    "Post-enrichment values from current first-wins intake; not "
                    "original physical observations."
                ),
                "emitted": (
                    "Rows actually appended by the current coalescer and bound to "
                    "their inserted state_id."
                ),
                "final": (
                    "Recorded state IDs inspected before build scratch was dropped; "
                    "assigned-owner rows and aliases are surrounding context."
                ),
            },
            "selected_members": selected,
            "groups": [
                self._groups[gkey] for gkey in sorted(self._groups, key=_sortable_gkey)
            ],
            "assigned_owner_context": self._owner_context(conn),
            "limits": [
                "Group membership and owner assignment do not prove field-level or window-level contribution.",
                "Projected missing values do not prove raw coding absence, and this trace does not infer absence versus explicit NULL from a final field.",
                "A missing state handle has unknown downstream fate; no row is recovered by name, slug, equal payload, or ownership stamp.",
                "Context-only final rows are not inferred replacements or contributions.",
                "The diagnostic observes current formation and neither repairs nor waives a conflict.",
            ],
        }


__all__ = ["ScbTraceCollector"]
