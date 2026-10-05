"""Artifact-derived independent admission census and reproducible binding requests.

Only aggregate receipts may be persisted; real identities stay in process memory.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass, replace

from reg_meta.catalog import Catalog


@dataclass(frozen=True)
class AcceptanceBinding:
    variable: str
    variant: str
    representation: str
    period: str
    physical_from: str | None
    physical_to: str | None
    strata: frozenset[str]

    def identity(self):
        return (
            self.variable,
            self.variant,
            self.representation,
            self.period,
            self.physical_from,
            self.physical_to,
        )

    def project(self, steward):
        return {
            "schema_version": "3.0.0",
            "steward": steward,
            "reg_meta_version": "conformance",
            "name": "Acceptance sample",
            "sources": [
                {
                    "name": "Sample",
                    "register_variant": self.variant,
                    "period": self.period,
                    "bindings": [
                        {
                            "variable": self.variable,
                            "type": "categorical",
                            "representation": self.representation,
                        }
                    ],
                }
            ],
        }


def identity_digest(identities):
    encoded = json.dumps(sorted(identities), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def admitted_bindings(conn, scope):
    """Independent whole-variable read admission from authored physical mappings."""
    query = """
        SELECT p.slug || '/' || r.slug || '/' || v.slug
        FROM variable v JOIN register r USING(register_id)
        JOIN provider p USING(provider_id)
        WHERE v.slug IS NOT NULL
    """
    if scope == "holdings":
        query += """
            AND v.variable_id IN (
                SELECT hm.variable_id FROM holding_mapping hm
                JOIN holding_column hc USING(column_id)
                JOIN holding_table ht USING(table_id) WHERE ht.scope != 'unknown'
            )
        """
    return {row[0] for row in conn.execute(query)}


def acceptance_sample(conn, generation, *, steward, size=50):
    """Rank independent delivery/physical intersections with generation SHA-256.

    Select one distinct binding from each available stratum, then fill in hash order.
    Public point resolution selects the applicable native spelling after proposal ranking.
    Counts returned describe raw proposals, not the entire eligible delivery universe.
    Unknown physical tables cannot supply a binding and are checked separately.
    """
    deliveries = defaultdict(list)
    spellings = defaultdict(set)
    for table, alias_window in (
        ("variable_state", False),
        ("variable_alias_window", True),
    ):
        for row in conn.execute(
            f"SELECT variable_id,register_variant_id,delivery_column_name,valid_from,valid_to FROM {table}"
            + (
                " WHERE period_scope IN ('intervals','year_independent')"
                if table == "variable_state"
                else ""
            )
        ):
            variable, variant, column, lo, hi = row
            if not column:
                continue
            key = (variable, variant, column.lower())
            spellings[key].add(column)
            deliveries[key].append((lo, hi, alias_window, column))
    for variable, variant, column in conn.execute(
        "SELECT variable_id,register_variant_id,delivery_column_name FROM variable_alias"
    ):
        spellings[(variable, variant, column.lower())].add(column)
    coordinates = {}
    for row in conn.execute("""
        SELECT v.variable_id,rv.register_variant_id,
               p.slug || '/' || r.slug || '/' || v.slug,
               p.slug || '/' || r.slug || '/' || rv.slug,v.name
        FROM variable v JOIN register r USING(register_id)
        JOIN provider p USING(provider_id) JOIN register_variant rv USING(register_id)
        WHERE v.slug IS NOT NULL AND rv.slug IS NOT NULL
    """):
        if row[4] and re.search(r"[^\W_]", row[4]):
            coordinates[(row[0], row[1])] = (row[2], row[3])
    if steward:
        facts = conn.execute("""
            SELECT hm.variable_id,hm.variant_id,hm.representation_canonical,
                   ht.scope,hp.lo,hp.hi,json_type(ht.edition_json),ht.partition,
                   hm.representation_literal
            FROM holding_mapping hm JOIN holding_column hc USING(column_id)
            JOIN holding_table ht USING(table_id) LEFT JOIN holding_period hp USING(table_id)
            WHERE ht.scope != 'unknown'
        """)
    else:
        facts = (
            (
                v,
                variant,
                column,
                "intervals" if lo is not None else "year_independent",
                lo,
                hi,
                None,
                None,
                column,
            )
            for (v, variant, _), windows in deliveries.items()
            for lo, hi, _, column in windows
        )
    candidates = {}
    for (
        variable,
        variant,
        column,
        scope,
        physical_lo,
        physical_hi,
        edition,
        partition,
        literal,
    ) in facts:
        coordinate = coordinates.get((variable, variant))
        if coordinate is None:
            continue
        key = (variable, variant, column.lower())
        for semantic_lo, semantic_hi, alias_window, semantic_column in deliveries.get(
            key, ()
        ):
            if scope == "year_independent":
                if semantic_lo is not None:
                    continue
                period = "_default"
            else:
                if any(
                    value is None
                    for value in (semantic_lo, semantic_hi, physical_lo, physical_hi)
                ):
                    continue
                lo, hi = max(semantic_lo, physical_lo), min(semantic_hi, physical_hi)
                if lo > hi or not "1900" <= lo[:4] <= "2099":
                    continue
                period = lo
            strata = set()
            if len(spellings[key]) > 1 or literal != column:
                strata.add("case_twin")
            if alias_window:
                strata.add("alias_window")
            if edition == "object":
                strata.add("range")
            if edition == "array":
                strata.add("list")
            if partition is not None:
                strata.add("partition")
            if not strata:
                strata.add("ordinary")
            candidate = AcceptanceBinding(
                *coordinate, column, period, physical_lo, physical_hi, frozenset(strata)
            )
            identity = candidate.identity()
            if identity in candidates:
                candidate = replace(
                    candidate, strata=candidates[identity].strata | candidate.strata
                )
            candidates[identity] = candidate

    def rank(candidate):
        encoded = json.dumps(
            candidate.identity(), ensure_ascii=False, separators=(",", ":")
        )
        return hashlib.sha256((generation + encoded).encode()).hexdigest()

    ranked = sorted(candidates.values(), key=rank)
    reference = Catalog(conn, scope="reference")
    resolved = {}

    def applicable(candidate):
        key = (candidate.variable, candidate.variant, candidate.period)
        if key not in resolved:
            resolved[key] = reference.resolve_at(
                candidate.variable,
                candidate.period,
                variant=candidate.variant.rsplit("/", 1)[1],
                with_codes=False,
            )
        native = sorted(
            {
                state.delivery_column_name
                for state in resolved[key]
                if state.delivery_column_name
                and state.delivery_column_name.lower()
                == candidate.representation.lower()
            }
        )
        return replace(candidate, representation=native[0]) if native else None

    sample = []
    selected = set()
    for stratum in (
        "case_twin",
        "alias_window",
        "range",
        "list",
        "partition",
        "ordinary",
    ):
        candidate = next(
            (
                eligible
                for c in ranked
                if stratum in c.strata and c.variable not in selected
                if (eligible := applicable(c)) is not None
            ),
            None,
        )
        if candidate is not None and len(sample) < size:
            sample.append(candidate)
            selected.add(candidate.variable)
    for candidate in ranked:
        if len(sample) >= size:
            break
        if candidate.variable not in selected:
            eligible = applicable(candidate)
            if eligible is not None:
                sample.append(eligible)
                selected.add(candidate.variable)
    return (
        sample,
        {
            stratum: sum(stratum in c.strata for c in ranked)
            for stratum in (
                "case_twin",
                "alias_window",
                "range",
                "list",
                "partition",
                "ordinary",
            )
        },
        len({c.variable for c in ranked}),
    )


def response_fqids(value):
    """Search groups may contain folded members; observe all navigable identities."""
    if isinstance(value, dict):
        result = {value["fqid"]} if isinstance(value.get("fqid"), str) else set()
        return result | set().union(
            *(response_fqids(child) for child in value.values())
        )
    if isinstance(value, list):
        return set().union(*(response_fqids(child) for child in value))
    return set()
