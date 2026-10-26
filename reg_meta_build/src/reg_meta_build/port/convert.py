"""Convert today's curation tree to the neutral verbs (`crates/reg-build-model`, §11).

    uv run python -m reg_meta_build.port.convert --prepared P --input-commit C \\
        --input-manifest-sha256 M --registers SPEC ... --out DIR

Writes `DIR/curation/`, a copy of the curation tree whose selected register files hold
the neutral verbs, and `DIR/convert-report.json`. Unselected register files are copied
unchanged: the Rust build reads only the selected registers' files. Deterministic: the
same tree and prepared root give the same bytes. It reads the prepared records of the
selected registers only, never the Python compile.

Each old entry becomes exactly one new entry (`legacy = { ref, head }`), except
`identity.route` entries with one destination, which D7 drops (the SoS adapter resolves
an unambiguous token). An old entry the converter cannot translate exactly is refused.
Deleted at phase 4, after its output is committed (`PORT.md`).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta_build._curation import fold_column, repo_curation_dir
from reg_meta_build.curation_compile import _bindable_split_literals, _naming_source_id
from reg_meta_build.pipeline import _selected_scopes
from reg_meta_build.port import emit
from reg_meta_build.port.select import (
    ConversionError,
    Member,
    Rec,
    Ref,
    RegisterRecords,
    exact_member,
)
from reg_meta_build.prepared_catalog import open_prepared_catalog_sources
from reg_meta_build.scb_errata import holdings_scope
from reg_meta_build.slug_grammar import derive_variable_slug
from reg_meta_build.source_coordinates import native_variable_key

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from reg_meta_build.prepared_sources import PreparedSourceRecords
    from reg_meta_build.source_coordinates import NativeKey

# Verbs the port leaves as they are; they keep their table and order, so their case ids
# stay derivable.
UNCHANGED = (
    "enrichment",
    "group",
    "code_label_pair",
    "acknowledge",
    "classification_timeline",
    "documentary",
)
# The old verb tables, in the order the old schema declares them.
OLD_VERBS = {
    "identity": (
        "partition",
        "unassigned",
        "column_owner",
        "route",
        "edition_split",
        "split",
        "rename",
    ),
    "errata": (
        "support",
        "field",
        "occurrence_period",
        "occurrence_carry",
        "flags",
        "delivered",
        "column",
        "version",
        "data_type",
        "classification_reference",
    ),
    "representation": (
        "matrix",
        "period_family",
        "delivery_metadata",
        "alias_window",
        "parallel",
    ),
    "coding": (
        "warning",
        "choice",
        "uncoded",
        "omit",
        "extend",
        "documented",
        "sentinel",
        "support",
    ),
}
# The new tables, in the order a converted file writes them.
NEW_TABLES = (
    "identity.assign",
    "table.assign",
    "errata.table",
    "errata.field",
    "errata.period",
    "errata.delivered",
    "errata.delivery",
    "errata.column",
    "errata.support",
    "representation.parallel",
    "representation.period_family",
    "representation.delivery_metadata",
    "representation.matrix",
    *(f"coding.{kind}" for kind in OLD_VERBS["coding"]),
)


def _drop(entry: dict[str, Any], *names: str) -> dict[str, Any]:
    return {k: v for k, v in entry.items() if k not in names}


def _guard(entry: dict[str, Any], *names: str, **renamed: Any) -> dict[str, Any] | None:
    guard = {name: entry[name] for name in names if entry.get(name) is not None}
    guard.update({k: v for k, v in renamed.items() if v is not None})
    return guard or None


def _legacy(case: str, head: str | None = None) -> dict[str, str]:
    return {"ref": case} if head is None else {"ref": case, "head": head}


@dataclass
class Register:
    """One register curation file and the prepared records it addresses."""

    file: str
    """`curation/registers/<provider>/<name>.toml`, as case ids name it."""
    raw: dict[str, Any]
    index: RegisterRecords
    variable_slugs: dict[str, str]
    variant_slugs: dict[str, str]
    non_neutral: list[dict[str, Any]] = field(default_factory=list)
    refused: list[str] = field(default_factory=list)
    added: list[dict[str, Any]] = field(default_factory=list)
    """Members that address occurrences only additions create (no prepared record)."""
    scoped: set[Ref] | None = None
    edition_names: dict[str, str] | None = None
    """Old entries the converter could not translate exactly; the file is not written."""

    # ── old coordinates ────────────────────────────────────────────────────────

    def slug(self, naming_id: str, *, case: str) -> str:
        if (slug := self.variable_slugs.get(naming_id)) is None:
            raise ConversionError(f"{case}: owner {naming_id!r} has no pinned slug")
        return slug

    def table(self, variant: str, *, case: str) -> str:
        """The table slug an old `variant` / `deldatamangd` names, records or not."""
        if variant in self.variant_slugs:
            return self.variant_slugs[variant]
        if variant in self.variant_slugs.values():
            return variant
        # A thin provider's `<register>.<authored key>`; the key is the table slug.
        local = variant.removeprefix(f"{self.index.register_id}.")
        if local != variant and local in self.variant_slugs.values():
            return local
        tables = {
            table
            for rec in self.index.records
            if rec.token == variant
            for table in self.index.tables(rec, source=True)
        }
        if len(tables) != 1:
            raise ConversionError(
                f"{case}: variant {variant!r} names {len(tables)} tables"
            )
        return tables.pop()

    def _variable(self, variable: str) -> Callable[[Rec], bool]:
        local = f"{self.index.register_id}.{variable}"
        return lambda rec: (
            rec.variable_id in (variable, local)
            or variable.startswith(rec.variable_id + ".")
        )

    def _variant(self, variant: str, *, split: bool) -> Callable[[Rec], bool]:
        slug = self.variant_slugs.get(variant, variant)
        if split:
            # After `table.assign`: the deliveries a split moved away are not the
            # variant's any more (the coding verbs address compiled variants).
            return lambda rec: slug in self.index.tables(rec)
        return lambda rec: (
            rec.token == variant
            or rec.variant_id == variant
            or (
                rec.variant_id is not None
                and self.variant_slugs.get(rec.variant_id) == slug
            )
        )

    def old(
        self,
        *,
        variable: str | None = None,
        variant: str | None = None,
        column: str | None = None,
        editions: Iterable[str] | None = None,
        split: bool = False,
        fold: bool = False,
    ) -> list[Rec]:
        """The records an old entry's coordinates name. An old `variant` is the
        source's own, unless `split` (after edition splits). `fold` matches the column
        case-insensitively, where Python does (`fold_column`)."""
        # simplify: a scan of the register's records per old entry (the 21 golden-corpus
        # registers convert in 45 s); index by variable if the full tree is too slow.
        tests = []
        if variable is not None:
            tests.append(self._variable(variable))
        if variant is not None:
            tests.append(self._variant(variant, split=split))
        if column is not None and fold and column:
            folded = fold_column(column)
            tests.append(
                lambda rec: rec.column is not None and fold_column(rec.column) == folded
            )
        elif column is not None:
            tests.append(lambda rec: rec.column == (column or None))
        if editions is not None:
            wanted = set(editions)
            tests.append(
                lambda rec: rec.delivery in wanted or str(rec.edition_id) in wanted
            )
        return [rec for rec in self.index.records if all(test(rec) for test in tests)]

    def deliveries(self, editions: Iterable[str]) -> list[str]:
        """Old edition coordinates (names or SCB RegVerIDs) as delivery names, in order."""
        if self.edition_names is None:
            self.edition_names = {
                str(rec.edition_id): rec.delivery
                for rec in self.index.records
                if rec.edition_id is not None and rec.delivery is not None
            }
        by_id = self.edition_names
        out = []
        for edition in editions:
            name = by_id.get(edition, edition)
            if name not in out:
                out.append(name)
        return out

    def period_text(self, text: Any, deliveries: list[str]) -> dict[str, str] | None:
        """An old `expected_period_text`, keyed by delivery name."""
        if text is None:
            return None
        if isinstance(text, dict):
            return {self.deliveries([k])[0]: v for k, v in text.items()}
        if len(deliveries) != 1:
            raise ConversionError("a period text needs exactly one delivery")
        return {deliveries[0]: text}

    # ── neutral members ────────────────────────────────────────────────────────

    def members(
        self,
        target: list[Rec],
        *,
        case: str,
        table: bool,
        column: str | None,
        deliveries: list[str] | None = None,
        source: bool = False,
        allow_empty: bool = False,
    ) -> list[Member]:
        """Members that select exactly `target`: one per table the records are on, each
        narrowed by `where` where the translated coordinates alone select more.

        Where no stated field separates them, the member names the records' table,
        then their deliveries too: coordinates the old entry implied (a family is on
        its own tables) but did not state."""
        if not target:
            if allow_empty:
                return []
            raise ConversionError(f"{case}: the old entry selects no record")
        try:
            return self._members(target, case, table, column, deliveries, source)
        except ConversionError:
            if not table:
                table = True
            elif deliveries is None and all(rec.delivery for rec in target):
                return [
                    member
                    for delivery in sorted({rec.delivery or "" for rec in target})
                    for member in self._members(
                        [rec for rec in target if rec.delivery == delivery],
                        case,
                        True,
                        column,
                        [delivery],
                        source,
                    )
                ]
            else:
                raise
        return self.members(
            target,
            case=case,
            table=table,
            column=column,
            deliveries=deliveries,
            source=source,
        )

    def _members(
        self,
        target: list[Rec],
        case: str,
        table: bool,
        column: str | None,
        deliveries: list[str] | None,
        source: bool,
    ) -> list[Member]:
        # A member names the delivered literal: an old column may differ from it in
        # case where Python folds (`fold_column`).
        groups: dict[tuple[str | None, str | None], list[Rec]] = {}
        for rec in target:
            tables = self.index.tables(rec, source=source)
            key = tables[0] if table and len(tables) == 1 else None
            groups.setdefault((key, rec.column if column else None), []).append(rec)
        out = []
        for key, literal in sorted(
            groups, key=lambda k: tuple((part is None, part or "") for part in k)
        ):
            member = Member(
                table=key,
                column=literal,
                deliveries=tuple(deliveries) if deliveries is not None else None,
            )
            member, narrowed = exact_member(
                self.index, member, groups[key, literal], case=case, source=source
            )
            if narrowed:
                self.non_neutral.append({"case": case, "member": member.as_toml()})
            out.append(member)
        return out

    def member(self, target: list[Rec], *, case: str, **kwargs: Any) -> Member:
        members = self.members(target, case=case, **kwargs)
        tables = {member.table for member in members}
        if len(members) > 1 and len(tables) == 1 and None not in tables:
            # One table, several case spellings of the column (Python folds them):
            # a member without `column`, narrowed by the stated fields.
            (table,) = tables
            member, _ = exact_member(
                self.index,
                Member(table=table, deliveries=members[0].deliveries),
                target,
                case=case,
                source=kwargs.get("source", False),
            )
            self.non_neutral.append({"case": case, "member": member.as_toml()})
            return member
        if len(members) != 1:
            raise ConversionError(f"{case}: the old entry spans {len(members)} tables")
        return members[0]

    def addition(
        self,
        *,
        variant: str,
        column: str,
        case: str,
        deliveries: list[str] | None = None,
        period: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """A member for records the source does not have: the coordinates as named."""
        return Member(
            table=self.table(variant, case=case),
            column=column or None,
            deliveries=tuple(deliveries) if deliveries is not None else None,
            period=period,
        ).as_toml()


# ── identity ──────────────────────────────────────────────────────────────────


def _scoped(reg: Register) -> set[Ref]:
    """The records the file's `identity.column_owner` entries own: a partition's
    literal map does not apply to them."""
    if reg.scoped is not None:
        return reg.scoped
    out: set[Ref] = set()
    for entry in reg.raw.get("identity", {}).get("column_owner", []):
        out |= {
            rec.ref
            for rec in reg.old(
                variable=entry["variable"],
                variant=entry["variant"],
                column=entry["column"],
                editions=entry.get("source_editions"),
            )
        }
    reg.scoped = out
    return out


def _partition(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    scoped = _scoped(reg)
    family = [
        rec for rec in reg.old(variable=entry["variable"]) if rec.ref not in scoped
    ]
    by_owner: dict[str | bool, list[str]] = {}
    for literal, owner in entry["columns"].items():
        by_owner.setdefault(reg.slug(owner, case=case), []).append(literal)
    if entry.get("unassigned_columns"):
        by_owner[False] = list(entry["unassigned_columns"])
    links = []
    for owner, literals in by_owner.items():
        members = []
        for literal in literals:
            target = [rec for rec in family if rec.column == literal]
            members += reg.members(
                target, case=case, table=False, column=literal, allow_empty=True
            )
        if members:
            links.append({"owner": owner, "members": [m.as_toml() for m in members]})
    return "identity.assign", {
        "legacy": _legacy(case),
        "ref": entry.get("columns_ref"),
        "data_warning": entry.get("data_warning"),
        "guard": _guard(entry, "expected_evidence_sha256"),
        "link": links,
    }


def _unassigned(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    family = reg.old(variable=entry["variable"])
    literals = sorted(
        {rec.column for rec in family}, key=lambda c: (c is None, c or "")
    )
    members = []
    for literal in literals:
        target = [rec for rec in family if rec.column == literal]
        members += reg.members(target, case=case, table=False, column=literal)
    return "identity.assign", {
        "legacy": _legacy(case),
        "evidence": entry.get("evidence"),
        "guard": _guard(entry, "expected_evidence_sha256"),
        "link": [{"owner": False, "members": [m.as_toml() for m in members]}],
    }


def _column_owner(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    editions = entry.get("source_editions")
    target = reg.old(
        variable=entry["variable"],
        variant=entry["variant"],
        column=entry["column"],
        editions=editions,
    )
    members = reg.members(
        target,
        case=case,
        table=True,
        column=entry["column"],
        deliveries=reg.deliveries(editions) if editions else None,
    )
    return "identity.assign", {
        "legacy": _legacy(case),
        "ref": entry.get("ref"),
        "data_warning": entry.get("data_warning"),
        "guard": _guard(
            entry,
            "expected_evidence_sha256",
            "expected_fields_sha256",
            "expected_fields",
        ),
        "link": [
            {
                "owner": reg.slug(entry["owner"], case=case),
                "members": [m.as_toml() for m in members],
            }
        ],
    }


def _route(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict] | None:
    if len(entry["variants"]) == 1:
        return None  # D7: the adapter resolves a token with one destination.
    tables = sorted(
        {
            table
            for rec in reg.index.records
            if rec.token == entry["deldatamangd"]
            for table in reg.index.tables(rec, source=True)
        }
    )
    return "table.assign", {
        "legacy": _legacy(case),
        "tables": tables,
        "source_token": entry["deldatamangd"],
    }


def _edition_split(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    target = reg.old(variant=entry["variant"], editions=entry["editions"])
    deliveries = reg.deliveries(entry["editions"])
    members = reg.members(
        target, case=case, table=True, column=None, deliveries=deliveries, source=True
    )
    return "table.assign", {
        "legacy": _legacy(case),
        "tables": [reg.variant_slugs[entry["split"]]],
        "members": [m.as_toml() for m in members],
        "evidence": entry.get("evidence"),
        "noted": entry.get("noted"),
        "guard": {"expected_deliveries": reg.deliveries(entry["source_editions"])},
    }


_SPLIT_FIELDS = ("data_type", "name", "description")


def _split(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    family = reg.old(variable=entry["variable"], column=entry["variable"])
    by = entry["by"]
    links = []
    for part in entry["parts"]:
        if by == "deldatamangd":
            target = [rec for rec in family if rec.token == part["deldatamangd"]]
            members = reg.members(
                target, case=case, table=True, column=entry["variable"]
            )
        else:
            target = [rec for rec in family if rec.stated(by) == part[by]]
            member, _ = exact_member(
                reg.index,
                Member(column=entry["variable"], where=((by, part[by]),)),
                target,
                case=case,
            )
            members = [member]
        links.append(
            {
                "owner": reg.slug(part["owner"], case=case),
                "members": [m.as_toml() for m in members],
            }
        )
    return "identity.assign", {
        "legacy": _legacy(case),
        "data_warning": entry.get("data_warning"),
        "guard": _guard(entry, "expected_evidence_sha256", "expected_records"),
        "link": links,
    }


def _rename(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    target = [
        rec
        for rec in reg.old(
            variant=entry["deldatamangd"],
            variable=entry["variable"],
            column=entry["variable"],
        )
        if rec.stated("name") == entry["name"]
    ]
    member = reg.member(target, case=case, table=True, column=entry["variable"])
    owner = f"{reg.index.register_id}.{entry['column']}"
    return "identity.assign", {
        "legacy": _legacy(case),
        "link": [
            {
                "owner": reg.slug(owner, case=case),
                "members": [member.as_toml()],
                "column_name": entry["column"],
            }
        ],
    }


# ── errata ────────────────────────────────────────────────────────────────────


def _alternative(target: list[Rec], expected: Iterable[dict[str, Any]]) -> list[Rec]:
    """Where alternative records share a ref, the one the entry's `expected_fields`
    describe: Python's guard pins that alternative."""
    by_ref: dict[Ref, list[Rec]] = {}
    for rec in target:
        by_ref.setdefault(rec.ref, []).append(rec)
    if all(len(recs) == 1 for recs in by_ref.values()):
        return target

    def described(rec: Rec) -> bool:
        return all(
            rec.stated(item["name"])
            == (item.get("value") if item["status"] == "value" else None)
            for item in expected
            if item["name"] in Rec.FIELDS
        )

    return [
        rec
        for recs in by_ref.values()
        for rec in (recs if len(recs) == 1 else [r for r in recs if described(r)])
    ]


def _occurrence(
    reg: Register, entry: dict[str, Any], case: str, editions: list[str] | None
) -> tuple[Member, list[str] | None]:
    target = reg.old(
        variable=entry["variable"],
        variant=entry["variant"],
        column=entry["column"],
        editions=editions,
    )
    target = _alternative(target, entry.get("expected_fields") or ())
    deliveries = reg.deliveries(editions) if editions is not None else None
    member = reg.member(
        target, case=case, table=True, column=entry["column"], deliveries=deliveries
    )
    return member, deliveries


def _field(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    member, deliveries = _occurrence(reg, entry, case, entry.get("editions"))
    name, value = entry["field"], entry.get("value")
    if entry.get("clear"):
        if value is not None:
            raise ConversionError(f"{case}: clear with a value")
        change: dict[str, Any] = {"clear": [name]}
    elif value is None:
        raise ConversionError(f"{case}: neither a value nor clear")
    else:
        change = {"set": {name: value}}
    return "errata.field", {
        "legacy": _legacy(case, "errata:field"),
        "member": member.as_toml(),
        **change,
        "evidence": entry["evidence"],
        "noted": entry["noted"],
        "guard": _guard(
            entry,
            "expected_fields",
            "expected_scope",
            "expected_period",
            "expected_records",
            "authority_records",
            "expected_evidence_sha256",
            expected_period_text=reg.period_text(
                entry.get("expected_period_text"), deliveries or []
            ),
        ),
    }


def _flags(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    member, deliveries = _occurrence(reg, entry, case, [entry["edition"]])
    return "errata.field", {
        "legacy": _legacy(case, "errata:flags"),
        "member": member.as_toml(),
        "set": {
            "sensitivity": entry["is_sensitive"],
            "identifier": entry["is_identifier"],
        },
        "evidence": entry["evidence"],
        "noted": entry["noted"],
        "guard": _guard(
            entry,
            "expected_fields",
            expected_period_text=reg.period_text(
                entry.get("expected_period_text"), deliveries or []
            ),
            expected_scope=entry.get("expected_scope"),
            expected_period=entry.get("expected_period"),
            expected_evidence_sha256=entry.get("expected_evidence_sha256"),
        ),
    }


def _occurrence_period(
    reg: Register, entry: dict[str, Any], case: str
) -> tuple[str, dict]:
    editions = [entry["edition"]] if entry.get("edition") else None
    member, deliveries = _occurrence(reg, entry, case, editions)
    return "errata.period", {
        "legacy": _legacy(case, "errata:occurrence_period"),
        "member": member.as_toml(),
        "scope": entry["edition_scope"],
        "period": entry["edition_period_scope"],
        "authority": entry.get("authority") or None,
        "evidence": entry["evidence"],
        "noted": entry["noted"],
        "guard": _guard(
            entry,
            "expected_fields",
            expected_period_text=reg.period_text(
                entry.get("expected_period_text"), deliveries or []
            ),
            expected_scope=entry.get("expected_scope"),
            expected_period=entry.get("expected_period"),
        ),
    }


def _occurrence_carry(
    reg: Register, entry: dict[str, Any], case: str
) -> tuple[str, dict]:
    member, _ = _occurrence(reg, entry, case, None)
    return "errata.delivered", {
        "legacy": _legacy(case, "errata:occurrence_carry"),
        "member": {**member.as_toml(), "period": entry["carry_scope"]},
        "data_warning": entry.get("data_warning"),
        "evidence": entry["evidence"],
        "noted": entry["noted"],
        "guard": _guard(entry, "expected_fields", "expected_scope", "expected_period"),
    }


def _delivered(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    member = reg.addition(
        variant=entry["variant"],
        column=entry["column"],
        deliveries=list(entry["versions"]),
        case=case,
    )
    donor = owner = None
    native = entry.get("native_variable_id")
    if entry.get("donor_variant"):
        target = [
            rec
            for rec in reg.old(
                variant=entry["donor_variant"], column=entry["column"], fold=True
            )
            if native is None or rec.variable_id == f"{reg.index.register_id}.{native}"
        ]
        donor = reg.member(target, case=case, table=True, column=entry["column"])
        donor = donor.as_toml()
    elif native is not None:
        # The member's own table documents the column in other deliveries; it needs an
        # owner only where that column is documented under several native variables.
        folded = fold_column(entry["column"])
        documented = {
            rec.variable_id
            for rec in reg.old(variant=entry["variant"])
            if rec.column is not None and fold_column(rec.column) == folded
        }
        if documented != {f"{reg.index.register_id}.{native}"}:
            owner = reg.slug(f"{reg.index.register_id}.{native}", case=case)
    upstream = entry.get("upstream")
    return "errata.delivered", {
        "legacy": _legacy(case, f"errata:{upstream or 'omitted-column-in-version'}"),
        "member": member,
        "donor": donor,
        "owner": owner,
        "donor_coding": entry.get("donor_coding") or None,
        "data_type": entry.get("data_type"),
        "measurement_unit": entry.get("measurement_unit"),
        "data_warning": entry.get("data_warning"),
        "upstream": upstream,
        "guard": _guard(entry, "expected_evidence_sha256"),
        "evidence": entry["evidence"],
        "noted": entry["noted"],
    }


def _column(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    period = None
    if entry.get("holdings_period"):
        period = holdings_scope(entry["holdings_period"]).model_dump(
            mode="json", exclude_none=True, exclude_defaults=True
        )
        period["kind"] = holdings_scope(entry["holdings_period"]).kind
    member = reg.addition(
        variant=entry["variant"],
        column=entry["column"],
        deliveries=list(entry["versions"]) if entry.get("versions") else None,
        period=period,
        case=case,
    )
    return "errata.column", {
        "legacy": _legacy(case, f"errata:{entry['source']}"),
        "member": member,
        "all_deliveries": entry.get("all_versions") or None,
        **{
            name: entry.get(name)
            for name in (
                "name",
                "definition",
                "data_type",
                "measurement_unit",
                "operational_definition",
                "classification",
                "is_identifier",
                "is_sensitive",
                "data_warning",
                "source",
                "evidence",
                "noted",
            )
        },
    }


def _version(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    return "errata.delivery", {
        "legacy": _legacy(case),
        "table": reg.table(entry["variant"], case=case),
        "delivery": entry["name"],
        "evidence": entry["evidence"],
        "noted": entry["noted"],
    }


def _support(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    member, deliveries = _occurrence(reg, entry, case, [entry["edition"]])
    authority = None
    if (old := entry.get("authority")) is not None:
        a_member, a_deliveries = _occurrence(
            reg,
            old,
            f"{case} authority",
            [old["edition"]] if old.get("edition") else None,
        )
        authority = {
            "member": a_member.as_toml(),
            "evidence": old.get("evidence"),
            "noted": old.get("noted"),
            "guard": _guard(
                old,
                "expected_fields",
                "expected_scope",
                "expected_period",
                expected_period_text=reg.period_text(
                    old.get("expected_period_text"), a_deliveries or []
                ),
            ),
        }
    replacement = entry.get("replacement")
    return "errata.support", {
        "legacy": _legacy(case, "errata:support"),
        "member": member.as_toml(),
        # The old default (`curation_tree.ErrataSupportEntry`), stated.
        "kind": entry.get("kind", "contradictory_question"),
        "replacement": reg.slug(replacement, case=case) if replacement else None,
        "evidence": entry["evidence"],
        "noted": entry["noted"],
        "guard": _guard(
            entry,
            "expected_fields",
            "expected_scope",
            "expected_period",
            expected_period_text=reg.period_text(
                entry.get("expected_period_text"), deliveries or []
            ),
            expected_coding_sha256=entry.get("expected_coding_sha256"),
        ),
        "authority": authority,
    }


def _sos_field(
    reg: Register, entry: dict[str, Any], case: str, head: str, change: dict, *guards
) -> tuple[str, dict]:
    target = reg.old(
        variant=entry["deldatamangd"],
        variable=entry["variable"],
        column=entry["column"],
    )
    member = reg.member(target, case=case, table=True, column=entry["column"])
    return "errata.field", {
        "legacy": _legacy(case, head),
        "member": member.as_toml(),
        **change,
        "guard": _guard(entry, *guards),
        "evidence": entry["evidence"],
        "noted": entry["noted"],
    }


def _data_type(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    return _sos_field(
        reg,
        entry,
        case,
        "errata:data_type",
        {"set": {"data_type": entry["data_type"]}},
        "expected_type",
        "expected_representation",
    )


def _classification_reference(
    reg: Register, entry: dict[str, Any], case: str
) -> tuple[str, dict]:
    return _sos_field(
        reg,
        entry,
        case,
        "errata:classification_reference",
        {"unknown": ["classification_declared"]},
        "expected_reference",
        "expected_representation",
        "expected_description",
    )


# ── representation ────────────────────────────────────────────────────────────


def _parallel(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    return "representation.parallel", {
        "legacy": _legacy(case),
        "owner": reg.slug(entry["variable"], case=case),
        "table": reg.table(entry["variant"], case=case),
        "columns": [
            {
                "column": column["column"],
                "deliveries": reg.deliveries(column.get("source_editions", ())),
                "valid_from": column.get("valid_from"),
                "valid_to": column.get("valid_to"),
                "expected_codings": column.get("expected_codings"),
            }
            for column in entry["columns"]
        ],
        **{
            name: entry.get(name)
            for name in (
                "valid_from",
                "valid_to",
                "co_delivered",
                "case_aliases",
                "column_metadata",
                "coding_metadata",
            )
        },
        "guard": _guard(entry, "expected_evidence_sha256"),
        "evidence": entry.get("evidence"),
        "noted": entry.get("noted"),
    }


def _alias_window(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    provider, register, slug = entry["variable"].split("/")
    if f"{provider}/{register}" != reg.file.removeprefix(
        "curation/registers/"
    ).removesuffix(".toml"):
        raise ConversionError(f"{case}: alias window of another register")
    return "representation.parallel", {
        "legacy": _legacy(case, "curated:alias-window"),
        "owner": slug,
        "table": reg.table(entry["variant"], case=case),
        "columns": [
            {
                "column": entry["column"],
                "deliveries": reg.deliveries(entry["source_editions"]),
            }
        ],
        "alias_window": True,
        "evidence": entry["evidence"],
        "noted": entry["noted"],
    }


def _period_family(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    return "representation.period_family", {
        "legacy": _legacy(case, "curated:representation.period_family"),
        "family_stem": entry["family_stem"],
        "label": entry["label"],
        "slug": entry["slug"],
        "guard": _guard(entry, "expected_definitions"),
    }


def _delivery_metadata(
    reg: Register, entry: dict[str, Any], case: str
) -> tuple[str, dict]:
    columns = []
    for column in entry["columns"]:
        variant = _naming_source_id("register_variant", tuple(column["variant_key"]))
        table = reg.variant_slugs.get(variant)
        if table is None:
            raise ConversionError(f"{case}: variant {variant} has no pinned slug")
        literal = column["column"]
        target = [
            rec
            for rec in reg.old(variable=entry["variable"], column=literal)
            if table in reg.index.tables(rec)
        ]
        member = reg.member(target, case=case, table=True, column=literal)
        columns.append(
            {
                "member": member.as_toml(),
                "valid_from": column["valid_from"],
                "valid_to": column["valid_to"],
                "expected_codings": column.get("expected_codings"),
                "guard": _guard({}, expected_scope=column.get("source_scope")),
            }
        )
    return "representation.delivery_metadata", {
        "legacy": _legacy(case),
        "owner": reg.slug(entry["variable"], case=case),
        "fields": entry["fields"],
        "columns": columns,
        "support": entry.get("support") or None,
        "evidence": entry["evidence"],
        "noted": entry["noted"],
        "guard": _guard({}, expected_records=entry.get("records") or None),
    }


def _matrix(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
    selector = entry["selector"]
    variant = f"{selector['register_id']}.{selector['register_variant_id']}"
    target = reg.old(
        variable=f"{selector['register_id']}.{selector['var_id']}",
        variant=variant,
        editions=[str(selector["regver_id"])],
    )
    deliveries = reg.deliveries([str(selector["regver_id"])])
    member = reg.member(
        target, case=case, table=True, column=None, deliveries=deliveries
    )
    return "representation.matrix", {
        "legacy": _legacy(case, "curated:matrix-answer"),
        "member": member.as_toml(),
        "source_mode": entry["source_mode"],
        "evidence_file": entry["evidence_file"],
        "guard": {"expected_selector": selector},
    }


# ── coding ────────────────────────────────────────────────────────────────────

_CODING_ADDRESS = ("variable", "variant", "column", "periods", "reason", "source")


def _coding(kind: str) -> Callable[[Register, dict[str, Any], str], tuple[str, dict]]:
    def convert(reg: Register, entry: dict[str, Any], case: str) -> tuple[str, dict]:
        target = reg.old(
            variable=entry["variable"],
            variant=entry["variant"],
            column=entry["column"],
            split=True,
            fold=kind == "extend",
        )
        if target:
            member = reg.member(target, case=case, table=True, column=entry["column"])
        else:
            # An occurrence an `errata.column` or `errata.delivered` entry adds: the
            # coding applies after the additions, so the member names it as stated.
            member = Member(
                table=reg.table(entry["variant"], case=case), column=entry["column"]
            )
            reg.added.append({"case": case, "member": member.as_toml()})
        payload = _drop(entry, *_CODING_ADDRESS, "expected_evidence_sha256")
        return f"coding.{kind}", {
            "legacy": _legacy(case, f"curated:coding.{kind}"),
            "member": member.as_toml(),
            "periods": entry.get("periods") or None,
            "reason": entry["reason"],
            "source": entry["source"],
            "guard": _guard(entry, "expected_evidence_sha256"),
            "payload": payload or None,
        }

    return convert


CONVERTERS: dict[str, Callable[[Register, dict[str, Any], str], Any]] = {
    "identity.partition": _partition,
    "identity.unassigned": _unassigned,
    "identity.column_owner": _column_owner,
    "identity.route": _route,
    "identity.edition_split": _edition_split,
    "identity.split": _split,
    "identity.rename": _rename,
    "errata.support": _support,
    "errata.field": _field,
    "errata.occurrence_period": _occurrence_period,
    "errata.occurrence_carry": _occurrence_carry,
    "errata.flags": _flags,
    "errata.delivered": _delivered,
    "errata.column": _column,
    "errata.version": _version,
    "errata.data_type": _data_type,
    "errata.classification_reference": _classification_reference,
    "representation.matrix": _matrix,
    "representation.period_family": _period_family,
    "representation.delivery_metadata": _delivery_metadata,
    "representation.alias_window": _alias_window,
    "representation.parallel": _parallel,
    **{f"coding.{kind}": _coding(kind) for kind in OLD_VERBS["coding"]},
}


# ── files ─────────────────────────────────────────────────────────────────────


def read_pins(raw: dict[str, Any]) -> dict[str, str]:
    return {
        key: value if isinstance(value, str) else value["slug"]
        for key, value in raw.get("variable", {}).items()
    }


def _split_pin_partitions(reg: Register) -> list[dict[str, Any]]:
    """SCB families whose split pins (`<reg>.<var>.<split>`) own columns with no
    `identity.partition` entry: Python binds each literal whose derived slug is a
    split's suffix (`curation_compile._column_partition_plan`). The converter states
    those literals, so nothing infers ownership from a column's spelling."""
    raw_identity = reg.raw.get("identity", {})
    declared = {
        entry["variable"]
        for verb in ("partition", "unassigned")
        for entry in raw_identity.get(verb, [])
    }
    scoped = _scoped(reg)
    families: dict[str, list[Rec]] = {}
    for rec in reg.index.records:
        native = native_variable_key(rec.record)
        if native is not None and native[1] == "scb":
            families.setdefault(rec.variable_id, []).append(rec)
    out = []
    for family_id in sorted(families):
        if family_id in declared:
            continue
        split_ids = sorted(
            key
            for key in reg.variable_slugs
            if key.startswith(family_id + ".") and key[len(family_id) + 1 :]
        )
        if not split_ids:
            continue
        family = families[family_id]
        columns: dict[str, list] = {}
        for rec in family:
            if rec.column is not None:
                columns.setdefault(rec.column, []).append(rec.record)
        scoped_columns = {rec.column for rec in family if rec.ref in scoped}
        candidates: dict[str, list[str]] = {}
        for column in sorted(columns):
            if column not in scoped_columns:
                candidates.setdefault(derive_variable_slug(column) or "x", []).append(
                    column
                )
        case = f"{reg.file}#/variable/{family_id}"
        links = []
        for split_id in split_ids:
            literals = tuple(candidates.get(split_id[len(family_id) + 1 :], ()))
            if not _bindable_split_literals(literals, columns):
                continue
            members = []
            for literal in literals:
                target = [rec for rec in family if rec.column == literal]
                members += reg.members(target, case=case, table=False, column=literal)
            links.append(
                {
                    "owner": reg.slug(split_id, case=case),
                    "members": [m.as_toml() for m in members],
                }
            )
        if links:
            out.append({"legacy": _legacy(case), "link": links})
    return out


def convert_register(reg: Register) -> tuple[str, dict[str, int]]:
    """The converted file's text and the count of entries per old verb."""
    raw = reg.raw
    new: dict[str, list[dict[str, Any]]] = {table: [] for table in NEW_TABLES}
    counts: dict[str, int] = {}
    variants = []
    for variant in raw.get("variant", []):
        if "name" in variant:
            new["errata.table"].append(
                {"table": variant["slug"], "name": variant["name"]}
            )
            variant = _drop(variant, "name")
        variants.append(variant)
    for group, verbs in OLD_VERBS.items():
        for verb in verbs:
            table = f"{group}.{verb}"
            entries = raw.get(group, {}).get(verb, [])
            if not entries:
                continue
            counts[table] = len(entries)
            for index, entry in enumerate(entries, 1):
                case = f"{reg.file}#/{table}/{index}"
                if table == "representation.matrix":
                    case = f"{reg.file}#/matrix/{entry['selector']['edition']}"
                try:
                    converted = CONVERTERS[table](reg, entry, case)
                except ConversionError as exc:
                    reg.refused.append(str(exc))
                    continue
                if converted is not None:
                    path, item = converted
                    new[path].append({k: v for k, v in item.items() if v is not None})
    if implicit := _split_pin_partitions(reg):
        counts["variable (split pins)"] = len(implicit)
        new["identity.assign"] += implicit
    tables: list[tuple[str, Any]] = [("register", raw["register"])]
    if variants:
        tables.append(("variant", variants))
    if raw.get("variable"):
        tables.append(("variable", raw["variable"]))
    for name in UNCHANGED:
        item = raw.get(name)
        if isinstance(item, list):
            tables.append((name, item))
        elif isinstance(item, dict):
            tables += [(f"{name}.{sub}", entries) for sub, entries in item.items()]
    tables += [(path, entries) for path, entries in new.items() if entries]
    header = (
        f"Converted from {reg.file} by reg_meta_build.port.convert (PORT.md).\n"
        "Do not edit: change the tracked file and convert again."
    )
    return emit.document(header, tables), counts


def register_files(curation: Path) -> dict[str, Path]:
    """Each register file by its `[register].native_id`."""
    out = {}
    for path in sorted((curation / "registers").glob("*/*.toml")):
        if path.name.endswith(".auto.toml"):
            continue
        native = tomllib.loads(path.read_text(encoding="utf-8"))["register"][
            "native_id"
        ]
        if native in out:
            raise ConversionError(f"register {native} has two files")
        out[str(native)] = path
    return out


def read_variant_slugs(raw: dict[str, Any]) -> dict[str, str]:
    return {variant["native_id"]: variant["slug"] for variant in raw.get("variant", [])}


def old_routes(raw: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    return {
        entry["deldatamangd"]: tuple(entry["variants"])
        for entry in raw.get("identity", {}).get("route", [])
    }


def _old_splits(
    raw: dict[str, Any], variant_slugs: dict[str, str]
) -> dict[tuple[str, str], str]:
    splits = {}
    for entry in raw.get("identity", {}).get("edition_split", []):
        variant = entry["variant"]
        if variant not in variant_slugs:
            variant = next(
                key for key, slug in variant_slugs.items() if slug == variant
            )
        for edition in entry["editions"]:
            splits[(variant, edition)] = entry["split"]
    return splits


def register_index(
    raw: dict[str, Any],
    register_id: str,
    records: list,
    *,
    routes: dict[str, tuple[str, ...]],
    splits: dict[tuple[str, str], str],
) -> RegisterRecords:
    """`raw`'s records and tables, given its token routes and edition splits."""
    variant_slugs = read_variant_slugs(raw)
    places_in_default = (
        bool(records)
        and records[0].subject.provider == "sos"
        and not any(
            parent.kind == "variant"
            for record in records
            for parent in record.parent_facts
        )
        and all(variant["slug"] == "_default" for variant in raw.get("variant", []))
    )
    return RegisterRecords.build(
        register_id,
        records,
        variant_slugs=variant_slugs,
        routes=routes,
        splits=splits,
        places_in_default=places_in_default,
    )


def open_register(
    path: Path,
    curation: Path,
    register_id: str,
    records: list,
    *,
    routes: dict[str, tuple[str, ...]] | None = None,
    splits: dict[tuple[str, str], str] | None = None,
) -> Register:
    """A register file with its pins and records. Its routes and splits are its own
    old entries unless given."""
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    auto = path.with_name(path.name.removesuffix(".toml") + ".auto.toml")
    pins = (
        read_pins(tomllib.loads(auto.read_text(encoding="utf-8")))
        if auto.exists()
        else {}
    )
    pins.update(read_pins(raw))
    variant_slugs = read_variant_slugs(raw)
    return Register(
        file=f"curation/{path.relative_to(curation).as_posix()}",
        raw=raw,
        index=register_index(
            raw,
            register_id,
            records,
            routes=old_routes(raw) if routes is None else routes,
            splits=_old_splits(raw, variant_slugs) if splits is None else splits,
        ),
        variable_slugs=pins,
        variant_slugs=variant_slugs,
    )


def selected_records(
    records: PreparedSourceRecords,
    occurrence_sources: Iterable[str],
    specs: tuple[str, ...],
) -> dict[str, list]:
    """The prepared records of each `--registers` scope, by register naming id. A
    spec may also be a register file's `[register].native_id`."""
    keys: set[tuple[str, NativeKey | None]] = set()
    for source in sorted(occurrence_sources):
        keys.update(
            (source, register)
            for register, _, _ in records.register_coordinates(source)
        )
    by_naming = {
        _naming_source_id("register", register): f"{source}:{register[-1]}"
        for source, register in keys
        if register is not None
    }
    scopes = _selected_scopes(keys, tuple(by_naming.get(spec, spec) for spec in specs))
    out: dict[str, list] = {}
    for source, register in sorted(scopes, key=repr):
        if register is None:
            raise ConversionError(
                f"{source}: a whole-source scope has no register file"
            )
        naming = _naming_source_id("register", register)
        for _, slice_ in records.iter_register_slices(source, [register]):
            out.setdefault(naming, []).extend(slice_)
    return out


def convert_tree(
    curation: Path,
    out: Path,
    records_by_register: dict[str, list],
) -> dict[str, Any]:
    files = register_files(curation)
    target = out / "curation"
    if target.exists():
        raise ConversionError(f"{target} exists; give a new --out")
    shutil.copytree(curation, target)
    report: dict[str, Any] = {
        "registers": {},
        "non_neutral": [],
        "added": [],
        "refused": [],
    }
    for register_id in sorted(records_by_register):
        if register_id not in files:
            raise ConversionError(f"register {register_id} has no curation file")
        path = files[register_id]
        reg = open_register(
            path, curation, register_id, records_by_register[register_id]
        )
        text, counts = convert_register(reg)
        report["refused"] += reg.refused
        report["added"] += reg.added
        if reg.refused:
            continue
        (target / path.relative_to(curation)).write_text(text, encoding="utf-8")
        report["registers"][reg.file] = counts
        report["non_neutral"] += reg.non_neutral
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--input-commit", required=True)
    parser.add_argument("--input-manifest-sha256", required=True)
    parser.add_argument("--curation-dir", type=Path)
    parser.add_argument("--registers", action="append", required=True, metavar="SPEC")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    curation = args.curation_dir or repo_curation_dir()
    if curation is None:
        parser.error("no curation tree; pass --curation-dir")
    prepared = open_prepared_catalog_sources(
        args.prepared,
        expected_sha256=args.input_manifest_sha256,
        input_commit=args.input_commit,
    )
    sources = {
        entry.revision.dataset
        for entry in prepared.manifest.inputs
        if entry.record_usage == "occurrence" and entry.revision is not None
    }
    try:
        records = selected_records(prepared.records, sources, tuple(args.registers))
        report = convert_tree(curation, args.out, records)
    except ConversionError as exc:
        print(f"convert: {exc}", file=sys.stderr)
        return 1
    for refusal in report["refused"]:
        print(f"convert: {refusal}", file=sys.stderr)
    (args.out / "convert-report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "registers": len(report["registers"]),
                "entries": sum(sum(c.values()) for c in report["registers"].values()),
                "non_neutral": len(report["non_neutral"]),
                "refused": len(report["refused"]),
            }
        )
    )
    return 1 if report["refused"] else 0


if __name__ == "__main__":
    sys.exit(main())
