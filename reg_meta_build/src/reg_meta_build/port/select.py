"""The prepared records of one register, and the neutral member selector over them.

`Member` is `crates/reg-build-model/src/verbs.rs`'s selector: every given coordinate must
match, an absent one does not restrict. `RegisterRecords.select` is its reference
semantics; the Rust reader implements the same, and the equivalence harness checks this
one against Python's compile.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from reg_meta_build.curation_compile import _naming_source_id
from reg_meta_build.source_coordinates import (
    native_parent_key,
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_records import SourceFields, TemporalScope

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_records import SourceRecord

type Ref = tuple[str, tuple[str, ...]]
"""A record as Python's compile refers to it: (source, semantic record key)."""


class ConversionError(ValueError):
    """An old entry the converter cannot translate exactly."""


@dataclass(frozen=True)
class Rec:
    """One variable occurrence and the coordinates both curation forms address."""

    ref: Ref
    record: SourceRecord
    variable_id: str
    """The old address of its variable: `<register naming id>.<member key>`."""
    variant_id: str | None
    """Its own variant's naming id, before any route or split."""
    token: str | None
    """The source's own variant token (SoS Deldatamängd, SCB variant name)."""
    column: str | None
    """The delivered literal; None when the source states no physical column."""
    delivery: str | None
    """Its edition's name, where the source has editions."""
    edition_id: int | None

    FIELDS: ClassVar[frozenset[str]] = frozenset(SourceFields.model_fields)

    def stated(self, name: str) -> Any:
        """A field's normalized value where its status is `value`, else None."""
        value = getattr(self.record.fields, name)
        return value.value if value is not None and value.status == "value" else None


@dataclass(frozen=True)
class Member:
    table: str | None = None
    column: str | None = None
    deliveries: tuple[str, ...] | None = None
    period: Mapping[str, Any] | None = None
    where: tuple[tuple[str, Any], ...] = ()

    def as_toml(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.table is not None:
            out["table"] = self.table
        if self.column is not None:
            out["column"] = self.column
        if self.deliveries is not None:
            out["deliveries"] = list(self.deliveries)
        if self.period is not None:
            out["period"] = dict(self.period)
        if self.where:
            out["where"] = dict(self.where)
        return out


def scope(period: Mapping[str, Any]) -> TemporalScope:
    """A period as TOML or JSON states it (lists, date strings), validated in JSON
    mode as the strict model requires."""
    return TemporalScope.model_validate_json(json.dumps(period, default=str))


def _delivery(record: SourceRecord) -> tuple[str | None, int | None]:
    editions = {
        (parent.coordinate.name, parent.coordinate.native_id)
        for parent in record.parent_facts
        if parent.kind == "edition"
    }
    if len(editions) > 1:
        raise ConversionError(f"{record.record_id}: several edition parents")
    if not editions:
        return None, None
    name, native = editions.pop()
    return name, native if isinstance(native, int) else None


@dataclass
class RegisterRecords:
    """Every variable occurrence of one register, with its effective tables.

    `routes` are the old `identity.route` entries (token -> variant names); `splits`
    the old `identity.edition_split` entries ((variant naming id, edition name) ->
    split naming id). `variant_slugs` maps a variant naming id to its pinned slug.
    """

    register_id: str
    records: tuple[Rec, ...]
    variant_slugs: Mapping[str, str]
    routes: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    splits: Mapping[tuple[str, str], str] = field(default_factory=dict)
    _tables: dict[Ref, tuple[str, ...]] = field(default_factory=dict, init=False)
    _source_tables: dict[Ref, tuple[str, ...]] = field(default_factory=dict, init=False)

    @classmethod
    def build(
        cls,
        register_id: str,
        records: Iterable[SourceRecord],
        *,
        variant_slugs: Mapping[str, str],
        routes: Mapping[str, tuple[str, ...]],
        splits: Mapping[tuple[str, str], str],
        places_in_default: bool,
    ) -> RegisterRecords:
        records = tuple(records)
        parent_variants: dict[str, set[NativeKey]] = defaultdict(set)
        # A thin provider's variants by their authored key (`curation_compile`'s thin
        # declarations): a record lands in the ones it references, else in all.
        thin_variants: dict[str, NativeKey] = {}
        for record in records:
            for parent in record.parent_facts:
                if parent.kind != "variant" or parent.variant is None:
                    continue
                key = native_parent_key(record.source, record.subject.provider, parent)
                if key is None:
                    continue
                if parent.variant.name:
                    parent_variants[parent.variant.name].add(key)
                if isinstance(parent.variant.native_id, str):
                    thin_variants[parent.variant.native_id] = key
        recs = []
        tables: dict[Ref, tuple[str, ...]] = {}
        source_tables: dict[Ref, tuple[str, ...]] = {}
        for record in records:
            variable = native_variable_key(record)
            # Register- and variant-level facts are no variable occurrence.
            if variable is None or record.subject.variable.status != "value":
                continue
            ref = (record.source, record.locators[0].semantic_record_key)
            register = source_register_key(record)
            assert register is not None
            # The synthesized `_default` variant: a record that states no variant, or
            # every record of a SoS register with no Deldatamängder sheet.
            default_key = (*register, "variant", "not-applicable")
            own = native_variant_key(record)
            variant_id = (
                _naming_source_id("register_variant", default_key, member="_default")
                if record.subject.variant.status == "not_applicable"
                else _naming_source_id("register_variant", own)
                if own is not None
                else None
            )
            token = record.subject.variant.name
            column = record.fields.column_name
            delivery, edition_id = _delivery(record)
            rec = Rec(
                ref=ref,
                record=record,
                variable_id=_naming_source_id("variable", variable),
                variant_id=variant_id,
                token=token,
                column=str(column.value)
                if column is not None and column.status == "value" and column.value
                else None,
                delivery=delivery,
                edition_id=edition_id,
            )
            recs.append(rec)
            if places_in_default:
                ids = [
                    _naming_source_id(
                        "register_variant", default_key, member="_default"
                    )
                ]
            elif token is not None and token in routes:
                keys = tuple(
                    key
                    for name in routes[token]
                    for key in sorted(parent_variants.get(name, ()), key=repr)
                )
                if not keys:
                    raise ConversionError(f"route {token!r} names no variant parent")
                # A route into an unpinned variant (SoS lookup sheets) leaves the
                # rows support-only: they are on no table.
                ids = [
                    id_
                    for key in keys
                    if (id_ := _naming_source_id("register_variant", key))
                    in variant_slugs
                ]
            elif record.subject.provider not in ("scb", "sos") and (
                record.subject.variant.status == "not_applicable"
            ):
                names = [
                    str(ref.native_id)
                    for ref in record.subject.variant_references
                    if ref.native_id is not None
                ]
                names = names or sorted(thin_variants)
                ids = [
                    _naming_source_id("register_variant", thin_variants[name])
                    for name in names
                ] or [
                    _naming_source_id(
                        "register_variant", default_key, member="_default"
                    )
                ]
            else:
                ids = [variant_id] if variant_id is not None else []
            split = (
                [splits.get((id_, delivery), id_) for id_ in ids] if delivery else ids
            )
            try:
                source_tables[ref] = tuple(sorted({variant_slugs[id_] for id_ in ids}))
                tables[ref] = tuple(sorted({variant_slugs[id_] for id_ in split}))
            except KeyError as exc:
                raise ConversionError(
                    f"{register_id}: variant {exc.args[0]} of {record.record_id} has no "
                    "pinned slug"
                ) from None
        out = cls(register_id, tuple(recs), variant_slugs, routes, splits)
        out._tables = tables
        out._source_tables = source_tables
        return out

    def tables(self, rec: Rec, *, source: bool = False) -> tuple[str, ...]:
        """The tables a record is on: after `table.assign` splits, or before them
        (`source`), which is what a `table.assign` member selects on. Routes are
        applied in both, as the SoS adapter resolves 1:1 tokens (D7); a record of an
        ambiguous token is on every table it routes to."""
        return (self._source_tables if source else self._tables)[rec.ref]

    def matches(self, member: Member, rec: Rec, *, source: bool = False) -> bool:
        if member.table is not None and member.table not in self.tables(
            rec, source=source
        ):
            return False
        if member.column is not None and rec.column != member.column:
            return False
        if member.deliveries is not None and rec.delivery not in member.deliveries:
            return False
        if member.period is not None and rec.record.edition_scope != scope(
            member.period
        ):
            return False
        return all(rec.stated(name) == value for name, value in member.where)

    def select(self, member: Member, *, source: bool = False) -> set[Ref]:
        return {
            rec.ref for rec in self.records if self.matches(member, rec, source=source)
        }

    def chosen(self, member: Member, *, source: bool = False) -> set[int]:
        """The selected records themselves: alternative records share a ref."""
        return {
            id(rec) for rec in self.records if self.matches(member, rec, source=source)
        }


# The stated fields `exact_member` may add to `where`, in order: the name first, as the
# golden corpus does; the rest only where the name alone does not separate the records.
WHERE_FIELDS = ("name", "description", "data_type", "definition")


def exact_member(
    index: RegisterRecords,
    member: Member,
    target: Iterable[Rec],
    *,
    case: str,
    source: bool = False,
) -> tuple[Member, bool]:
    """`member` narrowed with `where` until it selects exactly the `target` records.

    Returns the member and whether it needed `where`. Refuses a member that misses a
    target record, or one that no stated field separates.
    """
    target = list(target)
    wanted = {id(rec) for rec in target}
    selected = index.chosen(member, source=source)
    if not wanted <= selected:
        raise ConversionError(
            f"{case}: the translated member misses {len(wanted - selected)} of "
            f"{len(wanted)} records"
        )
    if selected == wanted:
        return member, False
    where: list[tuple[str, Any]] = []
    for name in WHERE_FIELDS:
        values = {rec.stated(name) for rec in target}
        if len(values) != 1 or None in values:
            continue
        where.append((name, values.pop()))
        narrowed = Member(
            member.table, member.column, member.deliveries, member.period, tuple(where)
        )
        selected = index.chosen(narrowed, source=source)
        if selected == wanted:
            return narrowed, True
    raise ConversionError(
        f"{case}: no stated field separates its {len(wanted)} records from "
        f"{len(selected - wanted)} others"
    )
