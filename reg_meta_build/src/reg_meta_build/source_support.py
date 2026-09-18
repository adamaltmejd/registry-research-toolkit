"""Bind auxiliary flag declarations using relationships declared by preparation.

All selected native targets must be observed before binding: a name-based support
row cannot be used until its complete target cardinality is known. These joins
provide field evidence, never variable identity, period coverage or source priority.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from reg_meta_build.normalization import normalize_token
from reg_meta_build.source_coordinates import native_variable_key
from reg_meta_build.source_curation import ResolutionDiagnostic
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_records import SourceFields

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_records import (
        SourceCoordinate,
        SourceField,
        SourceRecord,
    )


class SourceSupportJoin(BaseModel):
    """One actual source-format relationship, pinned in the prepared manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    source: str = Field(min_length=1)
    target_sources: tuple[str, ...]
    keys: tuple[
        Literal[
            "register_name",
            "variant_name",
            "variable_name",
            "variable_id",
            "column_name",
        ],
        ...,
    ]
    fields: tuple[Literal["identifier", "sensitivity", "conditional_sensitivity"], ...]
    unique_variable: bool
    # The support fields that name an edition of their own target. One literal
    # key that covers several renumbered native variables binds a row only to
    # the single candidate whose observed edition names carry every endpoint.
    discriminator: tuple[Literal["coverage_from", "coverage_to"], ...] = ()
    rule: str = Field(min_length=1)
    provenance: tuple[str, ...]

    @model_validator(mode="after")
    def _explicit(self) -> Self:
        for values in (self.target_sources, self.keys, self.fields, self.provenance):
            if (
                not values
                or len(set(values)) != len(values)
                or any(not item.strip() for item in values)
            ):
                raise ValueError(
                    "support relationships need unique nonempty source keys, fields and provenance"
                )
        if self.source in self.target_sources or not self.rule.strip():
            raise ValueError(
                "support relationships need distinct source roles and an explicit rule"
            )
        if self.discriminator and (
            not self.unique_variable
            or len(set(self.discriminator)) != len(self.discriminator)
        ):
            raise ValueError(
                "a discriminator names unique endpoints of one native variable"
            )
        return self


def _key(record: SourceRecord, join: SourceSupportJoin) -> tuple[str | int, ...] | None:
    return support_join_key(
        join,
        coordinates={
            "register_name": record.subject.register_name,
            "variant_name": record.subject.variant,
            "variable_name": record.subject.variable,
            "variable_id": record.subject.variable,
        },
        column=record.fields.column_name,
    )


def support_join_key(
    join: SourceSupportJoin,
    *,
    coordinates: Mapping[str, SourceCoordinate],
    column: SourceField | None,
) -> tuple[str | int, ...] | None:
    """Apply the same literal join contract to full records and prepared projections."""
    values = []
    for name in join.keys:
        if name == "column_name":
            value = (
                column.value
                if column is not None and column.status == "value"
                else None
            )
        else:
            coordinate = coordinates[name]
            value = (
                (coordinate.native_id if name == "variable_id" else coordinate.name)
                if coordinate.status == "value"
                else None
            )
        if not isinstance(value, (str, int)) or value == "":
            return None
        values.append(value)
    return tuple(values)


def _endpoints(join: SourceSupportJoin, record: SourceRecord) -> tuple[str, ...] | None:
    """The row's declared endpoint names; an absent or empty endpoint has none."""
    values = []
    for name in join.discriminator:
        field = getattr(record.fields, name)
        if field is None or field.status != "value" or not isinstance(field.value, str):
            return None
        token = normalize_token(field.value)
        if not token:
            return None
        values.append(token)
    return tuple(values)


@dataclass(frozen=True)
class SupportTarget:
    """Cardinality evidence only; never a synthetic or partially decoded source row."""

    support_source: str
    record_source: str
    variable_key: NativeKey | None
    key: tuple[str | int, ...] | None
    edition_name: str | None


@dataclass(frozen=True)
class SupportMatch:
    record: SourceRecord
    fields: SourceFields
    # The one native variable a discriminated row was bound to, if its literal
    # key named several. Rows bound by the key alone apply to every target.
    variable_key: NativeKey | None = None


type SupportDisposition = Literal[
    "bound_support", "unreferenced_support", "unknown_key", "ambiguous_target"
]
# One group of a literal key's support rows: the rows, the single native variable
# a discriminator chose for them, their disposition and its diagnostic code.
type _SupportGroup = tuple[
    tuple[SourceRecord, ...], NativeKey | None, SupportDisposition, str | None
]


@dataclass(frozen=True)
class SupportAccounting:
    record: SourceRecord
    disposition: SupportDisposition
    targets: tuple[NativeKey, ...]


class SourceSupportBindings:
    """Collect target cardinalities during the ordinary parent-record scan."""

    def __init__(
        self, joins: tuple[SourceSupportJoin, ...], records: Iterable[SourceRecord]
    ) -> None:
        self.joins = {join.source: join for join in joins}
        if len(self.joins) != len(joins):
            raise ValueError("support sources must have one declared relationship")
        self._by_target: dict[str, list[SourceSupportJoin]] = defaultdict(list)
        for join in joins:
            for target in join.target_sources:
                self._by_target[target].append(join)
        self._support: dict[
            tuple[str, tuple[str | int, ...] | None], list[SourceRecord]
        ] = defaultdict(list)
        for record in records:
            if record.source not in self.joins:
                raise ValueError(
                    f"missing support relationship for source {record.source!r}"
                )
            self._support[
                record.source, _key(record, self.joins[record.source])
            ].append(record)
        self._targets: dict[
            tuple[str, tuple[str | int, ...]], set[NativeKey | None]
        ] = defaultdict(set)
        self._matches: dict[
            tuple[str, tuple[str | int, ...]], tuple[SupportMatch, ...]
        ] = {}
        self._editions: dict[
            tuple[str, tuple[str | int, ...]], dict[NativeKey, set[str]]
        ] = defaultdict(dict)
        self._sealed = False
        self.accounting: tuple[SupportAccounting, ...] = ()
        self.diagnostics: tuple[ResolutionDiagnostic, ...] = ()

    def observe(self, record: SourceRecord) -> None:
        if self._sealed:
            raise ValueError("support target collection is already complete")
        for join in self._by_target.get(record.source, ()):
            self.observe_target(
                SupportTarget(
                    join.source,
                    record.source,
                    native_variable_key(record),
                    _key(record, join),
                    record.original_period_text,
                )
            )

    def observe_target(self, target: SupportTarget) -> None:
        if self._sealed:
            raise ValueError("support target collection is already complete")
        join = self.joins.get(target.support_source)
        if join is None or target.record_source not in join.target_sources:
            raise ValueError(
                "support target does not belong to a selected relationship"
            )
        if target.key is not None:
            if len(target.key) != len(join.keys) or any(
                type(v) not in {str, int} or v == "" for v in target.key
            ):
                raise ValueError("support target key does not match its join contract")
            if (join.source, target.key) in self._support:
                self._targets[join.source, target.key].add(target.variable_key)
                if (
                    join.discriminator
                    and target.variable_key is not None
                    and target.edition_name
                ):
                    self._editions[join.source, target.key].setdefault(
                        target.variable_key, set()
                    ).add(normalize_token(target.edition_name))

    def _dispositions(
        self,
        join: SourceSupportJoin,
        key: tuple[str | int, ...] | None,
        records: list[SourceRecord],
        targets: set[NativeKey | None],
    ) -> Iterator[_SupportGroup]:
        """Decide one literal key: the join itself, then its declared discriminator."""
        if key is None:
            yield tuple(records), None, "unknown_key", "unknown_support_key"
        elif not targets:
            yield (
                tuple(records),
                None,
                "unreferenced_support",
                "unreferenced_support_record",
            )
        elif None in targets or (join.unique_variable and len(targets) != 1):
            # An unknown native identity among the candidates stays unresolved:
            # nothing observed its editions, so no partition can exclude it.
            if not join.discriminator or None in targets:
                yield (
                    tuple(records),
                    None,
                    "ambiguous_target",
                    "ambiguous_support_target",
                )
            else:
                yield from self._discriminated(join, key, records)
        else:
            yield tuple(records), None, "bound_support", None

    def _discriminated(
        self,
        join: SourceSupportJoin,
        key: tuple[str | int, ...],
        records: list[SourceRecord],
    ) -> Iterator[_SupportGroup]:
        """Split one ambiguous key by the endpoints its discriminator declares.

        A partition binds only to the single candidate whose observed edition
        names contain every endpoint. Endpoints are compared as literal names:
        nothing is ordered, parsed as a year or read as continuous coverage.
        """
        editions = self._editions.get((join.source, key), {})
        partitions: dict[tuple[str, ...] | None, list[SourceRecord]] = defaultdict(list)
        for record in records:
            partitions[_endpoints(join, record)].append(record)
        for endpoints, group in partitions.items():
            families = [
                family
                for family, names in editions.items()
                if endpoints is not None and names >= set(endpoints)
            ]
            if len(families) == 1:
                yield tuple(group), families[0], "bound_support", None
            else:
                yield tuple(group), None, "ambiguous_target", "ambiguous_support_target"

    def seal(self) -> None:
        if self._sealed:
            raise ValueError("support target collection is already complete")
        accounting = []
        diagnostics = []
        for (source, key), records in sorted(
            self._support.items(), key=lambda item: repr(item[0])
        ):
            join = self.joins[source]
            targets = (
                self._targets.get((source, key), set()) if key is not None else set()
            )
            known = tuple(
                sorted((target for target in targets if target is not None), key=repr)
            )
            for group, family, disposition, code in self._dispositions(
                join, key, records, targets
            ):
                if disposition == "bound_support":
                    # Only a complete literal key ever reaches a bound group.
                    assert key is not None
                    self._matches[source, key] = self._matches.get(
                        (source, key), ()
                    ) + tuple(
                        SupportMatch(
                            record,
                            SourceFields.model_validate(
                                {
                                    name: getattr(record.fields, name)
                                    for name in join.fields
                                }
                            ),
                            family,
                        )
                        for record in group
                    )
                bound = known if family is None else (family,)
                for record in group:
                    accounting.append(SupportAccounting(record, disposition, bound))
                if code is not None:
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code=code,
                            severity="warning"
                            if disposition == "unreferenced_support"
                            else "error",
                            subject=f"{source}:{key!r}",
                            detail=f"Support relationship {join.rule!r} has disposition {disposition!r}; target identities {known!r}. No fields were joined.",
                            refs=tuple(
                                dict.fromkeys(record_ref(record) for record in group)
                            ),
                            fields=join.fields,
                            withheld_output=tuple(
                                f"support.{name}" for name in join.fields
                            ),
                        )
                    )
        self.accounting = tuple(accounting)
        self.diagnostics = tuple(diagnostics)
        self._sealed = True

    def bind(self, record: SourceRecord) -> tuple[SupportMatch, ...]:
        if not self._sealed:
            raise ValueError("complete support target collection before binding")
        result = []
        family = native_variable_key(record)
        for join in self._by_target.get(record.source, ()):
            key = _key(record, join)
            if key is not None:
                if (join.source, key) in self._matches and family not in self._targets[
                    join.source, key
                ]:
                    raise ValueError(
                        "support binding received an unobserved target variable"
                    )
                result.extend(
                    match
                    for match in self._matches.get((join.source, key), ())
                    if match.variable_key in {None, family}
                )
        return tuple(result)
