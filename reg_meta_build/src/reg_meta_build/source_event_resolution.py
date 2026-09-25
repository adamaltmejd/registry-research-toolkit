"""Bind explicit source succession events to already resolved catalog identities."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from reg_meta_build.catalog_dependencies import DEFERRED_REFERENCE
from reg_meta_build.resolved_metadata import (
    ResolvedSuccession,
    ResolvedVariantRef,
    ResolvedVariantSuccession,
    validate_metadata_structure,
)
from reg_meta_build.source_coordinates import native_variant_key, source_register_key
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_reference_resolution import ReferenceMetadataResolution

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from reg_meta_build.catalog_dependencies import DependencyKey
    from reg_meta_build.resolved_metadata import ResolvedMetadata
    from reg_meta_build.source_records import NativeCoordinates, SourceRecord
    from reg_meta_build.source_reference_records import SourceEventDeclaration
    from reg_meta_build.source_scope import ScopeResolution


type NativeEventKey = tuple[str, str, str]
_NATIVE_FIELD = {
    "register": "register_id",
    "variant": "register_variant_id",
    "variable": "variable_id",
    "member": "member_id",
}


class SourceEventBindings:
    """Collect only native IDs referenced by the selected succession events.

    Each event source explicitly names its target occurrence source. Neither a
    shared native number nor a publisher name can join unrelated namespaces.
    """

    def __init__(
        self,
        events: Iterable[SourceEventDeclaration],
        target_sources: Mapping[str, str],
    ) -> None:
        self.events = tuple(
            e for e in events if e.action in {"replaced_by", "replaces"}
        )
        self.sources = dict(target_sources)
        self.targets: dict[NativeEventKey, set[DependencyKey | None]] = {}
        self.refs: dict[NativeEventKey, set[SourceRecordRef]] = {}
        self.unselected: set[NativeEventKey] = set()
        self.skipped_events: list[SourceRecordRef] = []
        for event in self.events:
            if event.revision.dataset not in self.sources:
                raise ValueError(
                    "succession event source needs an explicit occurrence-source "
                    f"binding: {event.revision.dataset}"
                )
            if event.entity_kind not in _NATIVE_FIELD:
                raise ValueError(
                    f"unsupported succession event entity: {event.entity_kind}"
                )
            for cell in (event.first_token, event.second_token):
                key = self._key(event, cell.interpreted_value)
                self.targets.setdefault(key, set())
                self.refs.setdefault(key, set())

    def _key(self, event: SourceEventDeclaration, token: str) -> NativeEventKey:
        return self.sources[event.revision.dataset], event.entity_kind, token.strip()

    def _endpoints(
        self, source: str, native: NativeCoordinates
    ) -> Iterator[tuple[str, NativeEventKey]]:
        """The event endpoints one set of native IDs matches, by entity kind."""
        for kind, field in _NATIVE_FIELD.items():
            value = getattr(native, field)
            if (
                value is not None
                and (key := (source, kind, str(value))) in self.targets
            ):
                yield kind, key

    def observe_scope(
        self,
        originals: tuple[SourceRecord, ...],
        result: ScopeResolution,
        uses: Mapping[str, Sequence[Mapping[str, object]]],
    ) -> None:
        """Account for complete source scopes, including unresolved endpoint peers."""
        if not originals or originals[0].source not in self.sources.values():
            return
        for record in originals:
            for kind, key in self._endpoints(record.source, record.subject.native):
                self.refs[key].add(record_ref(record))
                targets = self.targets[key]
                if kind in {"variable", "member"}:
                    for use in uses[record.record_id]:
                        variable = use["variable"]
                        if variable is not None and not isinstance(variable, str):
                            raise TypeError(
                                "source event variable target must be an FQID"
                            )
                        targets.add(("variable", variable) if variable else None)
                    continue
                register = result.parents.registers.get(source_register_key(record))
                if register is None:
                    targets.add(None)
                    continue
                fqid = f"{register.provider}/{register.slug}"
                if kind == "register":
                    targets.add(("register", fqid))
                else:
                    variant = result.parents.variants.get(native_variant_key(record))
                    targets.add(("variant", fqid, variant.slug) if variant else None)

    def observe_unselected(
        self, source: str, natives: Iterable[NativeCoordinates]
    ) -> None:
        """After every selected scope: note endpoints no selected scope observed
        but an unselected register does. A register-scoped build defers them."""
        remaining = {
            key
            for key, targets in self.targets.items()
            if key[0] == source and not targets
        }
        for native in natives if remaining else ():
            for _kind, key in self._endpoints(source, native):
                if key in remaining:
                    remaining.remove(key)
                    self.unselected.add(key)
            if not remaining:
                break

    def resolve(self, metadata: ResolvedMetadata) -> ReferenceMetadataResolution:
        """Keep independent edges; never choose an ambiguous native-ID binding.

        In a register-scoped build an endpoint observed only in an unselected
        register is deferred; an event with every endpoint there is skipped.
        """
        diagnostics = []
        withheld = set()
        grouped = defaultdict(list)
        skipped = 0
        for event in self.events:
            event_ref = SourceRecordRef(
                source=event.revision.dataset,
                semantic_record_key=event.locator.semantic_record_key,
            )
            keys = tuple(
                self._key(event, cell.interpreted_value)
                for cell in (event.first_token, event.second_token)
            )
            refs = tuple(
                sorted(
                    {event_ref, *(r for key in keys for r in self.refs[key])}, key=repr
                )
            )
            unresolved = [
                key
                for key in keys
                if len(self.targets[key]) != 1 or None in self.targets[key]
            ]
            if unresolved:
                outside = [k for k in unresolved if k in self.unselected]
                if len(outside) == len(keys):
                    skipped += 1
                    self.skipped_events.append(event_ref)
                    continue
                withheld.add(event_ref)
                if len(outside) == len(unresolved):
                    code, severity = DEFERRED_REFERENCE, "warning"
                    detail = f"Succession endpoints {tuple(outside)!r} lie in unselected registers; the complete build resolves them."
                else:
                    code, severity = "unresolved_source_event_endpoint", "error"
                    detail = (
                        f"Succession endpoints {keys!r} do not each identify one supported catalog entity: "
                        f"{tuple(tuple(sorted(self.targets[k], key=repr)) for k in keys)!r}."
                    )
                diagnostics.append(
                    ResolutionDiagnostic(
                        code=code,
                        severity=severity,
                        subject=repr(event.locator.semantic_record_key),
                        detail=detail,
                        refs=refs,
                        withheld_output=("catalog_succession",),
                    )
                )
                continue
            endpoints = tuple(next(iter(self.targets[key])) for key in keys)
            if event.action == "replaces":
                endpoints = tuple(reversed(endpoints))
            if any(endpoint is None for endpoint in endpoints):
                raise AssertionError("unresolved event endpoint escaped its guard")
            description = (
                event.description.value if event.description.status == "value" else None
            )
            # Description disagreements concern the event rows themselves; the
            # repeated parent/variable occurrences do not supply that prose.
            grouped[endpoints].append((description, (event_ref,)))

        existing = {(e.predecessor, e.successor) for e in metadata.successions}
        existing_variants = {
            (
                ("variant", e.predecessor.register_ref, e.predecessor.variant),
                ("variant", e.successor.register_ref, e.successor.variant),
            )
            for e in metadata.variant_successions
        }
        successions, variants = [], []
        for endpoints, assertions in sorted(grouped.items()):
            a, b = endpoints
            if a == b:
                # An accepted identity merge already represents this transition.
                continue
            if a[0] == "variant":
                if endpoints in existing_variants:
                    continue
            elif (a[1], b[1]) in existing:
                continue
            descriptions = {value for value, _refs in assertions if value is not None}
            description = next(iter(descriptions)) if len(descriptions) == 1 else None
            if len(descriptions) > 1:
                refs = tuple(
                    sorted({r for _, rows in assertions for r in rows}, key=repr)
                )
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="conflicting_source_event_description",
                        severity="error",
                        subject=repr(endpoints),
                        detail=f"Source events agree on succession but supply different descriptions: {sorted(descriptions)!r}.",
                        refs=refs,
                        fields=("description",),
                        withheld_output=("catalog_succession.description",),
                    )
                )
            if a[0] == "variant":
                variants.append(
                    ResolvedVariantSuccession(
                        predecessor=ResolvedVariantRef(register=a[1], variant=a[2]),
                        successor=ResolvedVariantRef(register=b[1], variant=b[2]),
                        note="auto:timeseries_event",
                        description=description,
                    )
                )
            else:
                successions.append(
                    ResolvedSuccession(
                        predecessor=a[1],
                        successor=b[1],
                        note="auto:timeseries_event",
                        description=description,
                    )
                )
        combined = metadata.model_copy(
            update={
                "successions": (*metadata.successions, *successions),
                "variant_successions": (*metadata.variant_successions, *variants),
            }
        )
        validate_metadata_structure(combined)
        return ReferenceMetadataResolution(
            combined, tuple(diagnostics), tuple(sorted(withheld, key=repr)), skipped
        )
