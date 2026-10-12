"""Bind explicit source succession events to already resolved catalog identities."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.catalog_dependencies import DEFERRED_REFERENCE
from reg_meta_build.resolved_metadata import (
    ResolvedSuccession,
    ResolvedVariantRef,
    ResolvedVariantSuccession,
    validate_metadata_structure,
)
from reg_meta_build.source_coordinates import native_variant_key, source_register_key
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CurationCase,
    ResolutionDiagnostic,
    SourceRecordRef,
    acknowledgement_hashes_sha256,
    settle_acknowledgements,
)
from reg_meta_build.source_effects import record_ref

from .source_evidence import evidence_sha256

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from reg_meta_build.catalog_dependencies import DependencyKey
    from reg_meta_build.resolved_catalog import ResolvedRegister
    from reg_meta_build.resolved_metadata import ResolvedMetadata
    from reg_meta_build.source_records import NativeCoordinates, SourceRecord
    from reg_meta_build.source_reference_records import SourceEventDeclaration
    from reg_meta_build.source_scope import ScopeResolution


type NativeEventKey = tuple[str, str, str]
# A guarded record's ref, evidence hash, register key and resolved register.
type GuardedObservation = tuple[
    SourceRecordRef, str, tuple[str | int, ...] | None, ResolvedRegister | None
]
# An event endpoint one record matches, that record's ref and the targets it adds.
type EndpointObservation = tuple[
    NativeEventKey, SourceRecordRef, tuple[DependencyKey | None, ...]
]


@dataclass(frozen=True)
class ScopeEventObservation:
    guarded: tuple[GuardedObservation, ...]
    endpoints: tuple[EndpointObservation, ...]


@dataclass(frozen=True)
class ReferenceMetadataResolution:
    metadata: ResolvedMetadata
    diagnostics: tuple[ResolutionDiagnostic, ...]
    withheld: tuple[SourceRecordRef, ...]
    skipped: int = 0


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
        acknowledgements: tuple[CurationCase, ...] = (),
    ) -> None:
        self.events = tuple(
            e for e in events if e.action in {"replaced_by", "replaces"}
        )
        self.sources = dict(target_sources)
        self.acknowledgements = acknowledgements
        self.guarded_hashes: dict[SourceRecordRef, list[str]] = {
            ref: []
            for case in acknowledgements
            if isinstance(case.decision, AcknowledgeDecision)
            for ref in case.decision.refs
        }
        self.guarded_owners: dict[SourceRecordRef, object] = {}
        self.guarded_registers: dict[tuple[str | int, ...], ResolvedRegister] = {}
        self.targets: dict[NativeEventKey, set[DependencyKey | None]] = {}
        self.refs: dict[NativeEventKey, set[SourceRecordRef]] = {}
        self.unselected: set[NativeEventKey] = set()
        self.skipped_events: list[SourceRecordRef] = []
        for event in self.events:
            event_ref = SourceRecordRef(
                source=event.revision.dataset,
                semantic_record_key=event.locator.semantic_record_key,
            )
            if event_ref in self.guarded_hashes:
                self.guarded_hashes[event_ref].append(evidence_sha256(event))
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
        self.record_scope(self.scope_observation(originals, result, uses))

    def scope_observation(
        self,
        originals: tuple[SourceRecord, ...],
        result: ScopeResolution,
        uses: Mapping[str, Sequence[Mapping[str, object]]],
    ) -> ScopeEventObservation:
        """What `observe_scope` records for one scope, without recording it.

        It reads only the event endpoints and guarded refs fixed at construction,
        so a worker process can compute it for the parent to record in order.
        """
        guarded: list[GuardedObservation] = []
        endpoints: list[EndpointObservation] = []
        if not originals or originals[0].source not in self.sources.values():
            return ScopeEventObservation((), ())
        for record in originals:
            ref = record_ref(record)
            if ref in self.guarded_hashes:
                register_key = source_register_key(record)
                guarded.append(
                    (
                        ref,
                        evidence_sha256(record),
                        register_key,
                        result.parents.registers.get(register_key),
                    )
                )
            for kind, key in self._endpoints(record.source, record.subject.native):
                targets: list[DependencyKey | None] = []
                if kind in {"variable", "member"}:
                    for use in uses[record.record_id]:
                        variable = use["variable"]
                        if variable is not None and not isinstance(variable, str):
                            raise TypeError(
                                "source event variable target must be an FQID"
                            )
                        targets.append(("variable", variable) if variable else None)
                else:
                    register = result.parents.registers.get(source_register_key(record))
                    if register is None:
                        targets.append(None)
                    elif kind == "register":
                        targets.append(
                            ("register", f"{register.provider}/{register.slug}")
                        )
                    else:
                        variant = result.parents.variants.get(
                            native_variant_key(record)
                        )
                        targets.append(
                            (
                                "variant",
                                f"{register.provider}/{register.slug}",
                                variant.slug,
                            )
                            if variant
                            else None
                        )
                endpoints.append((key, ref, tuple(targets)))
        return ScopeEventObservation(tuple(guarded), tuple(endpoints))

    def record_scope(self, observation: ScopeEventObservation) -> None:
        """Record one scope's observation; scopes are recorded in a fixed order."""
        for ref, digest, register_key, register in observation.guarded:
            self.guarded_hashes[ref].append(digest)
            self.guarded_owners[ref] = register_key
            if register_key is not None and register is not None:
                self.guarded_registers[register_key] = register
        for key, ref, targets in observation.endpoints:
            self.refs[key].add(ref)
            self.targets[key].update(targets)

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
        register can defer an event only when no endpoint has selected-source
        evidence; an event with every endpoint outside is skipped.
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
                selected_evidence = any(self.refs[k] or self.targets[k] for k in keys)
                if len(outside) == len(keys) and not selected_evidence:
                    skipped += 1
                    self.skipped_events.append(event_ref)
                    continue
                withheld.add(event_ref)
                if outside and not selected_evidence:
                    code, severity = DEFERRED_REFERENCE, "warning"
                    unknown = tuple(k for k in unresolved if k not in self.unselected)
                    detail = (
                        f"Succession endpoints {tuple(outside)!r} are positively observed "
                        "in unselected source registers and no endpoint has selected-source evidence. "
                        f"Unknown endpoints {unknown!r} remain missing, without inferred ownership. "
                        "The scoped event is deferred; a complete build must reevaluate every endpoint."
                    )
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
                # A variant edge stores endpoints only, so its descriptions are moot.
                if endpoints not in existing_variants:
                    variants.append(
                        ResolvedVariantSuccession(
                            predecessor=ResolvedVariantRef(register=a[1], variant=a[2]),
                            successor=ResolvedVariantRef(register=b[1], variant=b[2]),
                        )
                    )
                continue
            if (a[1], b[1]) in existing:
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
        evidence_hashes = {}
        for case in self.acknowledgements:
            decision = case.decision
            assert isinstance(decision, AcknowledgeDecision)
            owners = {
                self.guarded_owners[ref]
                for ref in decision.refs
                if ref in self.guarded_owners
            }
            evidence_hashes[case.case_id] = (
                acknowledgement_hashes_sha256(
                    token for ref in decision.refs for token in self.guarded_hashes[ref]
                )
                if owners == {decision.register_key}
                and all(self.guarded_hashes[ref] for ref in decision.refs)
                else None
            )
        diagnostics = settle_acknowledgements(
            tuple(diagnostics), self.acknowledgements, evidence_hashes
        )
        return ReferenceMetadataResolution(
            combined, tuple(diagnostics), tuple(sorted(withheld, key=repr)), skipped
        )
