"""Bind explicit source succession events to already resolved catalog identities."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

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
    from collections.abc import Iterable, Mapping, Sequence

    from reg_meta_build.catalog_dependencies import DependencyKey
    from reg_meta_build.resolved_metadata import ResolvedMetadata
    from reg_meta_build.source_records import SourceRecord
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
            for kind, field in _NATIVE_FIELD.items():
                native = getattr(record.subject.native, field)
                if native is None:
                    continue
                key = record.source, kind, str(native)
                if key not in self.targets:
                    continue
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

    def resolve(self, metadata: ResolvedMetadata) -> ReferenceMetadataResolution:
        """Keep independent edges; never choose an ambiguous native-ID binding."""
        diagnostics = []
        withheld = set()
        grouped = defaultdict(list)
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
            if any(
                len(self.targets[key]) != 1 or None in self.targets[key] for key in keys
            ):
                withheld.add(event_ref)
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="unresolved_source_event_endpoint",
                        severity="error",
                        subject=repr(event.locator.semantic_record_key),
                        detail=f"Succession endpoints {keys!r} do not each identify one supported catalog entity: "
                        f"{tuple(tuple(sorted(self.targets[k], key=repr)) for k in keys)!r}.",
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
            grouped[endpoints].append((description, refs))

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
            combined, tuple(diagnostics), tuple(sorted(withheld, key=repr))
        )
