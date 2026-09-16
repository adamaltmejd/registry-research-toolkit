"""Finite naming conversion preserves intent without inventing native identity."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, Literal

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta_build.id import mint, mint_canonical_scb
from reg_meta_build.source_curation import (
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
)
from reg_meta_build.source_naming import (
    AcceptedNamingEntry,
    LegacyNamingBinding,
    NamingConversionError,
    NamingFreezeSetting,
    NamingSelection,
    NativeNamingTarget,
    authored_naming_id,
    check_naming_target,
    convert_naming,
    native_scb_naming_id,
    read_naming_selection,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceRevision,
    canonical_sha256,
)
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_naming import NativeKey
    from reg_meta_build.source_records import SourceRecord


def _revision(path: Path | None = None) -> SourceRevision:
    payload = path.read_bytes() if path else b"fixture"
    return SourceRevision.create(
        dataset="test-source",
        publisher="fixture",
        purpose="naming test",
        upstream_revision="fixture",
        artifact_path=f"fqid_slugs/{path.name}" if path else "fixture",
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _entry(
    key: str = "1.101",
    slug: str | None = "variable-a",
    *,
    origin: Literal["authored", "generated"] = "authored",
    kind: Literal[
        "register", "register_variant", "variable", "classification"
    ] = "variable",
    **metadata: Any,
) -> AcceptedNamingEntry:
    raw = {**({"slug": slug} if slug is not None else {}), **metadata}
    naming = SlugEntry(kind, key, slug, provider="scb", **metadata)
    revision = _revision().model_copy(
        update={"artifact_path": f"scb{'.auto' if origin == 'generated' else ''}.toml"}
    )
    return AcceptedNamingEntry(
        revision=revision,
        origin=origin,
        entry=naming,
        supplied_fields=tuple(sorted(raw)),
        content_sha256=canonical_sha256(raw),
    )


def _selection(
    *entries: AcceptedNamingEntry,
    state: Literal["churning", "curating", "frozen"] = "curating",
) -> NamingSelection:
    return NamingSelection(
        files=(),
        entries=entries,
        freeze=(NamingFreezeSetting(zone="scb", state=state),),
    )


def _target(
    key: NativeKey = ("source", "variable", 101),
    *,
    register: NativeKey = ("source", "register", 1),
    kind: Literal[
        "register", "register_variant", "variable", "classification"
    ] = "variable",
) -> NativeNamingTarget:
    return NativeNamingTarget(
        kind=kind,
        provider="scb",
        source_key=key,
        register_key=register if kind in ("variable", "register_variant") else None,
        identity_revision=_revision(),
    )


def _binding(
    entry: AcceptedNamingEntry, target: NativeNamingTarget | None = None
) -> LegacyNamingBinding:
    return LegacyNamingBinding(
        kind=entry.entry.kind,
        provider=entry.entry.provider,
        source_id=entry.entry.source_id,
        target=target or _target(),
    )


def _record(
    cvid: int = 1001, var_id: int = 101, *, description: str = "label"
) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    row = _var_row(colname="COL", cvid=cvid, var_id=var_id, vardesc=description).split(
        "|"
    )
    cells: dict[str, tuple[bool, str | None, str]] = {
        name: (True, value, value) for name, value in zip(header, row, strict=True)
    }
    return clean_scb_row(header, 2, cells, _revision()).record


def test_pinned_reader_accounts_authored_generated_control_and_comparison(
    tmp_path: Path,
) -> None:
    (tmp_path / "scb.toml").write_text(
        '[variable."1.101"]\nslug = "authored"\ndeprecated = true\n'
    )
    (tmp_path / "scb.auto.toml").write_text('[variable."1.101"]\nslug = "generated"\n')
    (tmp_path / "classifications.toml").write_text(
        '[classification.SUN2020]\nslug="sun2020"\n'
    )
    (tmp_path / "freeze.toml").write_text('scb = "curating"\n')
    (tmp_path / ".snapshot.json").write_text(
        '{"variable": {"scb/1.101": "obsolete-baseline"}}'
    )
    pins = tuple(_revision(path) for path in tmp_path.iterdir())
    selection = read_naming_selection(tmp_path, pins)
    assert len(selection.entries) == 3
    assert {file.role for file in selection.files} == {
        "authored",
        "generated",
        "control",
        "comparison",
    }
    assert sum(file.entry_count for file in selection.files) == 5
    authored = next(
        entry
        for entry in selection.entries
        if entry.revision.artifact_path.endswith("/scb.toml")
    )
    assert authored.content_sha256 == canonical_sha256(
        {"slug": "authored", "deprecated": True}
    )
    assert authored.supplied_fields == ("deprecated", "slug")
    result = convert_naming(selection, [_binding(authored)])
    assert result.declarations[0].naming.slug == "authored"
    assert result.declarations[0].naming.deprecated
    assert len(result.dispositions) == 3
    assert {row.status for row in result.dispositions} == {
        "bound",
        "shadowed",
        "pending_source_binding",
    }
    assert "obsolete-baseline" not in result.model_dump_json()
    (tmp_path / "scb.toml").write_text('[variable."1.101"]\nslug = "modified"\n')
    with pytest.raises(NamingConversionError, match="declared revision"):
        read_naming_selection(tmp_path, pins)


def test_reader_rejects_unselected_files_and_invalid_inventory(tmp_path: Path) -> None:
    path = tmp_path / "scb.toml"
    path.write_text("")
    with pytest.raises(NamingConversionError, match="inventory"):
        read_naming_selection(tmp_path, ())
    with pytest.raises(NamingConversionError, match="unique"):
        read_naming_selection(tmp_path, (_revision(path), _revision(path)))


def test_absent_authored_slug_preserves_generated_name_and_authored_metadata() -> None:
    authored = _entry(slug=None, deprecated=True, replaced_by="1.102")
    generated = _entry(origin="generated")
    result = convert_naming(_selection(authored, generated), [_binding(authored)])
    assert result.complete
    (declaration,) = result.declarations
    assert declaration.naming.slug == "variable-a"
    assert declaration.naming.deprecated
    assert declaration.naming.replaced_by == "1.102"
    assert {entry.entry.slug for entry in declaration.contributors} == {
        None,
        "variable-a",
    }
    assert {row.status for row in result.dispositions} == {"bound"}
    without_auto = convert_naming(_selection(authored), [_binding(authored)])
    assert without_auto.declarations[0].naming.slug is None
    assert without_auto.declarations[0].contributors == (authored,)


def test_variant_metadata_and_typo_pointer_remain_exact_intent() -> None:
    authored = _entry(
        "1.10",
        "subset",
        kind="register_variant",
        display_group="Group",
        panel_entity_key=("person", "household"),
        panel_time_key="period",
        panel_time_grain="delivery",
        deprecated=True,
        replaced_by="1.11",
    )
    result = convert_naming(
        _selection(authored), [_binding(authored, _target(kind="register_variant"))]
    )
    assert result.complete
    assert result.declarations[0].naming == authored.entry


def test_churning_generated_files_are_accounted_but_never_converted() -> None:
    generated = _entry(origin="generated")
    result = convert_naming(_selection(generated, state="churning"), ())
    assert result.complete
    assert not result.declarations
    assert result.dispositions[0].status == "inactive_generated"


def test_missing_split_bridge_remains_engineering_conversion_gap() -> None:
    ordinary, split = _entry(), _entry("1.101.sibling", "sibling")
    result = convert_naming(_selection(ordinary, split), [_binding(ordinary)])
    assert len(result.declarations) == 1
    assert result.dispositions[1].status == "pending_source_binding"
    assert result.diagnostics[0].code == "naming_source_binding_pending"
    assert "split-member" in result.diagnostics[0].detail
    converted = convert_naming(
        _selection(split),
        [_binding(split, _target(("explicit-split", 101, "member:5")))],
    )
    assert converted.complete
    assert converted.declarations[0].naming.source_id == "1.101.sibling"


def test_nonidentical_duplicate_bindings_block_and_identical_duplicates_coalesce() -> (
    None
):
    entry = _entry()
    binding = _binding(entry)
    assert convert_naming(_selection(entry), (binding, binding)).complete
    changed_guard = binding.target.model_copy(
        update={
            "identity_revision": _revision().model_copy(
                update={"upstream_revision": "other"}
            )
        }
    )
    result = convert_naming(
        _selection(entry), (binding, _binding(entry, changed_guard))
    )
    assert not result.declarations
    assert result.dispositions[0].status == "blocked"
    assert result.diagnostics[0].code == "naming_binding_ambiguous"


def test_distinct_effective_names_for_one_exact_target_are_blocked() -> None:
    first, second = _entry(), _entry("1.102", "variable-b")
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second)]
    )
    assert not result.declarations
    assert {row.status for row in result.dispositions} == {"blocked"}
    assert result.diagnostics[0].code == "naming_target_multiple_bindings"


def test_identical_accepted_assignments_keep_every_contributor() -> None:
    first, second = _entry(), _entry("1.102")
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second)]
    )
    assert result.complete
    assert len(result.declarations) == 1
    assert result.declarations[0].contributors == (first, second)
    assert len(result.dispositions) == 2


def test_collisions_use_exact_register_scope_and_keep_integer_string_keys_distinct() -> (
    None
):
    first, second = _entry(), _entry("2.101")
    target_b = _target(("source", "variable", "101"))
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second, target_b)]
    )
    assert not result.declarations
    assert result.diagnostics[0].code == "naming_slug_collision"
    target_b = target_b.model_copy(update={"register_key": ("source", "register", 2)})
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second, target_b)]
    )
    assert result.complete
    assert len(result.declarations) == 2


def test_native_and_existing_hashed_bridges_reproduce_current_keys() -> None:
    assert native_scb_naming_id("register", 1) == "1"
    assert native_scb_naming_id("register_variant", 1, 10) == "1.10"
    assert native_scb_naming_id("variable", 1, 101) == "1.101"
    assert authored_naming_id("register", provider="sos", register_key="par") == str(
        mint("sos", "par")
    )
    assert (
        authored_naming_id(
            "register_variant",
            provider="sos",
            register_key="par",
            member_key="Slutenvård",
        )
        == f"{mint('sos', 'par')}.{mint('sos', 'par', 'Slutenvård')}"
    )
    assert (
        authored_naming_id(
            "variable", provider="fk", register_key="midas", member_key="BENAMNING"
        )
        == f"{mint('fk', 'midas')}.BENAMNING"
    )
    assert authored_naming_id(
        "register", provider="scb", register_key="utland", canonical_scb=True
    ) == str(mint_canonical_scb("scb", "utland"))
    with pytest.raises(NamingConversionError, match="split"):
        authored_naming_id(
            "variable", provider="sos", register_key="par", member_key="CODE.sibling"
        )
    with pytest.raises(NamingConversionError, match="explicit"):
        authored_naming_id("register", provider="scb", register_key="utland")
    with pytest.raises(NamingConversionError, match="integer"):
        native_scb_naming_id("register", True)


def test_naming_guards_check_identity_and_new_peers_without_unrelated_fields() -> None:
    record = _record()
    ref = SourceRecordRef(
        source=record.source, semantic_record_key=record.locators[0].semantic_record_key
    )
    expectation = RecordExpectation(
        ref=ref, alternatives=(RecordProjection(subject=record.subject),)
    )
    guard = PeerGuard(
        guard_id="variable-101",
        source=record.source,
        native=NativeCoordinates(register_id=1, variable_id=101),
        expected_members=(ref,),
    )
    target = NativeNamingTarget(
        kind="variable",
        provider="scb",
        source_key=(
            "test-source",
            "scb",
            "register",
            "native-int",
            1,
            "variable",
            "native-int",
            101,
        ),
        register_key=("test-source", "scb", "register", "native-int", 1),
        expectations=(expectation,),
        peer_guards=(guard,),
    )
    assert not check_naming_target(target, [record])
    assert not check_naming_target(
        target, [_record(description="changed unrelated prose")]
    )
    changed = check_naming_target(target, [_record(var_id=102)])
    assert changed and any(
        issue.applicability_issue and issue.applicability_issue.code == "target_missing"
        for issue in changed
    )
    added = check_naming_target(target, [record, _record(cvid=1002)])
    assert len(added) == 1
    assert added[0].applicability_issue is not None
    assert added[0].applicability_issue.code == "peer_membership_changed"
    with pytest.raises(ValidationError, match="cover exactly"):
        NativeNamingTarget.model_validate(
            {
                **target.model_dump(),
                "peer_guards": (
                    guard.model_copy(
                        update={
                            "expected_members": (
                                SourceRecordRef(
                                    source=record.source,
                                    semantic_record_key=("different",),
                                ),
                            )
                        }
                    ),
                ),
            }
        )


def test_source_artifact_identity_requires_the_exact_selected_revision() -> None:
    target = _target()
    assert not check_naming_target(target, (), revisions=(_revision(),))
    assert check_naming_target(target, ())[0].code == "naming_identity_revision_changed"
