"""Read accepted SCB errata declarations for input validation and conversion.

The version, delivered-column and new-column declarations preserve the original
accepted evidence. Offline conversion binds them to exact prepared source
occurrences and checked common-layer decisions; builds do not clone SQL rows.
"""

from __future__ import annotations

import functools
import json
import re
from dataclasses import dataclass, fields
from datetime import date
from typing import TYPE_CHECKING, cast

from ._curation import (
    curation_error,
    data_type_class,
    fold_column,
    require_bool,
    require_evidence,
    require_str,
    widen_data_type_classes,
)
from .curation_tree import load_classifications, load_register_files
from .edition_bounds import edition_claims
from .fqid_slugs import (
    _parse_register_id,
    _parse_variant_id,
    iter_curated_provider_entries,
)
from .normalization import normalize_text
from .source_coordinates import native_variant_key, source_register_key
from .source_curation import (
    CheckedFieldChange,
    CuratedOccurrenceAddition,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    capture_expectations,
    record_ref,
)
from .source_occurrences import source_occurrence
from .source_periods import source_scopes
from .source_records import (
    NativeCoordinates,
    SourceFields,
    TemporalScope,
    canonical_sha256,
    value_field,
)
from .sources.swecov_column_types import (
    SWECOV_COLUMN_TYPES_PATH,
    infer_steward_column_type,
    steward_column_storage_classes,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from .curation_tree import RegisterCuration
    from .source_coordinates import NativeKey
    from .source_curation import OccurrenceEffect, SourceRecordRef
    from .source_records import SourceRecord
    from .source_reference_records import SourceColumnTypeDeclaration

_FILE_NAME = "curation/registers/scb/<slug>.toml"
_CODE = "scb_errata_invalid"
# The provider whose export this surface corrects. `register` FQIDs are
# 2-segment and must name it: another provider's slug cannot resolve to an
# SCB (register, variant).
_PROVIDER = "scb"

_require_evidence = functools.partial(
    require_evidence,
    code=_CODE,
    prefix="scb_errata",
    file_name=_FILE_NAME,
)

# Where a `[[column]]`'s evidence comes from. Documentary only — the build mints
# the same rows either way — but it is the first thing whoever retires an entry
# needs, so it is a closed vocabulary rather than free text.
_SOURCES = frozenset({"scb-docs", "steward-holdings"})
_DEFAULT_DELIVERED_CLASS = "omitted-column-in-version"
_RESERVED_CORRECTION_CLASSES = frozenset(
    {"scoped-attributions", "overlapping-attributions"}
)

# `data_type` vocabulary, shared with `input_data/scb_canonical/scb_canonical.toml`
# (CanonicalScbAdapter). Stored verbatim on the synthetic row; the gate keeps a
# typo (`txt`, `int`) from shipping a meaningless type. Optional: a steward
# holdings list often names no type, and an absent one is a NULL state type.
_DATA_TYPES = frozenset({"text", "decimal", "integer", "date"})

# `variable.source_label` for a `[[column]]`-minted variable. ONE label for both
# `source` values: the fact ("SCB's export lacks this column") is the same one,
# while `source` becomes the state-provenance class researchers can inspect.
ERRATA_COLUMN_SOURCE_LABEL = "scb-errata"

_require_str = functools.partial(
    require_str, code=_CODE, prefix="scb_errata", file_name=_FILE_NAME
)
_require_bool = functools.partial(
    require_bool, code=_CODE, prefix="scb_errata", file_name=_FILE_NAME
)


@dataclass(frozen=True)
class ErrataVersion:
    """One `[[version]]`, resolved to SCB source ids. `name` is SCB's
    `Registerversionnamn` token verbatim."""

    register_variant_id: int
    name: str


@dataclass(frozen=True)
class ErrataDelivered:
    """One `[[delivered]]`, resolved to SCB source ids. `column` is the curator's
    delivery-column spelling (matched folded, like every curated column key);
    `versions` are `Registerversionnamn` tokens verbatim."""

    register_id: int
    register_variant_id: int
    column: str
    versions: tuple[str, ...]
    provenance: str
    native_variable_id: int | None = None


# The `[[column]]` fields that say WHERE the variable is delivered rather
# than what it is; everything else is `ErrataColumn.identity`.
_PER_VARIANT_FIELDS = frozenset(
    {
        "register_id",
        "register_variant_id",
        "versions",
        "holdings_period",
        "source",
        "provenance",
    }
)


@dataclass(frozen=True)
class ErrataColumn:
    """One `[[column]]`, resolved to SCB source ids: a column SCB documents
    nowhere on the variant, with the variable identity to mint for it.

    `versions` are `Registerversionnamn` tokens verbatim, `all_versions = true`
    leaves them None (every edition the variant has — the legacy undated reading
    of a steward holdings list, which states that the column is in the delivery
    without dating it), and `holdings_period` leaves them None while carrying the
    raw dataset-grain range (e.g. `"2002-2020"`) the column is held somewhere
    inside — never a claim it exists in every wave. `definition` becomes the variable's `description`, as the
    curated prose always has; `variable.definition` stays SCB's own export field
    and is left NULL.
    """

    register_id: int
    register_variant_id: int
    column: str
    name: str
    definition: str
    data_type: str | None
    classification: str | None
    is_identifier: bool
    is_sensitive: bool
    versions: tuple[str, ...] | None
    holdings_period: str | None
    source: str
    provenance: str

    @property
    def key(self) -> tuple[int, str]:
        """The minted variable's identity: `(register_id, folded column)`.

        NOT the variant — a column delivered on two variants of one register is
        ONE variable with a state per variant, exactly as a machine `var_id`
        spanning variants coalesces, and `variable.provider_key` (the column) is
        register-scoped so it could not be anything else."""
        return (self.register_id, fold_column(self.column))

    @property
    def identity(self) -> tuple:
        """Everything the minted variable IS, as opposed to where it is
        delivered. Two entries sharing a `key` must agree on all of it — they
        describe one variable, and whichever the materializer wrote first would
        silently decide. Derived from the fields rather than listed, so a new
        variable-grain field joins the agreement check by existing."""
        return tuple(
            getattr(self, f.name)
            for f in fields(self)
            if f.name not in _PER_VARIANT_FIELDS
        )


@dataclass(frozen=True)
class ScbErrata:
    """The loaded errata log. Empty when the file is absent (wheel installs,
    synthetic builds)."""

    versions: tuple[ErrataVersion, ...] = ()
    delivered: tuple[ErrataDelivered, ...] = ()
    columns: tuple[ErrataColumn, ...] = ()

    def __bool__(self) -> bool:
        return bool(self.versions or self.delivered or self.columns)


@dataclass(frozen=True)
class _CurationEntry:
    """One typed register-table value with its source coordinate."""

    values: dict[str, object]
    register_fqid: str
    source_file: str
    index: int


def _scb_slug_ids(slug_dir: Path | None) -> tuple[dict[str, int], dict[str, int]]:
    """`({register_slug: register_id}, {"<register_slug>/<variant_slug>":
    register_variant_id})` from the curated `scb.toml`.

    The errata applies inside the adapter, BEFORE `populate_slugs` — the DB's
    slug columns are still NULL there, so the curated TOML (the same source
    `populate_slugs` writes from) is what resolves the entries' FQIDs.
    """
    registers: dict[str, int] = {}
    variants: dict[str, int] = {}
    if slug_dir is None:
        return registers, variants
    # Deprecated entries are grow-only slug HISTORY, not live coordinates (the
    # rule `populate_slugs` and `slug_dir_curates_canonical_scb` both apply): a
    # retired slug must not shadow the live entry that replaced it.
    entries = [
        e
        for e in iter_curated_provider_entries(slug_dir)
        if e.provider == _PROVIDER and not e.deprecated
    ]
    by_register_id = {
        _parse_register_id(e.source_id): e.slug
        for e in entries
        if e.kind == "register" and e.slug
    }
    registers = {slug: rid for rid, slug in by_register_id.items()}
    for entry in entries:
        if entry.kind != "register_variant" or not entry.slug:
            continue
        register_id, variant_id = _parse_variant_id(entry.source_id)
        register_slug = by_register_id.get(register_id)
        if register_slug is not None:
            variants[f"{register_slug}/{entry.slug}"] = variant_id
    return registers, variants


def _entries(kind: str, registers: Sequence[RegisterCuration]) -> list[_CurationEntry]:
    """One entry kind flattened from the sorted register files."""
    entries: list[_CurationEntry] = []
    for register in registers:
        register_fqid = (
            f"{register.register_info.provider}/{register.register_info.slug}"
        )
        for index, entry in enumerate(getattr(register.errata, kind), start=1):
            entries.append(
                _CurationEntry(
                    values=entry.model_dump(mode="python", exclude_none=True),
                    register_fqid=register_fqid,
                    source_file=register.source_file,
                    index=index,
                )
            )
    return entries


def _state_provenance(class_name: str, evidence: str) -> str:
    """Stable, human-readable base provenance: class first, evidence after it.

    The first newline separates the class from free-text evidence; subsequent
    newlines remain part of that evidence. This keeps the catalog value useful
    as-is for CLI/JSON consumers while letting the SPA present the correction
    class and supporting evidence separately. When corrections overlap a
    provider-documented claim, the coalescer replaces this base form with a
    scoped-attributions value that retains every edition/evidence pair;
    correction-only overlaps use the same records under overlapping-attributions.
    """
    if class_name in _RESERVED_CORRECTION_CLASSES:
        raise curation_error(
            _CODE,
            f"scb_errata correction class {class_name!r} is reserved for "
            "builder-generated scoped provenance.",
            "Use a specific upstream correction class instead.",
        )
    if "\n" in class_name or "\r" in class_name:
        raise curation_error(
            _CODE,
            "scb_errata `upstream` correction class cannot contain line breaks.",
            "Keep that value on one line.",
        )
    return f"errata:{class_name}\n{evidence}"


def scoped_state_provenance(
    attributions: list[tuple[str, str]], *, provider_documented: bool = True
) -> str:
    """Encode ordered correction provenance with explicit source-edition scope."""
    grouped: dict[str, list[str]] = {}
    for provenance, edition in attributions:
        editions = grouped.setdefault(provenance, [])
        if edition not in editions:
            editions.append(edition)

    records = []
    for provenance, editions in grouped.items():
        header, evidence = provenance.split("\n", maxsplit=1)
        records.append(
            {
                "class": header.removeprefix("errata:"),
                "evidence": evidence,
                "source_editions": editions,
            }
        )
    payload = json.dumps(
        records, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    kind = "scoped-attributions" if provider_documented else "overlapping-attributions"
    return f"errata:{kind}\n{payload}"


def _resolve_variant(
    entry: _CurationEntry,
    table: str,
    registers: dict[str, int],
    variants: dict[str, int],
) -> tuple[int, int, str]:
    """`(register_id, register_variant_id, "<register_fqid>/<variant>")` for an
    entry's register-file coordinate and `variant`."""
    provider, register = entry.register_fqid.split("/")
    context = (
        f"{entry.source_file} [[errata.{table}]] entry {entry.index} "
        f"({entry.register_fqid})"
    )
    variant = _require_str(entry.values, "variant", context)
    context = f"{context}/{variant}"
    if provider != _PROVIDER:
        raise curation_error(
            "scb_errata_unknown_variant",
            f"scb_errata {context} names provider {provider!r}; this surface "
            f"corrects the {_PROVIDER!r} export only.",
            f"Move the entry to the {provider!r} adapter's own curation, or fix "
            f"the FQID in reg_meta_build/{_FILE_NAME}.",
        )
    register_id = registers.get(register)
    variant_id = variants.get(f"{register}/{variant}")
    if register_id is None or variant_id is None:
        raise curation_error(
            "scb_errata_unknown_variant",
            f"scb_errata {context} does not name a curated SCB "
            f"{'register' if register_id is None else 'register variant'}.",
            "Use [register] / [[variant]] slugs in the owning register file.",
        )
    return register_id, variant_id, context


def load_scb_errata(
    path: Path | None,
    slug_dir: Path | None,
    *,
    classifications: frozenset[str] | None = None,
) -> ScbErrata:
    """Parse register-scoped errata, resolving each entry's `variant` slug
    against the curated `scb.toml` in `slug_dir`. ``path`` is the curation root.
    Empty when no curation tree (synthetic
    builds, wheel installs).

    The supplied classification names, or the classifications under ``path``,
    are consulted only when a `[[column]]` names a classification. This lets a
    candidate curation tree introduce a book without depending on the checkout.

    Strict load, all EXIT_CONFIG with a remediation: only `[[errata.version]]` /
    `[[errata.delivered]]` / `[[errata.column]]` register-file tables; no unknown
    key inside an entry; the register is implied by its file and its `variant`
    must be curated; `evidence`
    and `noted` (canonical `YYYY-MM-DD`) present; a `[[version]]` name carrying
    a parseable claimed year; a `[[column]]` carrying exactly one of `versions`
    (a non-empty list of non-empty strings naming each version at most once),
    `all_versions = true`, or `holdings_period` (an ordered `YYYY-YYYY` year
    range, or `YYYY-MM-DD/YYYY-MM-DD` dates); and no duplicate
    `(variant, name)` / `(variant, column)` entry — two entries for one column
    must be ONE entry listing both versions, or the log stops being readable as
    the record of what SCB missed. `[[delivered]]` and `[[column]]` share that
    column key: a column is one kind of omission or the other, never both.
    """
    registers_curation = load_register_files(path) if path is not None else ()
    version_entries = _entries("version", registers_curation)
    delivered_entries = _entries("delivered", registers_curation)
    column_entries = _entries("column", registers_curation)
    if not version_entries and not delivered_entries and not column_entries:
        # Before touching the slug dir: resolving FQIDs parses the whole
        # curated scb.toml (~20k entries), and the common case — no file, or a
        # build whose provider set never reaches it — has nothing to resolve.
        return ScbErrata()
    registers, variants = _scb_slug_ids(slug_dir)
    # Register-scoped curation now carries the same native coordinates that the
    # former provider-wide slug file supplied.
    for register in registers_curation:
        if register.register_info.provider != _PROVIDER:
            continue
        native_id = register.register_info.native_id
        if native_id is None:
            raise ValueError(f"{register.source_file}: SCB register has no native_id")
        register_id = int(native_id)
        registers[register.register_info.slug] = register_id
        for variant in register.variant:
            native_register, native_variant = _parse_variant_id(variant.native_id)
            if native_register != register_id:
                raise ValueError(
                    f"{register.source_file}: variant {variant.native_id} belongs to "
                    f"register {native_register}, expected {register_id}"
                )
            variants[f"{register.register_info.slug}/{variant.slug}"] = native_variant

    versions: list[ErrataVersion] = []
    seen_versions: set[tuple[int, str]] = set()
    for entry in version_entries:
        _, variant_id, context = _resolve_variant(entry, "version", registers, variants)
        values = entry.values
        name = _require_str(values, "name", context)
        _require_evidence(values, f"{context}/{name}")
        if not _edition_years(name):
            raise curation_error(
                "scb_errata_version_year_unknown",
                f"{context}/{name} has no parseable claimed year.",
                "Use SCB's exact version name containing a four-digit year so "
                "the coalescer can place the edition chronologically.",
            )
        if (variant_id, name) in seen_versions:
            raise curation_error(
                _CODE,
                f"{context}: duplicate version {name!r}.",
                "Each (register, variant, name) may appear once.",
            )
        seen_versions.add((variant_id, name))
        versions.append(ErrataVersion(variant_id, name))

    delivered: list[ErrataDelivered] = []
    seen_columns: set[tuple[int, str]] = set()
    for entry in delivered_entries:
        register_id, variant_id, context = _resolve_variant(
            entry, "delivered", registers, variants
        )
        values = entry.values
        column = _require_str(values, "column", context)
        ctx = f"{context}/{column}"
        evidence = _require_evidence(values, ctx)
        named = _named_versions(values, ctx)
        upstream = (
            _require_str(values, "upstream", ctx)
            if "upstream" in values
            else _DEFAULT_DELIVERED_CLASS
        )
        key = (variant_id, fold_column(column))
        if key in seen_columns:
            raise curation_error(
                _CODE,
                f"{context}: duplicate delivered column {column!r}.",
                "Each (register, variant, column) may appear once — list every "
                "omitted version in that entry's `versions`.",
            )
        seen_columns.add(key)
        delivered.append(
            ErrataDelivered(
                register_id,
                variant_id,
                column,
                named,
                _state_provenance(upstream, evidence),
                cast("int | None", values.get("native_variable_id")),
            )
        )

    columns: list[ErrataColumn] = []
    # The variable each `[[column]]` key mints, so a column declared on two
    # variants of one register is checked to describe the SAME variable — it
    # will BE one (`ErrataColumn.key`), and a disagreement would silently ship
    # whichever entry the materializer wrote first.
    identities: dict[tuple[int, str], tuple] = {}
    declared_classifications: frozenset[str] | None = None
    for entry in column_entries:
        register_id, variant_id, context = _resolve_variant(
            entry, "column", registers, variants
        )
        values = entry.values
        column = _require_str(values, "column", context)
        ctx = f"{context}/{column}"
        source = _column_source(values, ctx)
        evidence = _require_evidence(values, ctx)
        placement = _column_placement(values, ctx)
        classification = _column_classification(values, ctx)
        if classification is not None:
            if declared_classifications is None:
                declared_classifications = (
                    classifications
                    if classifications is not None
                    else frozenset(
                        item.classification.short_name
                        for item in load_classifications(path)
                    )
                    if path is not None
                    else frozenset()
                )
            if classification not in declared_classifications:
                raise curation_error(
                    _CODE,
                    f"scb_errata {ctx} names undeclared classification "
                    f"{classification!r}.",
                    "Use an existing classification short_name (e.g. 'SSYK96') "
                    "or declare it in reg_meta_build/curation/classifications/.",
                )
        loaded = ErrataColumn(
            register_id=register_id,
            register_variant_id=variant_id,
            column=column,
            name=_require_str(values, "name", ctx),
            definition=_require_str(values, "definition", ctx),
            data_type=_column_data_type(values, ctx),
            classification=classification,
            is_identifier=_require_bool(values, "is_identifier", ctx),
            is_sensitive=_require_bool(values, "is_sensitive", ctx),
            versions=placement[0],
            holdings_period=placement[1],
            source=source,
            provenance=_state_provenance(source, evidence),
        )
        key = (variant_id, fold_column(column))
        if key in seen_columns:
            raise curation_error(
                _CODE,
                f"{context}: duplicate column entry for {column!r}.",
                "Each (register, variant, column) may appear once, in ONE of "
                "[[errata.delivered]] (SCB documents the column elsewhere on the "
                "variant) or [[errata.column]] (it documents it nowhere).",
            )
        seen_columns.add(key)
        if identities.setdefault(loaded.key, loaded.identity) != loaded.identity:
            raise curation_error(
                _CODE,
                f"scb_errata {ctx} describes a different variable than the "
                f"other [[column]] entry for column {column!r} in the same "
                "register.",
                "A column delivered on two variants of one register is ONE "
                "variable: give both entries the same column spelling, name, "
                "definition, data_type, classification and PII flags, or "
                "rename one of the columns.",
            )
        columns.append(loaded)

    return ScbErrata(tuple(versions), tuple(delivered), tuple(columns))


def _named_versions(entry: dict, ctx: str) -> tuple[str, ...]:
    """An entry's `versions` list: non-empty, every member a non-empty
    `Registerversionnamn` string, each named at most once (a second mention
    would mint the same synthetic row twice and die on the id collision
    mid-insert — caught here, where the maintainer gets a remediation)."""
    raw = entry.get("versions")
    if (
        not isinstance(raw, list)
        or not raw
        or not all(isinstance(v, str) and v.strip() for v in raw)
    ):
        raise curation_error(
            _CODE,
            f"scb_errata {ctx} needs `versions` as a non-empty list of "
            f"`Registerversionnamn` strings, got {raw!r}.",
            'Give `versions = ["2010", "2011"]` — SCB version names verbatim, '
            "never dates.",
        )
    named = tuple(v.strip() for v in raw)
    repeated = sorted({n for n in named if named.count(n) > 1})
    if repeated:
        raise curation_error(
            _CODE,
            f"scb_errata {ctx} repeats version(s) {repeated} in `versions`.",
            "Each version may appear once in an entry's `versions` — the "
            "second mention would mint the same synthetic row twice.",
        )
    return named


_YEAR_RANGE = re.compile(r"^(\d{4})-(\d{4})$")
_DATE_RANGE = re.compile(r"^(\d{4}-\d{2}-\d{2})/(\d{4}-\d{2}-\d{2})$")


def holdings_period_bounds(raw: str) -> tuple[str, str]:
    """A `holdings_period`'s inclusive `(pooled_start, pooled_end)` ISO dates.

    Either whole years (`"2002-2020"` → `"2002-01-01"` .. `"2020-12-31"`) or
    exact dates (`"2002-01-01/2020-12-31"`, verbatim). Anything else — and a
    range ending before it starts — is a load-time refusal, where the maintainer
    gets a remediation instead of an unresolvable occurrence."""
    match = _YEAR_RANGE.match(raw)
    if match is not None:
        start_year, end_year = match.groups()
        if end_year < start_year:
            raise curation_error(
                _CODE,
                f"scb_errata holdings_period {raw!r} ends before it starts.",
                'Give the range oldest-first, e.g. `holdings_period = "2002-2020"`.',
            )
        return f"{start_year}-01-01", f"{end_year}-12-31"
    match = _DATE_RANGE.match(raw)
    if match is not None:
        start, end = match.groups()
        try:
            ordered = date.fromisoformat(start) <= date.fromisoformat(end)
        except ValueError:
            ordered = False
        if not ordered:
            raise curation_error(
                _CODE,
                f"scb_errata holdings_period {raw!r} is not an ordered "
                "`YYYY-MM-DD/YYYY-MM-DD` range.",
                "Give the range oldest-first, e.g. "
                '`holdings_period = "2002-01-01/2020-12-31"`.',
            )
        return start, end
    raise curation_error(
        _CODE,
        f"scb_errata holdings_period {raw!r} is not a `YYYY-YYYY` year range "
        "or a `YYYY-MM-DD/YYYY-MM-DD` date range.",
        'Give e.g. `holdings_period = "2002-2020"` — the dataset-grain years '
        "the steward holds the column somewhere inside.",
    )


def _column_placement(
    entry: dict, ctx: str
) -> tuple[tuple[str, ...] | None, str | None]:
    """A `[[column]]`'s placement: `(named versions, holdings raw)`.

    Exactly one of `versions`, `all_versions = true`, `holdings_period`.
    `all_versions` is the undated holdings claim — the column is in the delivery,
    undated — and naming every edition instead would make the entry rot the next
    time SCB ships one. `holdings_period` dates that same claim at dataset grain:
    the column is held somewhere inside the range, never in every wave, so
    conversion keeps it as one pooled range instead of per-edition claims."""
    all_versions = _require_bool(entry, "all_versions", ctx)
    forms = [
        name
        for name in ("versions", "all_versions", "holdings_period")
        if name in entry
    ]
    if len(forms) != 1:
        raise curation_error(
            _CODE,
            f"scb_errata {ctx} needs exactly one of `versions`, "
            f"`all_versions = true`, `holdings_period`, not {forms}.",
            'Name the editions the column was delivered in (`versions = ["2010"]`), '
            "declare it delivered in every edition of the variant "
            "(`all_versions = true`), or date the steward holding at dataset grain "
            '(`holdings_period = "2002-2020"`).',
        )
    if "holdings_period" in entry:
        raw = _require_str(entry, "holdings_period", ctx)
        holdings_period_bounds(raw)  # fail fast: refuse the bad range here
        return None, raw
    if "all_versions" in entry:
        if not all_versions:
            raise curation_error(
                _CODE,
                f"scb_errata {ctx}: `all_versions` must be `true` when present.",
                "Remove the key or set `all_versions = true` — an undated holding "
                "covers every edition of the variant.",
            )
        return None, None
    return _named_versions(entry, ctx), None


def _column_source(entry: dict, ctx: str) -> str:
    source = _require_str(entry, "source", ctx)
    if source not in _SOURCES:
        raise curation_error(
            _CODE,
            f"scb_errata {ctx}: source {source!r} is not one of {sorted(_SOURCES)}.",
            "Say where the evidence comes from: `scb-docs` (SCB's own "
            "documentation) or `steward-holdings` (a steward's delivery list).",
        )
    return source


def _column_data_type(entry: dict, ctx: str) -> str | None:
    """A `[[column]]`'s optional `data_type`, gated on the canonical vocabulary.
    Absent → None (a NULL state type): a steward holdings list routinely carries
    no type, and inventing one would publish a guess as a fact."""
    if "data_type" not in entry:
        return None
    data_type = _require_str(entry, "data_type", ctx)
    if data_type not in _DATA_TYPES:
        raise curation_error(
            _CODE,
            f"scb_errata {ctx}: data_type {data_type!r} is not one of "
            f"{sorted(_DATA_TYPES)}.",
            f"Use a canonical data_type: {sorted(_DATA_TYPES)}, or omit the key.",
        )
    return data_type


def _column_classification(entry: dict, ctx: str) -> str | None:
    if "classification" not in entry:
        return None
    return _require_str(entry, "classification", ctx)


# Columns cloned from a real source row onto the synthetic one. `cvid` /
# `regver_id` are minted; `classification_id` and `variable_id` stay NULL —
# they are stamped by later passes that see the synthetic row like any other.


def _edition_years(name: str) -> tuple[int, ...]:
    """Read the declared edition's years to validate its source coordinate."""
    return tuple(year for year, _lo, _hi in edition_claims(name))


@dataclass(frozen=True)
class ErrataEditionBinding:
    """Already-established edition, including an existing declared missing edition."""

    key: NativeKey
    name: str
    edition_scope: TemporalScope
    edition_period_scope: TemporalScope
    # Existing editions use their member rows; declared editions use the finite
    # native variant evidence checked by the separate edition declaration.
    support: tuple[SourceRecordRef, ...]
    native_id: int | None = None


@dataclass(frozen=True)
class ErrataConversion:
    case: CurationCase | None
    blockers: tuple[str, ...]
    identity_refs: tuple[SourceRecordRef, ...]


def edition_bindings(
    records: tuple[SourceRecord, ...], versions: tuple[ErrataVersion, ...]
) -> tuple[ErrataEditionBinding, ...]:
    """Bind native and accepted missing editions once for a complete variant."""
    if not records:
        raise ValueError("errata edition bindings require a native variant slice")
    variant_key = native_variant_key(records[0])
    assert variant_key is not None
    by_id: dict[int, ErrataEditionBinding] = {}
    for record in sorted(records, key=lambda item: str(record_ref(item))):
        native_id = record.subject.native.edition_id
        if native_id is None:
            continue
        key = source_occurrence(record).edition_key
        name = record.original_period_text
        if key is None or name is None:
            raise ValueError(f"native edition {native_id} has no checked key or name")
        binding = ErrataEditionBinding(
            key=key,
            name=name,
            edition_scope=record.edition_scope,
            edition_period_scope=record.edition_period_scope,
            support=(record_ref(record),),
            native_id=native_id,
        )
        previous = by_id.setdefault(native_id, binding)
        if (
            previous.key,
            previous.name,
            previous.edition_scope,
            previous.edition_period_scope,
        ) != (
            binding.key,
            binding.name,
            binding.edition_scope,
            binding.edition_period_scope,
        ):
            raise ValueError(
                f"native edition {native_id} has conflicting interpretations"
            )
    names = {item.name for item in by_id.values()}
    bindings = list(by_id.values())
    for version in versions:
        if version.name in names:
            raise ValueError(
                f"declared missing edition {version.name!r} is already native"
            )
        names.add(version.name)
        edition_scope, period_scope, _ = source_scopes(version.name)
        bindings.append(
            ErrataEditionBinding(
                key=(*variant_key, "accepted-edition", version.name),
                name=version.name,
                edition_scope=edition_scope,
                edition_period_scope=period_scope,
                support=tuple(sorted({record_ref(r) for r in records}, key=str)),
            )
        )
    return tuple(sorted(bindings, key=lambda item: (item.name, repr(item.key))))


def _text(record: SourceRecord, field: str) -> str | None:
    claim = getattr(record.fields, field)
    return claim.value if claim is not None and claim.status == "value" else None


def _variant_records(
    records: tuple[SourceRecord, ...],
    register_id: int,
    variant_id: int,
    editions: tuple[ErrataEditionBinding, ...],
) -> None:
    if not records or len({record.source for record in records}) != 1:
        raise ValueError("conversion requires a complete single-source variant slice")
    if any(
        record.subject.provider != "scb"
        or record.subject.native.register_id != register_id
        or record.subject.native.register_variant_id != variant_id
        for record in records
    ):
        raise ValueError(
            "source slice differs from the accepted errata register/variant"
        )
    refs = {record_ref(record) for record in records}
    if any(
        not edition.support or not set(edition.support) <= refs for edition in editions
    ):
        raise ValueError(
            "edition bindings require checked evidence in the supplied slice"
        )


def convert_delivered_entry(
    entry: ErrataDelivered,
    *,
    case_id: str,
    records: tuple[SourceRecord, ...],
    editions: tuple[ErrataEditionBinding, ...],
    steward_table_prefixes: tuple[str, ...] = (),
    storage_columns: dict[tuple[str, str], SourceColumnTypeDeclaration] | None = None,
) -> ErrataConversion:
    """Retain the availability stated by an accepted omission declaration.

    An elsewhere-documented column must identify one source-native variable.
    Its documented type and matching storage schemas supply widened type evidence.
    Proximity in time never proves flags or code membership.
    """
    _variant_records(records, entry.register_id, entry.register_variant_id, editions)
    for name in entry.versions:
        if not any(edition.name == name for edition in editions):
            raise ValueError(f"missing edition conversion binding: {name!r}")
    documented = tuple(
        record
        for record in records
        if (column := _text(record, "column_name"))
        and fold_column(column) == fold_column(entry.column)
    )
    if not documented:
        return ErrataConversion(None, ("no_documented_column_identity",), ())
    candidates = (
        tuple(
            record
            for record in documented
            if record.subject.native.variable_id == entry.native_variable_id
        )
        if entry.native_variable_id is not None
        else documented
    )
    if not candidates:
        return ErrataConversion(
            None,
            ("native_variable_id_not_documented_for_column",),
            tuple(sorted({record_ref(record) for record in documented}, key=str)),
        )
    references = tuple(sorted({record_ref(record) for record in candidates}, key=str))
    identities = {source_occurrence(record).variable_key for record in candidates}
    if None in identities or len(identities) != 1:
        return ErrataConversion(
            None, ("ambiguous_documented_column_identity",), references
        )
    original = source_occurrence(candidates[0])
    assert original.variable_key is not None and original.variant_key is not None
    native = candidates[0].subject.native
    documented_values = tuple(
        sorted(
            (
                (
                    record.original_period_text
                    or str(record.subject.native.edition_id),
                    _text(record, "data_type"),
                )
                for record in candidates
            ),
            key=lambda item: (item[0], item[1] or ""),
        )
    )
    documented_classes = tuple(
        data_type_class(value) for _, value in documented_values if value is not None
    )
    storage_classes, storage_evidence = steward_column_storage_classes(
        entry.column, steward_table_prefixes, storage_columns or {}
    )
    inferred_type = (
        widen_data_type_classes(
            (
                *storage_classes,
                *(kind for kind in documented_classes if kind is not None),
            )
        )
        if None not in documented_classes
        else None
    )
    provenance = "\n".join(
        (
            entry.provenance,
            storage_evidence or f"SWECOV storage {SWECOV_COLUMN_TYPES_PATH}: none",
            "Documented Datatyp: "
            + ", ".join(
                f"{edition}={value or 'none'}" for edition, value in documented_values
            ),
        )
    )
    effects: list[OccurrenceEffect] = []
    targets: set[SourceRecordRef] = set()
    blockers = []
    guards = [
        PeerGuard(
            guard_id=f"{case_id}:documented-column",
            source=records[0].source,
            native=NativeCoordinates(
                register_id=entry.register_id,
                register_variant_id=entry.register_variant_id,
                variable_id=entry.native_variable_id,
            ),
            folded_column=fold_column(entry.column),
            expected_members=references,
        )
    ]
    for edition in (item for item in editions if item.name in entry.versions):
        if edition.native_id is not None and any(
            record.subject.native.edition_id == edition.native_id
            for record in candidates
        ):
            blockers.append(f"now_present:{edition.name}")
            continue
        matching = tuple(
            record
            for record in records
            if record.subject.native.variable_id == native.variable_id
            and (
                record.subject.native.edition_id == edition.native_id
                if edition.native_id is not None
                else record.edition_scope == edition.edition_scope
            )
        )
        guards.append(
            PeerGuard(
                guard_id=f"{case_id}:{edition.name}:variable:{native.variable_id}",
                source=records[0].source,
                native=NativeCoordinates(
                    register_id=entry.register_id,
                    register_variant_id=entry.register_variant_id,
                    edition_id=edition.native_id,
                    variable_id=native.variable_id,
                ),
                edition_scopes=()
                if edition.native_id is not None
                else (edition.edition_scope,),
                expected_members=tuple(
                    sorted({record_ref(record) for record in matching}, key=str)
                ),
            )
        )
        if len({record.subject.native.member_id for record in matching}) > 1:
            blockers.append(
                f"ambiguous_target:{edition.name}:variable:{native.variable_id}"
            )
            continue
        if any(_text(record, "column_name") for record in matching):
            blockers.append(
                f"target_under_other_column:{edition.name}:variable:{native.variable_id}"
            )
            continue
        if matching:
            for ref in sorted({record_ref(record) for record in matching}, key=str):
                targets.add(ref)
                effects.append(
                    CheckedFieldChange(
                        ref=ref,
                        replacement=FieldExpectation(
                            name="column_name", status="value", value=entry.column
                        ),
                    )
                )
                if inferred_type is not None:
                    effects.append(
                        CheckedFieldChange(
                            ref=ref,
                            replacement=FieldExpectation(
                                name="data_type", status="value", value=inferred_type
                            ),
                        )
                    )
            continue
        targets.update(references)
        effects.append(
            CuratedOccurrenceAddition(
                occurrence_key=f"{case_id}:{canonical_sha256([list(edition.key), entry.column])}",
                provider="scb",
                variable_key=original.variable_key,
                variant_key=original.variant_key,
                edition_key=edition.key,
                fields=SourceFields(
                    availability=value_field(True),
                    column_name=value_field(entry.column),
                    data_type=value_field(inferred_type)
                    if inferred_type is not None
                    else None,
                ),
                edition_scope=edition.edition_scope,
                edition_period_scope=edition.edition_period_scope,
                evidence=tuple(sorted({*references, *edition.support}, key=str)),
            )
        )
    if blockers:
        return ErrataConversion(None, tuple(sorted(set(blockers))), references)
    required = (
        targets
        | set(references)
        | {
            ref
            for edition in editions
            if edition.name in entry.versions
            for ref in edition.support
        }
    )
    # Documented type is a dependency: changing it makes the decision stale.
    # Flags and coding are never copied from adjacent editions.
    expected = capture_expectations(
        tuple(record for record in records if record_ref(record) in required),
        fields=("column_name", "data_type"),
    )
    case = CurationCase(
        case_id=case_id,
        targets=tuple(item for item in expected if item.ref in targets),
        support=tuple(item for item in expected if item.ref not in targets),
        peer_guards=tuple(guards),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(effects),
            reason=entry.provenance,
            provenance=provenance,
        ),
    )
    return ErrataConversion(case, (), references)


def convert_column_entry(
    entry: ErrataColumn,
    *,
    case_id: str,
    records: tuple[SourceRecord, ...],
    editions: tuple[ErrataEditionBinding, ...],
    declared_flags: frozenset[str],
    steward_table_prefixes: tuple[str, ...] = (),
    storage_columns: dict[tuple[str, str], SourceColumnTypeDeclaration] | None = None,
) -> ErrataConversion:
    """Retain supplied facts; infer only uncurated steward-held storage types.

    The old ``all_versions`` interpretation of undated holdings supplies no annual
    evidence. Retain one undated occurrence, not a claim for each existing edition.
    A ``holdings_period`` instead dates the holding at dataset grain: retain one
    pooled-range occurrence over the whole range — the delivery list says the
    column is held somewhere inside it, never that it exists in every wave.
    ``declared_flags`` names the keys actually present in the original TOML, since
    the legacy loader has already replaced omitted flags with false. Steward-held
    columns use false for missing flags; curated values still win.
    Classification references remain declarations for common binding; canonical
    code memberships are never copied into these occurrences.
    """
    if not declared_flags <= {"is_identifier", "is_sensitive"}:
        raise ValueError("declared_flags must name original boolean declaration keys")
    _variant_records(records, entry.register_id, entry.register_variant_id, editions)
    if any(
        (column := _text(record, "column_name"))
        and fold_column(column) == fold_column(entry.column)
        for record in records
    ):
        return ErrataConversion(None, ("column_now_documented",), ())
    names = set(entry.versions or ())
    missing = names - {edition.name for edition in editions}
    if missing or entry.versions == ():
        raise ValueError(
            f"missing column edition conversion bindings: {sorted(missing)!r}"
        )
    selected = tuple(edition for edition in editions if edition.name in names)
    references = {ref for edition in selected for ref in edition.support}
    if entry.versions is None:
        # This member establishes the native variant coordinate only. It says
        # nothing about when the independently declared column was delivered.
        references.add(record_ref(records[0]))
    anchors = tuple(record for record in records if record_ref(record) in references)
    expected = capture_expectations(anchors, fields=("availability",))
    guards = [
        PeerGuard(
            guard_id=f"{case_id}:column-absent",
            source=records[0].source,
            native=NativeCoordinates(
                register_id=entry.register_id,
                register_variant_id=entry.register_variant_id,
            ),
            folded_column=fold_column(entry.column),
            expected_members=(),
        )
    ]
    for ref in sorted(references, key=str):
        anchor = next(record for record in anchors if record_ref(record) == ref)
        guards.append(
            PeerGuard(
                guard_id=f"{case_id}:edition:{ref.semantic_record_key!r}",
                source=anchor.source,
                native=anchor.subject.native,
                expected_members=(ref,),
            )
        )
    register = source_register_key(records[0])
    variant = native_variant_key(records[0])
    assert register is not None and variant is not None
    variable = (*register, "declared-column", fold_column(entry.column))
    inferred_type = None
    storage_evidence = None
    if (
        entry.source == "steward-holdings"
        and entry.data_type is None
        and storage_columns
    ):
        inferred_type, storage_evidence = infer_steward_column_type(
            entry.column, steward_table_prefixes, storage_columns
        )
    provenance = (
        f"{entry.provenance}\n{storage_evidence}"
        if storage_evidence is not None
        else entry.provenance
    )
    fields = SourceFields(
        availability=value_field(True),
        column_name=value_field(entry.column),
        name=value_field(normalize_text(entry.name)),
        description=value_field(normalize_text(entry.definition, multiline=True)),
        data_type=(
            value_field(entry.data_type)
            if entry.data_type is not None
            else value_field(inferred_type)
            if inferred_type is not None
            else None
        ),
        classification_declared=value_field(entry.classification)
        if entry.classification is not None
        else None,
        identifier=value_field(entry.is_identifier)
        if "is_identifier" in declared_flags or entry.source == "steward-holdings"
        else None,
        sensitivity=value_field(entry.is_sensitive)
        if "is_sensitive" in declared_flags or entry.source == "steward-holdings"
        else None,
    )
    effects: tuple[OccurrenceEffect, ...] = tuple(
        CuratedOccurrenceAddition(
            occurrence_key=f"{case_id}:{canonical_sha256(list(edition.key))}",
            provider="scb",
            variable_key=variable,
            variant_key=variant,
            edition_key=edition.key,
            fields=fields,
            edition_scope=edition.edition_scope,
            edition_period_scope=edition.edition_period_scope,
            evidence=edition.support,
        )
        for edition in selected
    )
    if entry.versions is None and entry.holdings_period is None:
        effects = (
            CuratedOccurrenceAddition(
                occurrence_key=f"{case_id}:undated",
                provider="scb",
                variable_key=variable,
                variant_key=variant,
                edition_key=None,
                fields=fields,
                edition_scope=TemporalScope(
                    kind="unknown",
                    label="Undated holdings; legacy all_versions is not annual evidence",
                ),
                edition_period_scope=TemporalScope(kind="not_applicable"),
                evidence=tuple(sorted(references, key=str)),
            ),
        )
    elif entry.holdings_period is not None:
        start, end = holdings_period_bounds(entry.holdings_period)
        pooled = TemporalScope(
            kind="pooled",
            label=entry.holdings_period,
            pooled_start=start,
            pooled_end=end,
        )
        effects = (
            CuratedOccurrenceAddition(
                occurrence_key=f"{case_id}:holdings",
                provider="scb",
                variable_key=variable,
                variant_key=variant,
                edition_key=None,
                fields=fields,
                edition_scope=pooled,
                edition_period_scope=pooled,
                evidence=tuple(sorted(references, key=str)),
            ),
        )
    case = CurationCase(
        case_id=case_id,
        targets=expected,
        peer_guards=tuple(guards),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=effects,
            reason=provenance,
            provenance=provenance,
        ),
    )
    return ErrataConversion(case, (), ())
