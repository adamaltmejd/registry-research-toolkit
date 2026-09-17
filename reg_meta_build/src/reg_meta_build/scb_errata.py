"""Read accepted SCB errata declarations for input validation and conversion.

The version, delivered-column and new-column declarations preserve the original
accepted evidence. Offline conversion binds them to exact prepared source
occurrences and checked common-layer decisions; builds do not clone SQL rows.
"""

from __future__ import annotations

import functools
import json
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    fold_column,
    load_curation_entries,
    require_bool,
    require_evidence,
    require_str,
)
from .classifications import declared_short_names
from .edition_bounds import edition_claims
from .fqid_slugs import (
    PROVIDER_FILE_SUFFIX,
    _parse_register_id,
    _parse_variant_id,
    load_provider_toml,
)

if TYPE_CHECKING:
    from pathlib import Path

_FILE_NAME = "curation/scb_errata.toml"
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

_VERSION_FIELDS = frozenset({"register", "variant", "name", "evidence", "noted"})
_DELIVERED_FIELDS = frozenset(
    {"register", "variant", "column", "versions", "evidence", "noted", "upstream"}
)
_COLUMN_FIELDS = frozenset(
    {
        "register",
        "variant",
        "column",
        "name",
        "definition",
        "data_type",
        "classification",
        "is_identifier",
        "is_sensitive",
        "versions",
        "all_versions",
        "source",
        "evidence",
        "noted",
    }
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


# The `[[column]]` fields that say WHERE the variable is delivered rather
# than what it is; everything else is `ErrataColumn.identity`.
_PER_VARIANT_FIELDS = frozenset(
    {"register_id", "register_variant_id", "versions", "source", "provenance"}
)


@dataclass(frozen=True)
class ErrataColumn:
    """One `[[column]]`, resolved to SCB source ids: a column SCB documents
    nowhere on the variant, with the variable identity to mint for it.

    `versions` are `Registerversionnamn` tokens verbatim, or None for
    `all_versions = true` (every edition the variant has — the faithful reading
    of a steward holdings list, which states that the column is in the delivery
    without dating it). `definition` becomes the variable's `description`, as the
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
    path = slug_dir / f"{_PROVIDER}{PROVIDER_FILE_SUFFIX}"
    if not path.is_file():
        return registers, variants
    # Deprecated entries are grow-only slug HISTORY, not live coordinates (the
    # rule `populate_slugs` and `slug_dir_curates_canonical_scb` both apply): a
    # retired slug must not shadow the live entry that replaced it.
    entries = [e for e in load_provider_toml(path) if not e.deprecated]
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


def _unknown_keys(entry: dict, allowed: frozenset[str], table: str) -> None:
    unknown = sorted(set(entry) - allowed)
    if unknown:
        raise curation_error(
            _CODE,
            f"scb_errata [[{table}]] entry has unknown key(s): {unknown}.",
            f"A [[{table}]] entry takes only {sorted(allowed)} — "
            f"fix the typo in reg_meta_build/{_FILE_NAME}.",
        )


# The three entry kinds this file carries. Each is loaded by its own call, and
# names the other two as legal siblings — derived here so a fourth kind cannot
# be added to one list and forgotten in another, which would report a legitimate
# table as an unknown top-level key.
_KINDS: dict[str, str] = {
    "version": "register / variant / name / evidence / noted",
    "delivered": "register / variant / column / versions / evidence / noted",
    "column": "register / variant / column / name / definition / "
    "versions or all_versions / source / evidence / noted",
}


def _entries(path: Path | None, kind: str) -> list[dict]:
    """One entry kind's raw tables, under this file's shared error vocabulary."""
    return load_curation_entries(
        path,
        entry_key=kind,
        label="SCB-errata",
        prefix="scb_errata",
        code_base="scb_errata",
        file_name=_FILE_NAME,
        entry_fields=_KINDS[kind],
        sibling_keys=frozenset(_KINDS) - {kind},
    )


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
    entry: dict,
    table: str,
    registers: dict[str, int],
    variants: dict[str, int],
) -> tuple[int, int, str]:
    """`(register_id, register_variant_id, "<register_fqid>/<variant>")` for an
    entry's `register` / `variant` pair."""
    register_fqid = _require_str(entry, "register", f"[[{table}]]")
    parts = register_fqid.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            _CODE,
            f"scb_errata register {register_fqid!r} must be a 2-segment "
            "`provider/register` FQID.",
            'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
        )
    provider, register = parts
    variant = _require_str(entry, "variant", f"[[{table}]]")
    context = f"{register_fqid}/{variant}"
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
            f"Use a `[register]` / `[register_variant]` slug curated in "
            f"fqid_slugs/{_PROVIDER}{PROVIDER_FILE_SUFFIX}.",
        )
    return register_id, variant_id, context


def load_scb_errata(
    path: Path | None,
    slug_dir: Path | None,
    *,
    classification_seed_path: Path | None = None,
) -> ScbErrata:
    """Parse the errata TOML, resolving each entry's `register`/`variant` slugs
    against the curated `scb.toml` in `slug_dir`. Empty when no file (synthetic
    builds, wheel installs).

    `classification_seed_path` is the build's own `curation/classifications.toml` (the one
    `populate_classifications` seeds from), consulted only when some `[[column]]`
    names a `classification` — an undeclared short_name is a typo that would
    otherwise be dropped silently by the candidate feed.

    Strict load, all EXIT_CONFIG with a remediation: only `[[version]]` /
    `[[delivered]]` / `[[column]]` top-level; no unknown key inside an entry;
    `register` a 2-segment SCB FQID and `register`/`variant` curated; `evidence`
    and `noted` (canonical `YYYY-MM-DD`) present; a `[[version]]` name carrying
    a parseable claimed year; `versions` a non-empty list of non-empty strings
    naming each version at most once; and no duplicate
    `(variant, name)` / `(variant, column)` entry — two entries for one column
    must be ONE entry listing both versions, or the log stops being readable as
    the record of what SCB missed. `[[delivered]]` and `[[column]]` share that
    column key: a column is one kind of omission or the other, never both.
    """
    version_entries = _entries(path, "version")
    delivered_entries = _entries(path, "delivered")
    column_entries = _entries(path, "column")
    if not version_entries and not delivered_entries and not column_entries:
        # Before touching the slug dir: resolving FQIDs parses the whole
        # curated scb.toml (~20k entries), and the common case — no file, or a
        # build whose provider set never reaches it — has nothing to resolve.
        return ScbErrata()
    registers, variants = _scb_slug_ids(slug_dir)

    versions: list[ErrataVersion] = []
    seen_versions: set[tuple[int, str]] = set()
    for entry in version_entries:
        _unknown_keys(entry, _VERSION_FIELDS, "version")
        _, variant_id, context = _resolve_variant(entry, "version", registers, variants)
        name = _require_str(entry, "name", f"[[version]] {context}")
        _require_evidence(entry, f"[[version]] {context}/{name}")
        if not _edition_years(name):
            raise curation_error(
                "scb_errata_version_year_unknown",
                f"scb_errata [[version]] {context}/{name} has no parseable "
                "claimed year.",
                "Use SCB's exact version name containing a four-digit year so "
                "the coalescer can place the edition chronologically.",
            )
        if (variant_id, name) in seen_versions:
            raise curation_error(
                _CODE,
                f"scb_errata duplicate [[version]] {context}/{name}.",
                "Each (register, variant, name) may appear once.",
            )
        seen_versions.add((variant_id, name))
        versions.append(ErrataVersion(variant_id, name))

    delivered: list[ErrataDelivered] = []
    seen_columns: set[tuple[int, str]] = set()
    for entry in delivered_entries:
        _unknown_keys(entry, _DELIVERED_FIELDS, "delivered")
        register_id, variant_id, context = _resolve_variant(
            entry, "delivered", registers, variants
        )
        column = _require_str(entry, "column", f"[[delivered]] {context}")
        ctx = f"[[delivered]] {context}/{column}"
        evidence = _require_evidence(entry, ctx)
        named = _named_versions(entry, ctx)
        upstream = (
            _require_str(entry, "upstream", ctx)
            if "upstream" in entry
            else _DEFAULT_DELIVERED_CLASS
        )
        key = (variant_id, fold_column(column))
        if key in seen_columns:
            raise curation_error(
                _CODE,
                f"scb_errata duplicate [[delivered]] {context}/{column}.",
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
            )
        )

    columns: list[ErrataColumn] = []
    # The variable each `[[column]]` key mints, so a column declared on two
    # variants of one register is checked to describe the SAME variable — it
    # will BE one (`ErrataColumn.key`), and a disagreement would silently ship
    # whichever entry the materializer wrote first.
    identities: dict[tuple[int, str], tuple] = {}
    for entry in column_entries:
        _unknown_keys(entry, _COLUMN_FIELDS, "column")
        register_id, variant_id, context = _resolve_variant(
            entry, "column", registers, variants
        )
        column = _require_str(entry, "column", f"[[column]] {context}")
        ctx = f"[[column]] {context}/{column}"
        source = _column_source(entry, ctx)
        evidence = _require_evidence(entry, ctx)
        loaded = ErrataColumn(
            register_id=register_id,
            register_variant_id=variant_id,
            column=column,
            name=_require_str(entry, "name", ctx),
            definition=_require_str(entry, "definition", ctx),
            data_type=_column_data_type(entry, ctx),
            classification=_column_classification(entry, ctx),
            is_identifier=_require_bool(entry, "is_identifier", ctx),
            is_sensitive=_require_bool(entry, "is_sensitive", ctx),
            versions=_column_versions(entry, ctx),
            source=source,
            provenance=_state_provenance(source, evidence),
        )
        key = (variant_id, fold_column(column))
        if key in seen_columns:
            raise curation_error(
                _CODE,
                f"scb_errata duplicate entry for {context}/{column}.",
                "Each (register, variant, column) may appear once, in ONE of "
                "[[delivered]] (SCB documents the column elsewhere on the "
                "variant) or [[column]] (it documents it nowhere).",
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

    _check_declared_classifications(columns, classification_seed_path)

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


def _column_versions(entry: dict, ctx: str) -> tuple[str, ...] | None:
    """A `[[column]]`'s placement: the named versions, or None for
    `all_versions = true`.

    Exactly one of the two. `all_versions` is what a steward holdings list
    actually says — the column is in the delivery, undated — and naming every
    edition instead would make the entry rot the next time SCB ships one."""
    all_versions = _require_bool(entry, "all_versions", ctx)
    if all_versions == ("versions" in entry):
        raise curation_error(
            _CODE,
            f"scb_errata {ctx} needs EITHER `versions` or `all_versions = true`, "
            f"not {'both' if all_versions else 'neither'}.",
            'Name the editions the column was delivered in (`versions = ["2010"]`) '
            "or declare it delivered in every edition of the variant "
            "(`all_versions = true`).",
        )
    return None if all_versions else _named_versions(entry, ctx)


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


def _check_declared_classifications(
    columns: list[ErrataColumn], seed_path: Path | None
) -> None:
    """Every `[[column]]` `classification` must name a DECLARED classification
    short_name. The candidate feed drops an unknown one with no row and no error,
    so a typo would silently ship an untagged state."""
    named = {c.classification for c in columns if c.classification is not None}
    if not named:
        return
    declared = declared_short_names(seed_path)
    unknown = sorted(named - declared)
    if unknown:
        raise curation_error(
            _CODE,
            f"scb_errata [[column]] names undeclared classification(s) {unknown}.",
            "Use an existing classification short_name (e.g. 'SSYK96') or declare "
            "it in reg_meta_build/curation/classifications.toml.",
        )


# Columns cloned from a real source row onto the synthetic one. `cvid` /
# `regver_id` are minted; `classification_id` and `variable_id` stay NULL —
# they are stamped by later passes that see the synthetic row like any other.


def _edition_years(name: str) -> tuple[int, ...]:
    """Read the declared edition's years to validate its source coordinate."""
    return tuple(year for year, _lo, _hi in edition_claims(name))
