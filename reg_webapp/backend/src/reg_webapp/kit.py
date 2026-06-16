"""Kit-build domain logic (`POST /api/kit`) — REFACTOR_SPEC.md §8.

See DESIGN.md → Kit-build surface. Kit-build is just **file packaging**: it
turns a validated ``project_data.json`` into the downloadable **generation kit** a
researcher runs ``reg-mockdata generate`` against locally — reg_meta-free and
fully offline. The kit is freestanding from reg_meta: a project committed to git
regenerates the same mock data years later regardless of how reg_meta evolves.

This module is the pure DOMAIN half (no IO, no FastAPI): it takes a validated
``ProjectData`` + the raw dict + a ``Catalog`` (+ the underlying connection, only
for the classification-codes accessor reg_meta exposes as a query function, not a
``Catalog`` method) and returns plain Python / bytes. ``routes/kit.py`` owns the
HTTP + connection lifecycle and the validation GATE.

The kit is three files (the extract output ``project_data.stats.json`` is NOT
emitted here — it comes back from the researcher's MONA extract and is dropped in
beside the kit before ``generate``; the README says so):

- ``project_data.json`` — the spec, with every binding's ``display_name``
  **materialized** (resolved from reg_meta's ``delivery_column_name`` when the
  author left it unset) so the reg_meta-free consumer never has to resolve one.
  Steward-namespaced blocks (``reg_monabundle`` / ``swecov`` / ``reg_mockdata``)
  are preserved by materializing onto the RAW dict, not the ``extra="ignore"``
  model.
- ``project_data.codes.json`` — dereferenced code lists (see ``build_codes``).
- ``README.md`` — what the kit is + the ready-to-run command.

**Determinism.** Same validated spec → byte-identical kit *within a build
environment* (mirrors the ``/api/bundle`` determinism property — stable for
content-hash caching + the round-trip tests): the ZIP entries are written in a
fixed order with a fixed timestamp + normalized mode bits, the JSON is
``sort_keys``-dumped, and the code lists are deterministically ordered. (The
DEFLATE byte stream is only guaranteed reproducible for a given zlib build — the
kit's *value* is the extracted JSON files, not the archive bytes.)
"""

from __future__ import annotations

import io
import json
import zipfile
from typing import TYPE_CHECKING, Any

from reg_meta.fqid import parse
from reg_meta.queries import get_classification_codes

from reg_webapp.order_export import resolve_display_name
from reg_webapp.semantic import period_display, period_for_resolve, period_segments

if TYPE_CHECKING:
    import sqlite3

    from reg_meta.catalog import Catalog
    from reg_schema.project_data import Binding, ProjectData, Source

# Fixed ZIP entry timestamp (DOS epoch's first legal value — ZIP can't encode
# pre-1980) so a kit is byte-identical across builds. A real mtime would make the
# archive non-reproducible, breaking the "commit a kit to git, re-zip identically"
# property and the determinism test.
_ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)

KIT_FILES = ("project_data.json", "project_data.codes.json", "README.md")


class KitBuildError(ValueError):
    """A spec that VALIDATES but cannot be packaged into a coherent kit (e.g. two
    same-FQID bindings in one source that collide in the codes keyspace). Raised by
    the domain layer; the route maps it to a 422 (it is bad INPUT, not a 500)."""


def build_codes(
    project: ProjectData, catalog: Catalog, conn: sqlite3.Connection
) -> dict[str, Any]:
    """Dereference ``project_data.codes.json`` from a validated project.

    Codes live in this SIBLING file, never inline in ``project_data.json``. Two
    keyspaces, split by what determines the list (REFACTOR_SPEC.md §8):

    - ``classifications`` — keyed by the binding's ``value_set`` FQID
      (``class/<slug>``). The canonical, period-invariant code list, dereferenced
      from reg_meta and shared across every binding that references it.
    - ``sources`` — keyed by ``source.name`` then binding FQID. The codes for an
      ad-hoc-coded categorical binding (no ``value_set``) within one source,
      unioned across the states its ``(variant, period)`` resolves to. Nested by
      source because one binding projects different value sets across deliveries;
      a flat binding-FQID key would collide or force a lossy union.

    Only **categorical** bindings carry codes (id/numeric/date/datetime/opaque
    have none). A categorical binding's ``value_set`` selects the path: present →
    ``classifications[value_set]``; absent → ``sources[source.name][fqid]``. Every
    such binding contributes its key (possibly an empty list — the keyspace is
    TOTAL so the reg_mockdata consumer's lookup never KeyErrors).

    **Keying (decided #206/#217).** The kit-contract keyspace is column-based —
    keyed on ``(binding FQID, resolved delivery column)``. Post-validation a
    binding within a source resolves to exactly one delivery column (co-existing
    columns without a ``representation`` pin are a blocking
    ``binding_value_set_version_ambiguous`` error that can't reach kit-build), so
    the resolved column is implied by the binding FQID and the pair collapses to
    the binding-FQID key the §8 consumer contract reads.

    ``conn`` is the same connection ``catalog`` wraps; it is needed only because
    the classification canonical code list has no ``Catalog`` accessor — we go
    through ``reg_meta.queries.get_classification_codes`` (id-resolved, so it picks
    up the ``same_as`` redirect ``Catalog.resolve`` followed)."""
    classifications: dict[str, list[dict[str, str]]] = {}
    sources: dict[str, dict[str, list[dict[str, str]]]] = {}
    for source in project.sources:
        for binding in source.bindings:
            if binding.type != "categorical":
                continue
            if binding.value_set is not None:
                if binding.value_set not in classifications:
                    classifications[binding.value_set] = _classification_codes(
                        binding.value_set, catalog, conn
                    )
            else:
                # `check_unique_binding_fqids` already proved no source binds a
                # `variable` twice, so this key is collision-free.
                sources.setdefault(source.name, {})[binding.variable] = _binding_codes(
                    binding, source, catalog
                )
    return {"classifications": classifications, "sources": sources}


def check_unique_binding_fqids(project: ProjectData) -> None:
    """Raise ``KitBuildError`` if any source binds the same ``variable`` FQID more
    than once. The kit/stats contract keys BOTH ``project_data.codes.json``
    ``sources`` and ``project_data.stats.json`` ``bindings`` on ``source.name`` →
    binding FQID, so a source with two same-FQID bindings (distinct
    ``representation``s — structurally legal; ``display_name_collision`` catches
    only EXPLICIT same names) is unrepresentable for EVERY binding type, not just
    the ad-hoc-coded case. Fail loudly rather than silently drop a column (#450
    tracks lifting this with resolved-column keying)."""
    for source in project.sources:
        seen: set[str] = set()
        for binding in source.bindings:
            if binding.variable in seen:
                raise KitBuildError(
                    f"source {source.name!r} binds {binding.variable!r} more than "
                    "once (distinct representations of one variable); the kit/stats "
                    "contract keys by source name → binding FQID and cannot represent "
                    "two same-FQID bindings in one source — split them into separate "
                    "sources or pin distinct variables (see #450)"
                )
            seen.add(binding.variable)


def _resolved_columns(
    binding: Binding, source: Source, catalog: Catalog
) -> set[str | None]:
    """The distinct delivery columns the binding's ``(variant, period)`` resolves
    to (per #307 segment), narrowed to the pinned ``representation``. >1 means the
    binding maps to several real columns — a sequential delivery-column rename or a
    merged monthly family (#319)."""
    parsed = parse(binding.variable)
    variant = source.register_variant.split("/")[2]
    cols: set[str | None] = set()
    for segment in period_segments(source.period):
        for state in catalog.resolve_at(
            parsed, period_for_resolve(segment), variant=variant
        ):
            if (
                binding.representation is not None
                and state.delivery_column_name != binding.representation
            ):
                continue
            cols.add(state.delivery_column_name)
    return cols


def check_single_delivery_column(project: ProjectData, catalog: Catalog) -> None:
    """Raise ``KitBuildError`` if a binding (without a disambiguating
    ``representation``) resolves to MORE THAN ONE delivery column over its source
    period — a sequential rename (``KonOld`` → ``KonNew`` across a range) or a
    merged monthly family bound at annual grain (#319). The kit maps one binding to
    ONE materialized ``display_name`` (one output column), and the MONA runtime /
    generator index overrides by ``display_name`` — so the second real column would
    be lost. The semantic layer only flags this ``binding_state_drifts_within_period``
    (info), so kit-build escalates it: the author must pin a ``representation`` or
    narrow the source period. A pinned binding resolves to its one column (even if
    it under-covers — the author's explicit choice), so this never fires on it."""
    for source in project.sources:
        for binding in source.bindings:
            columns = _resolved_columns(binding, source, catalog)
            if len(columns) > 1:
                shown = ", ".join(sorted(c for c in columns if c is not None))
                raise KitBuildError(
                    f"binding {binding.variable!r} in source {source.name!r} "
                    f"resolves to multiple delivery columns ({shown}) over period "
                    f"{period_display(source.period)} — a column rename or a merged "
                    "monthly family. The kit maps one binding to one column; pin a "
                    "`representation` or narrow the source period (see #450)"
                )


def _code_pairs(pairs: list[tuple[str, str]]) -> list[dict[str, str]]:
    """Project (code, label) pairs to the JSON shape, deduped by ``code`` (first
    label wins) and ordered by ``code`` — deterministic regardless of input
    order."""
    by_code: dict[str, str] = {}
    for code, label in pairs:
        by_code.setdefault(code, label)
    return [{"code": c, "label": by_code[c]} for c in sorted(by_code)]


def _classification_codes(
    value_set: str, catalog: Catalog, conn: sqlite3.Connection
) -> list[dict[str, str]]:
    """The canonical code list for a ``class/<slug>`` value_set FQID.

    Resolve the FQID via ``Catalog.resolve`` (which follows the curated
    ``classification_same_as`` graph) to the live ``classification_id``, then read
    its canonical code list. The semantic validator already proved the FQID
    resolves before kit-build gates, so ``resolve`` does not raise here.

    **Canonical only.** ``get_classification_codes`` returns BOTH the canonical
    rows (``is_valid=1``) and observed-only noise rows (``is_valid=0``) for a
    classification that has a curated canonical CSV. The kit is documented as the
    canonical list, so the noise rows are dropped (``is_valid != 0``). We do NOT
    pass ``only_valid=True``: a classification WITHOUT a canonical CSV has
    ``is_valid`` NULL everywhere (stripped from the row), and ``only_valid`` would
    return ZERO codes for it — keeping the ``is_valid``-absent rows preserves its
    full code list."""
    from reg_meta.catalog import ResolvedClassification  # noqa: PLC0415 — lazy

    resolved = catalog.resolve(parse(value_set))
    # `value_set` is a structurally-validated 2-seg `class/<slug>` FQID, so
    # `resolve` dispatches to the classification resolver — narrow for the type
    # checker (the else is unreachable post-validation: the semantic gate already
    # proved this FQID resolves to a classification).
    if not isinstance(resolved, ResolvedClassification):
        return []
    # Pass the live id id-resolved (a str int → WHERE id = ?) so we read the SAME
    # classification `same_as` redirected to, not a fuzzy short_name match.
    meta = get_classification_codes(conn, str(resolved.classification_id))
    return _code_pairs(
        [(c["code"], c["label"]) for c in meta["codes"] if c.get("is_valid") != 0]
    )


def _binding_codes(
    binding: Binding, source: Source, catalog: Catalog
) -> list[dict[str, str]]:
    """The ad-hoc code list for a categorical binding with no ``value_set``: the
    UNION of the value-set codes on every state the binding's ``(variant, period)``
    resolves to, narrowed to the pinned ``representation`` column when set.

    Resolves PER SEGMENT (#307 list period) like the semantic validator. A state
    with no value set contributes nothing; a binding whose resolved states carry
    no codes yields an empty list (still a present key). Best-effort: an
    unresolvable binding can't reach kit-build (it gates on the validator's
    errors), so resolution does not raise here."""
    parsed = parse(binding.variable)
    variant = source.register_variant.split("/")[2]
    pairs: list[tuple[str, str]] = []
    for segment in period_segments(source.period):
        for state in catalog.resolve_at(
            parsed, period_for_resolve(segment), variant=variant
        ):
            if (
                binding.representation is not None
                and state.delivery_column_name != binding.representation
            ):
                continue
            if state.value_set:
                pairs.extend(state.value_set)
    return _code_pairs(pairs)


def strip_blank_display_names(raw: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of the raw spec with BLANK (``""`` / whitespace) binding
    ``display_name``s REMOVED (treated as unset), to run BEFORE the first
    ``validate_structural`` pass.

    A blank label is unusable, so the kit treats it as unset and materializes the
    reg_meta default. But ``validate_structural`` sees a blank as an explicit string:
    two blank names trip ``display_name_collision``, and a blank keeps
    ``all_have_display`` True so panel column-ref checks fire against the blank
    (``entity_key_unknown_column``) — both rejecting the spec BEFORE normalization.
    Stripping blanks up front makes blank-as-unset consistent across the whole
    pipeline (the structural gate, the model, materialization, and the re-validation).

    Runs on UNVALIDATED input, so it is defensive about shape — a malformed source /
    binding passes through untouched for ``validate_structural`` to flag."""
    sources = raw.get("sources")
    if not isinstance(sources, list):
        return raw
    out = dict(raw)
    new_sources: list[Any] = []
    for raw_source in sources:
        bindings = raw_source.get("bindings") if isinstance(raw_source, dict) else None
        if not isinstance(bindings, list):
            new_sources.append(raw_source)
            continue
        new_source = dict(raw_source)
        new_bindings: list[Any] = []
        for raw_binding in bindings:
            dn = (
                raw_binding.get("display_name")
                if isinstance(raw_binding, dict)
                else None
            )
            if isinstance(dn, str) and not dn.strip():
                stripped = dict(raw_binding)
                del stripped["display_name"]
                new_bindings.append(stripped)
            else:
                new_bindings.append(raw_binding)
        new_source["bindings"] = new_bindings
        new_sources.append(new_source)
    out["sources"] = new_sources
    return out


def materialize_display_names(
    raw: dict[str, Any], project: ProjectData, catalog: Catalog
) -> dict[str, Any]:
    """Return a copy of the raw spec with every binding's ``display_name`` filled.

    The kit is reg_meta-free, so its ``project_data.json`` must carry an explicit
    ``display_name`` on every binding (the resolved ``delivery_column_name`` when
    the author left it unset) — the consumer can't resolve one. Materializes onto
    the RAW dict (not the ``extra="ignore"`` model) so steward-namespaced blocks
    survive. ``raw`` and ``project`` are positionally parallel (the model was built
    from this raw dict after structural validation), so binding ``[i][j]`` lines
    up. Defensive copies keep the input untouched (the caller may still serialize
    the original elsewhere)."""
    out = dict(raw)
    out_sources = []
    for s_idx, raw_source in enumerate(raw.get("sources", [])):
        source = project.sources[s_idx]
        new_source = dict(raw_source)
        new_bindings = []
        for b_idx, raw_binding in enumerate(raw_source.get("bindings", [])):
            new_binding = dict(raw_binding)
            # Set display_name on EVERY binding: `resolve_display_name` returns a
            # non-blank explicit name unchanged, and resolves the reg_meta default for
            # an unset OR blank/whitespace one (a blank header is unusable and the
            # generator rejects falsey names). So this both fills defaults and
            # normalizes blanks, while preserving genuine names.
            new_binding["display_name"] = resolve_display_name(
                source.bindings[b_idx], source, catalog
            )
            new_bindings.append(new_binding)
        new_source["bindings"] = new_bindings
        out_sources.append(new_source)
    out["sources"] = out_sources
    return out


def render_readme(project: ProjectData) -> str:
    """The kit README: what it is + the ready-to-run command. No timestamp (kept
    out for the byte-identical determinism property)."""
    return (
        f"# Generation kit — {project.name}\n"
        "\n"
        "This kit regenerates mock data locally with `reg-mockdata`, with no "
        "reg_meta dependency and fully offline. Committed to git, it regenerates "
        "the same mock data regardless of how reg_meta evolves.\n"
        "\n"
        "## Contents\n"
        "\n"
        "- `project_data.json` — the project spec (FQID references; every "
        "binding's `display_name` is materialized).\n"
        "- `project_data.codes.json` — dereferenced code lists "
        "(classifications + per-source ad-hoc codes).\n"
        "- `project_data.stats.json` — the aggregate statistics from your MONA "
        "extract. **Add this file yourself** (download it from your MONA "
        "project) before running.\n"
        "\n"
        "## Run\n"
        "\n"
        "```sh\n"
        "reg-mockdata generate\n"
        "```\n"
    )


def assemble_kit(files: dict[str, bytes]) -> bytes:
    """Pack the kit files into a deterministic ZIP archive.

    Entries are written in the fixed ``KIT_FILES`` order with a fixed timestamp
    (``_ZIP_EPOCH``) and no compression metadata that varies run-to-run, so the
    same inputs yield byte-identical bytes. ``files`` maps each ``KIT_FILES`` name
    to its UTF-8 bytes."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in KIT_FILES:
            info = zipfile.ZipInfo(filename=name, date_time=_ZIP_EPOCH)
            # Normalize external attrs (0o644) so the archive doesn't carry the
            # process umask — another determinism-breaker.
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, files[name])
    return buffer.getvalue()


# reg_schema ColumnType values the local generator cannot consume. The generator's
# supported set is `reg_monabundle.runtime.classify.COLUMN_TYPES`
# (id/categorical/numeric/opaque/date); `datetime` is the one reg_schema type with no
# generator support (it lands with composite keys in step 10b). Held as a small constant
# rather than importing the runtime's COLUMN_TYPES — the webapp import graph deliberately
# excludes `reg_monabundle.runtime.*` (pinned by a test), and the kit takes NO
# reg_mockdata dependency (it is pure file packaging).
_GENERATOR_UNSUPPORTED_TYPES: frozenset[str] = frozenset({"datetime"})


def check_generatable(project: ProjectData) -> None:
    """Raise ``KitBuildError`` if a binding declares a column type the local
    generator cannot produce. The kit's whole point is a runnable ``reg-mockdata
    generate``, so a type the generator rejects (today only ``datetime``) is gated
    HERE with an actionable message rather than failing opaquely at generate time —
    the same class of capability gate ``/api/bundle`` runs for the MONA runtime."""
    for source in project.sources:
        for binding in source.bindings:
            if binding.type in _GENERATOR_UNSUPPORTED_TYPES:
                raise KitBuildError(
                    f"binding {binding.variable!r} in source {source.name!r} has "
                    f"type {binding.type!r}, which the generator cannot produce yet "
                    "(datetime support lands with composite keys in step 10b)"
                )


def build_kit_archive(
    materialized: dict[str, Any],
    project: ProjectData,
    catalog: Catalog,
    conn: sqlite3.Connection,
) -> bytes:
    """Dereference codes, render the README, and pack the deterministic ZIP from
    the already-materialized spec. ``materialized`` is the raw dict with every
    ``display_name`` filled (the caller owns materialization + re-validation, so the
    deferred default-dependent structural checks have already gated). Pure given a
    validated project + a live ``Catalog`` — the caller owns the connection lifetime.
    Raises ``KitBuildError`` on a spec that validates but can't be packaged: an
    unsupported generator type, a duplicate binding FQID in one source, or a binding
    that resolves to several delivery columns (rename / merged family)."""
    check_generatable(project)
    check_unique_binding_fqids(project)
    check_single_delivery_column(project, catalog)
    codes = build_codes(project, catalog, conn)
    files = {
        "project_data.json": _json_bytes(materialized),
        "project_data.codes.json": _json_bytes(codes),
        "README.md": render_readme(project).encode("utf-8"),
    }
    return assemble_kit(files)


def _json_bytes(payload: dict[str, Any]) -> bytes:
    """Deterministic UTF-8 JSON bytes: ``sort_keys`` + a trailing newline +
    ``ensure_ascii=False`` (Swedish labels stay readable, not ``\\uXXXX``)."""
    return (
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    ).encode("utf-8")
