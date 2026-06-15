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

**Determinism.** Same validated spec → byte-identical kit (mirrors the
``/api/bundle`` determinism property): the ZIP entries are written in a fixed
order with a fixed timestamp, the JSON is ``sort_keys``-dumped, and the code lists
are deterministically ordered. A kit committed to git re-zips identically.
"""

from __future__ import annotations

import io
import json
import zipfile
from typing import TYPE_CHECKING, Any

from reg_meta.fqid import parse
from reg_meta.queries import get_classification_codes

from reg_webapp.order_export import resolve_display_name
from reg_webapp.semantic import period_for_resolve, period_segments

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
                sources.setdefault(source.name, {})[binding.variable] = _binding_codes(
                    binding, source, catalog
                )
    return {"classifications": classifications, "sources": sources}


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
    its full canonical code list. The semantic validator already proved the FQID
    resolves before kit-build gates, so ``resolve`` does not raise here."""
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
    return _code_pairs([(c["code"], c["label"]) for c in meta["codes"]])


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
            if new_binding.get("display_name") is None:
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


def build_kit_archive(
    raw: dict[str, Any],
    project: ProjectData,
    catalog: Catalog,
    conn: sqlite3.Connection,
) -> bytes:
    """The full kit-build: materialize display names, dereference codes, render
    the README, and pack the deterministic ZIP. Pure given a validated project +
    a live ``Catalog`` — the caller owns validation + the connection lifetime."""
    materialized = materialize_display_names(raw, project, catalog)
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
