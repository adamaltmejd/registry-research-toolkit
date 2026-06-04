"""In-memory steward catalog index (§9.1 / §9.2).

Built once at FastAPI startup from a steward's validated
``steward.project_data.json`` (§9.1) and held on ``app.state`` for the lifetime
of the process. It is the filter that scopes a steward deployment to a subset of
reg_meta's universe: the validate endpoint and the variable-list authoring
endpoints (A5.2b-ii) consult it, and ``fqid_outside_steward_catalog`` (§6.8.3)
fires when a researcher's project references an FQID not in it.

Two maps (REFACTOR_SPEC §9.1, derived directly from the steward project's
``sources[]``):

- ``bindings_by_variant`` — ``register_variant coordinate`` (the 3-part
  ``<provider>/<register>/<variant>`` string) → ``frozenset`` of the binding
  FQIDs (3-segment ``<provider>/<register>/<slug>``) admitted under it.
- ``period_range_by_register`` — ``register FQID`` (2-segment
  ``<provider>/<register>``) → the ``(lo, hi)`` period span the steward declares
  for that register, as the raw period tokens seen on its sources. Best-effort
  span for UI hinting, NOT a validity gate (the semantic validator's per-binding
  ``period_outside_state_validity`` is the gate).

This is an INTERNAL dataclass — never a response body — so it is a plain stdlib
frozen ``@dataclass``, not Pydantic (§9.6: only response models are Pydantic;
reg_webapp internals are dataclasses). The ``global`` deployment
(``has_catalog_filter=False``) has NO index (``None``); the catalog endpoints
pass through to reg_meta's full universe.

A binding that the semantic validator (steward-caller mode) could not resolve
(``fqid_unresolved`` / ``period_outside_state_validity`` / ``value_set_missing``,
all downgraded to ``warning`` at boot, §6.8.3) is DROPPED from the index — it
can't be authored against until the steward updates the catalog. ``drift_warnings``
carries those downgraded warnings so ``/api/context`` can surface a "catalog
drift" banner (§6.8.3, §9.1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from reg_schema.project_data import ProjectData
    from reg_schema.validation import ValidationIssue


@dataclass(frozen=True)
class DriftWarning:
    """A single boot-time catalog-drift warning (§6.8.3 steward downgrade).

    A thin, JSON-serializable projection of the steward-mode ``ValidationIssue``
    that caused a binding to drop from the index — surfaced on ``/api/context``
    so the SPA can show which FQIDs the steward's committed catalog references but
    reg_meta no longer admits. Mirrors ``ValidationIssue`` minus ``level`` (these
    are all warnings by construction)."""

    code: str
    path: str
    message: str


@dataclass(frozen=True)
class CatalogIndex:
    """The §9.1 in-memory steward catalog filter. Internal — never serialized."""

    bindings_by_variant: dict[str, frozenset[str]]
    period_range_by_register: dict[str, tuple[str, str]]
    drift_warnings: tuple[DriftWarning, ...]

    def admits(self, fqid: str) -> bool:
        """True iff ``fqid`` (a 3-segment binding FQID) is admitted by ANY variant
        in the index. The index keys on the binding FQID directly (no ``@version``
        pin to normalize away — that grammar is retired). Used by
        ``fqid_outside_steward_catalog`` (A5.2b-ii)."""
        return any(fqid in bindings for bindings in self.bindings_by_variant.values())


def _dropped_binding_paths(warnings: tuple[ValidationIssue, ...]) -> set[str]:
    """JSON-pointer prefixes of bindings the steward-mode validator could not
    resolve. A binding warning has path ``/sources/<i>/bindings/<j>/variable`` (or
    ``…/value_set``); we key on the ``…/bindings/<j>`` prefix so a binding is
    dropped whether its variable OR its value_set failed to resolve.

    An UNRESOLVED ``register_variant`` (path ``/sources/<i>/register_variant``)
    drops the whole source: every binding under it is unauthorable. We record the
    source prefix so all its bindings drop."""
    dropped: set[str] = set()
    for issue in warnings:
        path = issue.path
        marker = "/bindings/"
        if marker in path:
            # …/sources/<i>/bindings/<j>/<field> → …/sources/<i>/bindings/<j>
            head, _, tail = path.partition(marker)
            j = tail.split("/", 1)[0]
            dropped.add(f"{head}{marker}{j}")
        elif path.endswith("/register_variant"):
            # The whole source is unauthorable; record its prefix.
            dropped.add(path.rsplit("/register_variant", 1)[0])
    return dropped


def _is_dropped(source_base: str, binding_base: str, dropped: set[str]) -> bool:
    """A binding drops if its own ``/bindings/<j>`` prefix OR its parent source
    prefix is in ``dropped`` (an unresolved register_variant drops every binding
    under the source)."""
    return binding_base in dropped or source_base in dropped


def build_catalog_index(
    project: ProjectData,
    issues: tuple[ValidationIssue, ...],
) -> CatalogIndex:
    """Build the §9.1 index from a validated steward ``project`` plus the
    steward-mode semantic ``issues`` it produced.

    ``project`` has already passed ``validate_structural`` (well-formed FQIDs /
    period grammar) and ``validate_semantic`` in steward-caller mode. We walk its
    ``sources``, skipping any binding (or whole source) flagged as drift, and
    accumulate the two §9.1 maps.

    Only ``warning``-level issues mark a DROP (and populate ``drift_warnings``):
    those are exactly the §6.8.3 steward-downgraded resolution failures
    (``fqid_unresolved`` / ``value_set_missing`` / ``period_outside_state_validity``
    / ``binding_representation_unknown``) — the FQID / value-set / period / pinned
    representation this catalog references that reg_meta no longer admits. An
    ``info`` ``binding_state_drifts_within_period`` (the binding RESOLVED, it just
    spans a transition or its representation under-covers the range) and a
    non-downgraded ``error`` ``binding_value_set_version_ambiguous`` (kept an error
    per §6.8.3 — a researcher-author-time concern, not catalog drift) must NOT drop
    the binding; the steward filter keeps it and the researcher path enforces it."""
    warnings = tuple(i for i in issues if i.level == "warning")
    dropped = _dropped_binding_paths(warnings)
    bindings_by_variant: dict[str, set[str]] = {}
    periods_by_register: dict[str, list[str]] = {}

    for s_idx, source in enumerate(project.sources):
        source_base = f"/sources/{s_idx}"
        variant_coord = source.register_variant
        # Always declare the variant slot (a dropped-variant source maps to an
        # empty set — declared, but admits nothing).
        variant_bindings = bindings_by_variant.setdefault(variant_coord, set())
        if source_base in dropped:
            # Whole source dropped (unresolved register_variant): not authorable, so
            # it must NOT contribute to the register's period span either.
            continue
        register_fqid = "/".join(variant_coord.split("/")[:2])
        periods_by_register.setdefault(register_fqid, []).append(_period_token(source))
        for b_idx, binding in enumerate(source.bindings):
            binding_base = f"{source_base}/bindings/{b_idx}"
            if _is_dropped(source_base, binding_base, dropped):
                continue
            variant_bindings.add(binding.variable)

    return CatalogIndex(
        bindings_by_variant={k: frozenset(v) for k, v in bindings_by_variant.items()},
        period_range_by_register={
            reg: (min(tokens), max(tokens))
            for reg, tokens in periods_by_register.items()
        },
        drift_warnings=tuple(
            DriftWarning(code=w.code, path=w.path, message=w.message) for w in warnings
        ),
    )


def _period_token(source) -> str:  # noqa: ANN001 — reg_schema Source (TYPE_CHECKING-only import)
    """A comparable string token for a source's period, for the best-effort
    register span. A bare int/str period is its own token; a range collapses to
    its endpoints' span via ``from``. ISO/period tokens sort chronologically as
    strings only loosely (mixed grammars don't), so this is a UI hint, never a
    gate — see the module docstring."""
    period = source.period
    if isinstance(period, (int, str)):
        return str(period)
    # PeriodRange: use the `from_` endpoint as the lower-bound token.
    return str(period.from_)
