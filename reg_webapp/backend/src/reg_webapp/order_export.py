"""Default v1 order-export CSV renderer (§9.5 `POST /api/project/order`).

The steward's *order export* is the human-readable manifest a researcher hands
to the data provider: one row per bound variable. v1 ships the DEFAULT template
only — a CSV with the §9.5 columns ``provider,register,variant,variable,period,
display_name`` — one row per ``sources[*].bindings[*]``. Stewards inherit this
default; per-steward jinja2 ``order_template`` overrides are DEFERRED (not v1).

``period`` follows the §9.5 wire serialization (matching the ``/api/catalog``
``?period=`` and the catch-all): an int year prints as-is; a ``PeriodRange``
prints ``"<from>..<to>"`` (literal ``..``); the ``"_default"`` snapshot sentinel
prints literally.

``display_name`` defaults from reg_meta when ``Binding.display_name`` is None:
the binding is resolved against the live ``Catalog`` at the source's
``(register_variant, period)`` and the matching state's
``delivery_column_name`` is used (the §6.3 default-resolution rule). A binding
that doesn't resolve, or has no covering state, falls back to its bare
``variable`` FQID leaf — the order is a manifest, so a best-effort label beats a
crash; the semantic validator is where unresolved bindings are surfaced as
errors, not here.

Output is DETERMINISTIC: rows follow ``sources`` then ``bindings`` declaration
order (no sort — the spec's own order is the contract), and the CSV is written
with ``\r\n`` line terminators via the stdlib ``csv`` writer for a stable,
round-trippable artifact.
"""

from __future__ import annotations

import csv
import io
from typing import TYPE_CHECKING

from reg_meta.errors import RegMetaError
from reg_meta.fqid import FqidError, parse

from .semantic import period_for_resolve

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog
    from reg_schema.project_data import Binding, PeriodRange, ProjectData, Source

# §9.5: the default order-export column header (fixed order is the contract).
ORDER_COLUMNS = (
    "provider",
    "register",
    "variant",
    "variable",
    "period",
    "display_name",
)


def render_order_csv(project: ProjectData, catalog: Catalog) -> str:
    """Render ``project`` to the default v1 order-export CSV (§9.5).

    One row per ``sources[*].bindings[*]`` in declaration order. ``catalog`` is
    consulted only to default a missing ``display_name`` from the binding's
    ``delivery_column_name``; it is never mutated and no connection is opened
    here (the caller owns the ``Catalog`` lifetime, mirroring ``validate_semantic``).
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(ORDER_COLUMNS)
    for source in project.sources:
        provider, register, variant = _coordinate_parts(source.register_variant)
        period_str = _period_str(source.period)
        for binding in source.bindings:
            writer.writerow(
                _csv_safe(cell)
                for cell in (
                    provider,
                    register,
                    variant,
                    binding.variable,
                    period_str,
                    _display_name(binding, source, catalog),
                )
            )
    return buffer.getvalue()


# Spreadsheet formula-injection triggers: a cell that begins with one of these is
# interpreted as a FORMULA by Excel / Sheets / LibreOffice when the file is opened.
_FORMULA_TRIGGERS = "=+-@\t\r"


def _csv_safe(value: str) -> str:
    """Neutralize spreadsheet formula injection (§16). The order CSV is the manifest
    a researcher hands to a DATA PROVIDER, who opens it in a spreadsheet — a
    researcher-controlled ``display_name`` like ``=HYPERLINK("http://evil","x")``
    would otherwise execute as a formula on the provider's machine. A leading-
    trigger cell is prefixed with a single quote so the spreadsheet treats it as
    text. The csv writer's own quoting (delimiters / quotes / CRLF) does NOT cover
    this — formula triggers aren't quote-triggering characters. Slug cells
    (provider/register/variant/variable) can't trigger, so this is a no-op for them;
    applied uniformly for defense in depth."""
    if value and value[0] in _FORMULA_TRIGGERS:
        return "'" + value
    return value


def _coordinate_parts(register_variant: str) -> tuple[str, str, str]:
    """Split a ``<provider>/<register>/<variant>`` coordinate. Structural
    validation guarantees the 3-part shape upstream (this renderer runs only on a
    validated ``ProjectData``); a defensive pad keeps a malformed coordinate from
    raising mid-render (it would already have failed validation)."""
    parts = register_variant.split("/")
    parts += [""] * (3 - len(parts))
    return parts[0], parts[1], parts[2]


def _period_str(period: int | str | PeriodRange) -> str:
    """Serialize ``Source.period`` to its §9.5 wire string. ``PeriodRange`` →
    ``"<from>..<to>"`` (literal ``..``, matching the ``?period=`` range form); int
    / str (incl. the ``"_default"`` sentinel) → ``str()``."""
    if isinstance(period, (int, str)):
        return str(period)
    # PeriodRange: `from_` is the Python-safe alias of the wire key `from`.
    return f"{period.from_}..{period.to}"


def _display_name(binding: Binding, source: Source, catalog: Catalog) -> str:
    """The binding's ``display_name``, defaulting from reg_meta when unset (§6.3).

    Explicit ``display_name`` wins. Otherwise resolve the binding at the source's
    ``(register_variant, period)`` and use the first covering state's
    ``delivery_column_name``. If the binding doesn't resolve, has no covering
    state, or that state has a NULL ``delivery_column_name``, fall back to the
    bare FQID leaf — a manifest needs *a* label, and unresolved bindings are the
    validator's job to flag (this renderer is best-effort)."""
    if binding.display_name is not None:
        return binding.display_name
    # A `representation` is the chosen delivery column — it IS the default label.
    if binding.representation is not None:
        return binding.representation

    try:
        parsed = parse(binding.variable)
    except FqidError:
        return _fqid_leaf(binding.variable)

    variant = (
        source.register_variant.split("/")[2]
        if "/" in source.register_variant
        else None
    )
    period = period_for_resolve(source.period)
    try:
        states = catalog.resolve_at(parsed, period, variant=variant)
    except RegMetaError:
        return _fqid_leaf(binding.variable)
    for state in states:
        if state.delivery_column_name:
            return state.delivery_column_name
    return _fqid_leaf(binding.variable)


def _fqid_leaf(fqid: str) -> str:
    """The bare leaf segment of an FQID — the fallback display label."""
    return fqid.rpartition("/")[2] or fqid
