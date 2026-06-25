"""Doc-coverage diagnostic: doc-documented columns missing from the catalog (#400).

A read-only per-register diff of the ingested SCB doc library
(``reg_meta_docs.db`` → the ``doc`` table) against the built catalog's
``variable_alias.delivery_column_name`` set. It surfaces, per register, the
columns that are DOCUMENTED in the doc library but ABSENT from the catalog's
delivery columns — a maintainer-review candidate set quantifying the
metadata-coverage gap.

It is the exact analog of the #416/#513 ``classification-residue`` diagnostic:
it PRODUCTIZES the gap into a review worklist; it does NOT auto-merge or mint
anything. Strictly read-only — it opens the built catalog DB and the colocated
doc DB, reads, and materializes nothing. Minting the missing columns (via
``scb_canonical/``) is a separate, deferred curation step (#400 PR2), driven by
hand from this worklist.

The register join is the one genuine ambiguity: the doc library keys each doc on
its subdirectory name (``doc.register``, e.g. ``"lisa"``), while the catalog
keys registers on the SCB literal ``register.name`` (e.g. ``"LISA"``). They are
matched deterministically on ``lower(register.name) == doc.register`` — the SCB
literal name is the source-of-truth register identity the doc subdir was derived
from, and (unlike ``register.slug``) it does not churn. A doc register that maps
to NO catalog register is REPORTED (never silently dropped) so the gap stays
fail-loud.

Columns are matched CASE-INSENSITIVELY: SCB delivery columns are case-variant
(``Ssyk4_J16`` in docs vs ``Ssyk4_2012_J16`` in delivery, ``Kon`` vs ``KON``),
so a case-sensitive diff would manufacture phantom-missing columns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .fqid_slugs import _toml_comment, _toml_str

if TYPE_CHECKING:
    import sqlite3


@dataclass(frozen=True)
class MissingColumn:
    """One doc-documented column with no catalog ``variable_alias`` match in its
    register: the catalog ``register`` (the doc subdir name), the documented
    ``column`` (raw ``doc.variable``), and the doc-side evidence a maintainer
    reviews — ``display_name``, ``filename``, ``source``."""

    register: str
    column: str
    display_name: str | None
    filename: str
    source: str | None


@dataclass(frozen=True)
class DocCoverageResult:
    """The diagnostic's output: the per-register missing columns plus the headline
    counts. Read-only — nothing is materialized.

    ``missing`` is sorted by (register, column) for deterministic output.
    ``unmapped_doc_registers`` holds the doc-library register subdir names that
    map to NO catalog register (``lower(register.name)`` matched none) — reported,
    never silently dropped, since each is itself a coverage gap. ``total`` is the
    number of missing columns; ``per_register_counts`` is ``{register: count}``
    over the mapped registers (a doc register with no missing columns is absent)."""

    missing: tuple[MissingColumn, ...]
    unmapped_doc_registers: tuple[str, ...]
    total: int
    per_register_counts: dict[str, int]


def compute_doc_coverage(
    catalog_conn: sqlite3.Connection,
    doc_conn: sqlite3.Connection,
) -> DocCoverageResult:
    """Diff doc-documented columns against the catalog's delivery columns (#400).

    For each doc register (``doc.register``), compute the documented columns
    (``doc.variable``) that are NOT present, case-insensitively, in that
    register's ``variable_alias.delivery_column_name`` set. The register join is
    ``lower(register.name) == doc.register`` (see module docstring). Read-only:
    only SELECTs against both connections; nothing is written or materialized.

    Rows in ``doc`` with a NULL ``variable`` are skipped (an appendix/topic doc
    documents no single column). A doc register mapping to no catalog register is
    reported in ``unmapped_doc_registers`` (every documented column under it is a
    gap, but with no register to diff against they are surfaced as the whole
    register being uncovered, not enumerated as phantom-missing columns)."""
    # Catalog delivery columns per register, keyed by lower(register.name). One
    # set of lowercased column names per register name — the case-insensitive
    # membership test the diff needs. The doc DB is small (one register today);
    # this whole-catalog projection is a maintainer diagnostic, not a hot path.
    catalog_cols_by_register: dict[str, set[str]] = {}
    for name, column in catalog_conn.execute(
        """
        SELECT r.name, va.delivery_column_name
        FROM variable_alias va
        JOIN variable v ON v.variable_id = va.variable_id
        JOIN register r ON r.register_id = v.register_id
        """
    ).fetchall():
        catalog_cols_by_register.setdefault(name.lower(), set()).add(column.lower())

    # Documented columns per doc register, with the evidence fields. DISTINCT on
    # the projection: a column documented in two files surfaces once per filename
    # (filename is part of the evidence), but a duplicate identical row is folded.
    missing: list[MissingColumn] = []
    seen_doc_registers: set[str] = set()
    for register, variable, display_name, filename, source in doc_conn.execute(
        """
        SELECT DISTINCT register, variable, display_name, filename, source
        FROM doc
        WHERE variable IS NOT NULL
        ORDER BY register, variable, filename
        """
    ).fetchall():
        seen_doc_registers.add(register)
        catalog_cols = catalog_cols_by_register.get(register.lower())
        if catalog_cols is None:
            # Doc register maps to no catalog register — reported below as an
            # unmapped register, not as per-column rows (no register to diff).
            continue
        if variable.lower() not in catalog_cols:
            missing.append(
                MissingColumn(
                    register=register,
                    column=variable,
                    display_name=display_name,
                    filename=filename,
                    source=source,
                )
            )

    unmapped = tuple(
        sorted(
            reg
            for reg in seen_doc_registers
            if reg.lower() not in catalog_cols_by_register
        )
    )

    missing.sort(key=lambda m: (m.register, m.column, m.filename))
    per_register_counts: dict[str, int] = {}
    for m in missing:
        per_register_counts[m.register] = per_register_counts.get(m.register, 0) + 1

    return DocCoverageResult(
        missing=tuple(missing),
        unmapped_doc_registers=unmapped,
        total=len(missing),
        per_register_counts=per_register_counts,
    )


def render_doc_coverage_toml(result: DocCoverageResult) -> str:
    """Render the doc-coverage gap as a comment-rich maintainer-review worklist.

    This is a REVIEW artifact for the human curation step, NOT a
    directly-machine-consumable format: minting the missing columns happens
    separately via ``scb_canonical/`` (#400 PR2). Each entry is emitted as a
    commented ``# [[column]]`` skeleton carrying the register, the documented
    column, and the doc-side evidence (display_name, source, filename) — nothing
    is uncommented to copy verbatim, mirroring how ``render_residue_toml`` marks
    its ambiguous evidence: a maintainer reads the gap and decides what to mint.

    Built by hand (not ``tomli_w``) so the per-column evidence ``#`` comments
    survive; every interpolated string goes through the shared ``_toml_str`` /
    ``_toml_comment`` leaves (``fqid_slugs.py``) for round-trip safety."""
    lines = [
        "# GENERATED doc-coverage gap worklist — reg-meta-build doc-coverage.",
        "#",
        "# Per-register diff of doc-documented columns (the ingested SCB doc",
        "# library) against the built catalog's variable_alias delivery columns.",
        "# Each entry is a column DOCUMENTED in the doc library but ABSENT from the",
        "# catalog (case-insensitive match) — a metadata-coverage gap.",
        "#",
        "# This is a REVIEW artifact, NOT a loadable file. Nothing here mints a",
        "# column: minting the confirmed gaps happens separately via scb_canonical/",
        "# (#400 PR2). Review each, then curate the addition by hand.",
        "#",
        f"# {result.total} doc-documented-but-missing column(s) across "
        f"{len(result.per_register_counts)} register(s).",
    ]

    if result.unmapped_doc_registers:
        lines.append("#")
        lines.append(
            "# Doc register(s) mapping to NO catalog register (reported, not diffed):"
        )
        for reg in result.unmapped_doc_registers:
            lines.append(f"#   - {_toml_comment(reg)}")

    if not result.missing:
        lines.append("")
        lines.append("# (no missing columns)")
        return "\n".join(lines) + "\n"

    current_register: str | None = None
    for m in result.missing:
        if m.register != current_register:
            current_register = m.register
            lines.append("")
            lines.append(
                f"# === register {_toml_comment(m.register)}: "
                f"{result.per_register_counts[m.register]} missing column(s) ==="
            )
        evidence = []
        if m.display_name:
            evidence.append(f"display_name={_toml_comment(m.display_name)}")
        if m.source:
            evidence.append(f"source={_toml_comment(m.source)}")
        evidence.append(f"filename={_toml_comment(m.filename)}")
        lines.append("")
        lines.append(f"# {', '.join(evidence)}")
        lines.append("# [[column]]")
        lines.append(f"# register = {_toml_str(m.register)}")
        lines.append(f"# column = {_toml_str(m.column)}")

    return "\n".join(lines) + "\n"
