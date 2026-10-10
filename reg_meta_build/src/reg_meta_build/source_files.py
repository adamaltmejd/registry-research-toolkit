"""SCB source-file IO, file hashing, path overlap and build progress/timing."""

from __future__ import annotations

import hashlib
import os
import sys
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

from .errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

    from .input_snapshot import ScbSnapshotReader

# Exact SCB type tokens shared by source cleaning and the standalone source audit.
# They are not enumerated codes when code == version == level. Source cleaning
# preserves their rows as non-membership evidence (sources/scb_values.py).
_VARDEMANGDER_SENTINELS = frozenset({"Tal", "Beskrivande text"})

# Bytes undefined in cp1252 but present in SCB data as DOS cp850 remnants.
# Map to their cp850 equivalents rather than rejecting.
_CP850_FIXUP = {0x8F: "Å", 0x90: "É", 0x9D: "Ø", 0x81: "ü", 0x8D: "ì"}


EXPECTED_HEADERS: dict[str, list[str]] = {
    "Registerinformation.csv": [
        "Registernamn",
        "Registerrubrik",
        "Registersyfte",
        "Registervariantrubrik",
        "Registervariantnamn",
        "Registervariantbeskrivning",
        "RegistervariantSekretess",
        "Registerversionnamn",
        "Registerversionbeskrivning",
        "Registerversionmätinformation",
        "Registerversion_DocStaus",
        "Registerversion_ForstaGodkannandeDatum",
        "Registerversion_SenastGodkandDatum",
        "Populationnamn",
        "Populationdefinition",
        "Populationkommentar",
        "Populationdatum",
        "Objekttypnamn",
        "Objekttypdefinition",
        "Variabelnamn",
        "Variabeldefinition",
        "Variabelbeskrivning",
        "VariabelOperationell_definition",
        "VariabelReferenstid",
        "VariabelHämtadFrån",
        "VariabelRegister_Källa",
        "VariabelExtern_kommentar",
        "Mattenhet",
        "Kolumnnamn",
        "Datatyp",
        "Datalängd",
        "CVID",
        "RegisterId",
        "RegVarID",
        "RegVerID",
        "VarId",
    ],
    "UnikaRegisterOchVariabler.csv": [
        "Registernamn",
        "Registerrubrik",
        "Registervariantnamn",
        "Registervariantrubrik",
        "Variabelnamn",
        "Kolumnnamn",
        "VersionForsta",
        "VersionSista",
        "KansligVariabel",
        "KansligVariabelIbland",
        "Identitetsvariabel",
    ],
    "Identifierare.csv": ["VarID", "Variabelnamn", "Variabeldefinition"],
    "Timeseries.csv": [
        "Namn",
        "Handelse",
        "Beskrivning",
        "Entitet",
        "ID1",
        "ID2",
        "FilID",
    ],
    "Vardemangder.csv": [
        "Värdemängdsversion",
        "Värdemängdsnivå",
        "Värdekod",
        "Värdebenämning",
        "CVID",
        "ItemId",
    ],
    "VardemangderValidDates.csv": ["ItemID", "ValidFrom", "ValidTo"],
}


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------


def _scb_snapshot_error(exc: Exception) -> RegMetaError:
    from .input_snapshot import SnapshotMaterializationError

    if isinstance(exc, SnapshotMaterializationError):
        return RegMetaError(
            exit_code=EXIT_CONFIG,
            code="scb_snapshot_materialization_required",
            error_class="configuration",
            message=str(exc),
            remediation=exc.hydration_action,
        )
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code="scb_snapshot_invalid",
        error_class="configuration",
        message=f"Selected SCB input snapshot is invalid: {exc}",
        remediation=(
            "Run the explicit snapshot verifier. If the selected identity changed "
            "or is unsupported, prepare, verify, and accept a new snapshot, then "
            "pass its exact Git commit and manifest SHA-256."
        ),
    )


def _paths_overlap(destinations: set[Path], inputs: set[Path]) -> bool:
    # The report temporary file is opened before replacement; a symlink or hard
    # link there must not turn a distinct-looking report into an input overwrite.
    return any(
        destination == input_path
        or (input_path.is_dir() and destination.is_relative_to(input_path))
        or (
            destination.exists()
            and input_path.exists()
            and destination.samefile(input_path)
        )
        for destination in destinations
        for input_path in inputs
    )


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


@contextmanager
def _open_scb_source_raw(
    path: Path, snapshot: ScbSnapshotReader
) -> Iterator[tuple[list[str], Iterator[list[str | None]]]]:
    from .input_snapshot import SnapshotError

    try:
        with snapshot.open_csv(path.name) as (raw_header, raw_rows):
            header = ["" if value is None else value for value in raw_header]

            yield header, raw_rows
    except SnapshotError as exc:
        raise _scb_snapshot_error(exc) from exc


@contextmanager
def _open_scb_csv_rows(
    path: Path,
    snapshot: ScbSnapshotReader,
) -> Iterator[tuple[list[str], Iterator[tuple[int, list[str | None]]]]]:
    """Share SCB header and row-width validation before cell interpretation."""
    with _open_scb_source_raw(path, snapshot) as (raw_header, reader):
        header = _validated_scb_header(path.name, raw_header)
        ncols = len(header)

        def rows() -> Iterator[tuple[int, list[str | None]]]:
            for row_number, fields in enumerate(reader, start=2):
                if len(fields) != ncols:
                    raise RegMetaError(
                        exit_code=EXIT_CONFIG,
                        code="csv_bad_row",
                        error_class="configuration",
                        message=f"Row {row_number} in {path.name} has {len(fields)} fields, expected {ncols}.",
                        remediation="Re-export the file from mikrometadata.scb.se.",
                    )
                yield row_number, fields

        yield header, rows()


def _validated_scb_header(filename: str, raw_header: Sequence[str]) -> list[str]:
    """Decode and validate one SCB CSV header at the interpretation boundary."""
    header = [_decode_cp1252(value) for value in raw_header]
    expected = EXPECTED_HEADERS.get(filename)
    if expected and header != expected:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="csv_bad_header",
            error_class="configuration",
            message=f"Unexpected header in {filename}.",
            remediation="Ensure the file is an unmodified SCB metadata export.",
        )
    return header


@contextmanager
def _open_scb_csv_prepared(
    path: Path,
    snapshot: ScbSnapshotReader,
) -> Iterator[
    tuple[list[str], Iterator[tuple[int, dict[str, tuple[bool, str | None, str]]]]]
]:
    """Yield lossless prepared cells through the normal SCB validation traversal.

    Each cell is ``(present, raw, interpreted)``.  ``present`` distinguishes a
    prepared NULL from a supplied empty scalar; interpreted values use the same
    cp1252 repair as :func:`_decode_cp1252`.
    """
    with _open_scb_csv_rows(path, snapshot) as (header, raw_rows):

        def rows() -> Iterator[tuple[int, dict[str, tuple[bool, str | None, str]]]]:
            for row_number, fields in raw_rows:
                yield (
                    row_number,
                    {
                        name: (
                            value is not None,
                            value,
                            _decode_cp1252(value or ""),
                        )
                        for name, value in zip(header, fields, strict=True)
                    },
                )

        yield header, rows()


def _decode_cp1252(raw: str) -> str:
    """Decode a latin-1-read string to proper cp1252.

    Bytes undefined in cp1252 but present as DOS cp850 remnants are mapped
    to their cp850 equivalents instead of rejecting the whole import.

    ASCII fast path: for a pure-ASCII string (the overwhelmingly common case
    across every SCB CSV), latin-1, cp1252, and the read string all agree on
    0x00–0x7F and none of the DOS-remnant fixup bytes (all >= 0x81) can occur —
    so the input is already correct and the encode + per-byte scan are skipped.
    """
    if raw.isascii():
        return raw
    raw_bytes = raw.encode("latin-1")
    if not any(b in _CP850_FIXUP for b in raw_bytes):
        return raw_bytes.decode("cp1252")
    return "".join(
        _CP850_FIXUP[b] if b in _CP850_FIXUP else bytes([b]).decode("cp1252")
        for b in raw_bytes
    )


def _progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _timing_enabled() -> bool:
    """True when per-stage build timing should be emitted.

    Opt-in via ``--timing`` (build-db) / ``REG_META_BUILD_TIMING=1`` — off by
    default so normal builds stay quiet. Checked at call time, not import, so the
    CLI flag (which sets the env var) takes effect.
    """
    return os.environ.get("REG_META_BUILD_TIMING") == "1"


def _emit_timing(label: str, t0: float) -> None:
    """Emit a greppable ``[timing] <label>: <s>`` stderr line if timing is on."""
    if _timing_enabled():
        _progress(f"[timing] {label}: {time.perf_counter() - t0:.1f}s")
