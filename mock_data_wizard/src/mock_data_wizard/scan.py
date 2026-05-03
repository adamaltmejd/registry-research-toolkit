"""Pre-export PII scanner.

Defense-in-depth: even though the per-type branches in ``summarize.py``
only emit aggregates, a misclassified column (e.g. ``FelPersonNr``
routed through the categorical bucket because the heuristic flickered)
could leak row-level personal data into the frequency table. This
scanner runs as the *final* step before any file leaves the bundle's
``output_dir`` -- if it matches anything PII-shaped, the target file
is never written.

The flow is in-memory scan + temp-file + atomic rename:

1. Caller hands us ``(path, payload)``.
2. We stamp an in-band ``pii_scan`` attestation into ``payload``.
3. Scan the in-memory ``payload``. Match -> raise; no file is written.
4. Serialise to ``<path>.tmp``.
5. ``os.replace(tmp, path)`` (atomic on Posix and Windows).

The PII payload never touches disk on a dirty scan, and a partially
written file can never become the canonical export.

Conservatively scoped: strings only by default. Numeric scalars (n_rows,
counts, etc.) are not scanned because plain large integers like
``19501231`` (a row count that happens to look like a date) would
false-positive. If we ever need to scan numbers, that becomes an
opt-in flag, never the default.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger("mdw.scan")

SCANNER_VERSION = "1"


# -- Regexes -------------------------------------------------------------
# Anchor patterns with \b so contiguous digit runs don't mass-match
# inside larger numbers.

_PNR_12 = re.compile(r"\b(\d{12})\b")
_PNR_10 = re.compile(r"\b(\d{6})[-+](\d{4})\b")
# Bare 10-digit personnummer (YYMMDDXXXX, no separator). Anchored to the
# whole string -- a 10-digit run inside narrative text is too FP-prone
# (random 10-digit strings pass the date+Luhn gate at ~0.4%, vs. ~0.09%
# for 12-digit). The leak vector we care about is a misclassified column
# emitting bare PNRs as frequency-table keys, and those keys are atomic
# strings, so the whole-string anchor catches the leak without expanding
# FP surface to UNC paths, log lines, row counts, etc.
_PNR_10_BARE = re.compile(r"^(\d{10})$")
# Email: kept tight to reduce false positives from arbitrary
# `something@thing` strings inside JSON paths.
_EMAIL = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\.[A-Za-z]{2,}\b"
)
# Swedish mobile: 07[02369] + 7-8 digits, optionally with +46 prefix and
# spaces between groups. Conservative so 4-digit kommun codes don't hit.
_MOBILE = re.compile(
    r"(?:(?<=^)|(?<=[^\d+]))"
    r"(?:\+46\s?|0)7[02369]\s?\d{2}\s?\d{2}\s?\d{2}\s?\d{0,2}"
    r"(?=$|[^\d])"
)


# -- Helpers -------------------------------------------------------------


def _luhn_valid(digits: str) -> bool:
    """Modulus-10 (Luhn) check on a digit string. Used to drop most
    arbitrary 10/12-digit runs that happen to match the personnummer
    shape but aren't real identifiers."""
    if not digits.isdigit() or len(digits) < 10:
        return False
    # Personnummer Luhn applies to the last 10 digits (so the leading
    # YYYY of a 12-digit number is excluded from the checksum).
    last10 = digits[-10:]
    s = 0
    for i, ch in enumerate(last10):
        d = int(ch)
        if i % 2 == 0:  # double every other from the left
            d *= 2
            if d > 9:
                d -= 9
        s += d
    return s % 10 == 0


def _is_calendar_date(yyyy: int, mm: int, dd: int) -> bool:
    try:
        date(yyyy, mm, dd)
    except ValueError:
        return False
    return True


def _is_plausible_yymmdd(yy: int, mm: int, dd: int) -> bool:
    # Century is ambiguous on the 10-digit form, so check (mm, dd) against
    # a leap year: accepts Feb 29 (could be 1996/2000/...) but still
    # rejects Feb 30 / Nov 31 / etc.
    return _is_calendar_date(2000, mm, dd)


def _is_plausible_yyyymmdd(yyyy: int, mm: int, dd: int) -> bool:
    # Birthdates in active personnummer space; exclude 4-digit runs that
    # are dates/years but unlikely to be a personnummer prefix.
    if not (1850 <= yyyy <= 2099):
        return False
    return _is_calendar_date(yyyy, mm, dd)


def _redact(s: str) -> str:
    """Show the first three characters; mask the rest. Used in error
    messages so the operator gets enough context to find the offending
    column without us echoing the raw PII back."""
    if len(s) <= 3:
        return "***"
    return s[:3] + "***"


@dataclass(frozen=True)
class ScanMatch:
    pattern: str
    json_path: str
    redacted: str

    def __str__(self) -> str:
        return f"{self.pattern} at {self.json_path} -> {self.redacted}"


class PIIScannerError(Exception):
    """Raised when a pre-export scan finds suspected PII. The matched
    file has been deleted; the canonical path was never created."""

    def __init__(self, matches: list[ScanMatch]):
        self.matches = matches
        super().__init__(
            "PII scanner blocked export: "
            + "; ".join(str(m) for m in matches[:10])
            + (f"; ...and {len(matches) - 10} more" if len(matches) > 10 else "")
        )


# -- Walkers -------------------------------------------------------------


def _walk_strings(node: Any, path: str = "$") -> Iterator[tuple[str, str]]:
    """Yield ``(json_path, value)`` for every string in a JSON tree.

    Includes string-typed dict keys: a misclassified personnummer
    column ends up with PNRs as frequency-table keys, not values.
    """
    if isinstance(node, str):
        yield path, node
        return
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(k, str):
                yield f"{path}.{k}", k
            yield from _walk_strings(v, f"{path}.{k}")
        return
    if isinstance(node, list):
        for i, v in enumerate(node):
            yield from _walk_strings(v, f"{path}[{i}]")


def _has_personnummer(s: str) -> bool:
    """True if ``s`` contains anything that passes a personnummer-shape
    check (date validity + Luhn). Three forms:

    - 12-digit ``YYYYMMDDXXXX`` anywhere in ``s``
    - 10-digit ``YYMMDD[-+]XXXX`` anywhere in ``s``
    - bare 10-digit ``YYMMDDXXXX`` only when it IS the entire stripped
      string (see ``_PNR_10_BARE`` for the rationale)
    """
    for m in _PNR_12.finditer(s):
        digits = m.group(1)
        yyyy = int(digits[:4])
        mm = int(digits[4:6])
        dd = int(digits[6:8])
        if _is_plausible_yyyymmdd(yyyy, mm, dd) and _luhn_valid(digits):
            return True
    for m in _PNR_10.finditer(s):
        ymd = m.group(1)
        last4 = m.group(2)
        yy = int(ymd[:2])
        mm = int(ymd[2:4])
        dd = int(ymd[4:6])
        if _is_plausible_yymmdd(yy, mm, dd) and _luhn_valid(ymd + last4):
            return True
    bare = _PNR_10_BARE.match(s.strip())
    if bare is not None:
        digits = bare.group(1)
        yy = int(digits[:2])
        mm = int(digits[2:4])
        dd = int(digits[4:6])
        if _is_plausible_yymmdd(yy, mm, dd) and _luhn_valid(digits):
            return True
    return False


def scan_string(s: str) -> list[str]:
    """Return the list of pattern names matched in ``s`` (each at most
    once per call)."""
    hits: list[str] = []
    if _has_personnummer(s):
        hits.append("personnummer")
    if _EMAIL.search(s):
        hits.append("email")
    if _MOBILE.search(s):
        hits.append("mobile")
    return hits


def scan_payload(payload: Any) -> list[ScanMatch]:
    """Walk a JSON payload, collect every PII-pattern match."""
    matches: list[ScanMatch] = []
    for json_path, value in _walk_strings(payload):
        for pattern in scan_string(value):
            matches.append(
                ScanMatch(pattern=pattern, json_path=json_path, redacted=_redact(value))
            )
    return matches


# -- Public API ----------------------------------------------------------

PATTERNS_APPLIED = ("personnummer", "email", "mobile")


def write_export(path: Path, payload: dict) -> None:
    """Stamp + scan + atomically rename. ``path`` is only created on a
    clean scan."""
    path = Path(path)
    payload["pii_scan"] = {
        "scanner_version": SCANNER_VERSION,
        "patterns_applied": list(PATTERNS_APPLIED),
        "matches_found": 0,
    }
    matches = scan_payload(payload)
    if matches:
        raise PIIScannerError(matches)
    path.parent.mkdir(parents=True, exist_ok=True)
    # ``with_name`` instead of ``with_suffix(path.suffix + ".tmp")`` -- the
    # latter relies on Python accepting multi-dot suffixes ("..json.tmp")
    # which historically raised ValueError; with_name always works.
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    os.replace(tmp, path)


def scan_file(path: Path) -> list[ScanMatch]:
    """Scan an existing JSON file in place. Used by the standalone CLI
    subcommand to re-check a file someone has on disk."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return scan_payload(payload)
