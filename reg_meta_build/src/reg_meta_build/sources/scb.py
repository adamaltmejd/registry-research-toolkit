"""SCB edition policy for converting accepted alias windows.

Source cleaning lives in scb_records/scb_values/scb_reference_records. This module
also supplies two constants/helpers used by the historical coding audit script;
they are not used to resolve the current catalog.
"""

from __future__ import annotations

from reg_meta_build._curation import fold_column
from reg_meta_build.edition_bounds import edition_claims, vintage_claim

# Historical comparison threshold used only by scripts/measure_subannual_codings.py.
_COSMETIC_MAX_SYM = 2

# The accepted forecast-register convention reads an edition as its vintage.
# This is retained for the existing alias declaration converter.
_PROJECTION_REGISTERS: set[int] = {310}


def _ascii_fold_lower(s: str | None) -> str:
    return fold_column(s) if s else ""


def register_edition_claims(
    register_id: int, versionname: str | None
) -> tuple[tuple[int, str, str], ...]:
    """Read edition claims using the accepted forecast-register convention."""
    if register_id in _PROJECTION_REGISTERS:
        return vintage_claim(versionname)
    return edition_claims(versionname)
