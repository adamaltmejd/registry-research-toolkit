"""Small shared contracts and deterministic IDs for resolved catalog inputs."""

from datetime import date
from typing import TYPE_CHECKING, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
    model_validator,
)

from reg_meta_build.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.id import mint, mint_canonical_scb

if TYPE_CHECKING:
    from collections.abc import Iterable

CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY = "classification_succession_as_of_year"
# Release-time policy for future-dated classification succession. Bump deliberately
# when a new DB release should activate a future classification hand-off.
CLASSIFICATION_SUCCESSION_AS_OF_YEAR = 2026

# The open-ended `variable_state.valid_to` the DDL defaults to (db.py).
_VALID_TO_SENTINEL = "9999-12-31"


class _ResolvedModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        populate_by_name=True,
        serialize_by_alias=True,
    )


def _require_trimmed(value: str) -> str:
    if not value or value != value.strip():
        raise ValueError(
            "resolved names, keys, and columns must be nonempty and trimmed"
        )
    return value


class _ResolvedWindow(_ResolvedModel):
    """Resolved coverage; 9999-12-31 is the catalog's open-ended upper sentinel."""

    valid_from: str
    valid_to: str

    @field_validator("valid_from", "valid_to")
    @classmethod
    def _coverage_date(cls, value: str, info: ValidationInfo) -> str:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value or (
            parsed.year == 9999
            and (info.field_name != "valid_to" or value != "9999-12-31")
        ):
            raise ValueError(
                "coverage requires full ISO dates; only valid_to may use the open-ended sentinel"
            )
        return value

    @model_validator(mode="after")
    def _ordered_bounds(self) -> Self:
        if self.valid_from > self.valid_to:
            raise ValueError("state valid_from must not exceed valid_to")
        return self


class _ResolvedDeliveryScope(_ResolvedModel):
    """Explicit calendar coverage or a delivery with no calendar claim."""

    period_scope: Literal["intervals", "year_independent"] = "intervals"
    valid_from: str | None = None
    valid_to: str | None = None

    @model_validator(mode="after")
    def _delivery_scope(self) -> Self:
        if self.period_scope == "year_independent":
            if self.valid_from is not None or self.valid_to is not None:
                raise ValueError(
                    "year-independent delivery must have no calendar bounds"
                )
        else:
            if self.valid_from is None or self.valid_to is None:
                raise ValueError("dated delivery requires both ISO bounds")
            _ResolvedWindow(valid_from=self.valid_from, valid_to=self.valid_to)
        return self


def _storage_id(provider: str, kind: str, *coordinates: str) -> int:
    # The shipped schema validator reserves separate SCB/non-SCB integer bands.
    allocate = mint_canonical_scb if provider == "scb" else mint
    return allocate("resolved-catalog", kind, provider, *coordinates)


def _classification_id(slug: str) -> int:
    return mint("resolved-catalog", "classification", slug)


def remaining_windows(
    intervals: Iterable[tuple[str, str]], valid_from: str, valid_to: str
) -> tuple[tuple[str, str], ...]:
    """The parts of one inclusive ISO window that known intervals leave uncovered."""
    next_day = date.fromisoformat(valid_from).toordinal()
    last_day = date.fromisoformat(valid_to).toordinal()
    if next_day > last_day:
        raise ValueError("coverage bounds are reversed")
    gaps = []
    for start, end in sorted(intervals):
        lower, upper = (
            date.fromisoformat(start).toordinal(),
            date.fromisoformat(end).toordinal(),
        )
        if lower > last_day:
            break
        if lower > next_day:
            gaps.append(
                (
                    date.fromordinal(next_day).isoformat(),
                    date.fromordinal(lower - 1).isoformat(),
                )
            )
        next_day = max(next_day, upper + 1)
        if next_day > last_day:
            return tuple(gaps)
    gaps.append(
        (date.fromordinal(next_day).isoformat(), date.fromordinal(last_day).isoformat())
    )
    return tuple(gaps)


def covers_window(
    intervals: Iterable[tuple[str, str]], valid_from: str, valid_to: str
) -> bool:
    """Whether known inclusive ISO intervals cover a window without a gap."""
    return not remaining_windows(intervals, valid_from, valid_to)


# Built-in data providers. `provider_id` values are stable: rows reference them
# from `register.provider_id`. Add new providers by appending — never renumber.
PROVIDER_ID_SCB = 1
PROVIDER_ID_SOS = 2
PROVIDER_ID_FOHM = 3
PROVIDER_ID_FK = 4
PROVIDER_ID_LV = 5
PROVIDER_ID_PLIKT = 6
PROVIDER_ID_RA = 7
PROVIDER_ID_UMU = 8
_PROVIDER_SEED: tuple[tuple[int, str, str], ...] = (
    (PROVIDER_ID_SCB, "scb", "Statistiska Centralbyrån"),
    (PROVIDER_ID_SOS, "sos", "Socialstyrelsen"),
    (PROVIDER_ID_FOHM, "fohm", "Folkhälsomyndigheten"),
    (PROVIDER_ID_FK, "fk", "Försäkringskassan"),
    (PROVIDER_ID_LV, "lakemedelsverket", "Läkemedelsverket"),
    (PROVIDER_ID_PLIKT, "pliktverket", "Pliktverket"),
    (PROVIDER_ID_RA, "riksarkivet", "Riksarkivet"),
    (PROVIDER_ID_UMU, "umu", "Umeå universitet"),
)

# Thin CURATED global providers (#422): public agencies with no machine-readable
# native export — their catalog content is a maintainer-authored TOML read by the
# shared `CuratedAdapter` (sources/curated.py). Each entry is
# (provider_slug, input_data subdir holding `<provider_slug>.toml`). Unlike the
# untracked SCB/SOS seed, this TOML is committed, so the subdir always exists on
# any checkout — which requires a per-agency `.gitignore` un-ignore line (the
# `input_data/*` rule otherwise hides it). See DESIGN.md → Curated thin providers.
_CURATED_PROVIDERS: tuple[tuple[str, str], ...] = (
    ("fohm", "Folkhalsomyndigheten"),
    ("fk", "Forsakringskassan"),
    ("lakemedelsverket", "Lakemedelsverket"),
    ("pliktverket", "Pliktverket"),
    ("riksarkivet", "Riksarkivet"),
    ("umu", "UMU"),
)


def _provider_id_for(provider: str) -> int:
    """Map an IR provider slug to its stable `provider.provider_id` seed value."""
    for pid, slug, _name in _PROVIDER_SEED:
        if slug == provider:
            return pid
    raise RegMetaError(
        exit_code=EXIT_CONFIG,
        code="unknown_provider",
        error_class="configuration",
        message=f"No provider_id seed for provider {provider!r}.",
        remediation="Add the provider to _PROVIDER_SEED.",
    )
