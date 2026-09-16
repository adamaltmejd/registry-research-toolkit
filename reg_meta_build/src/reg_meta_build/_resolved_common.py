"""Small shared contracts and deterministic IDs for resolved catalog inputs."""

from datetime import date
from typing import TYPE_CHECKING, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
    model_validator,
)

from reg_meta_build.id import mint, mint_canonical_scb

if TYPE_CHECKING:
    from collections.abc import Iterable


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


def _storage_id(provider: str, kind: str, *coordinates: str) -> int:
    # The shipped schema validator reserves separate SCB/non-SCB integer bands.
    allocate = mint_canonical_scb if provider == "scb" else mint
    return allocate("resolved-catalog", kind, provider, *coordinates)


def _classification_id(slug: str) -> int:
    return mint("resolved-catalog", "classification", slug)


def covers_window(
    intervals: Iterable[tuple[str, str]], valid_from: str, valid_to: str
) -> bool:
    """Whether known inclusive ISO intervals cover a window without a gap."""
    next_day = date.fromisoformat(valid_from).toordinal()
    last_day = date.fromisoformat(valid_to).toordinal()
    if next_day > last_day:
        raise ValueError("coverage bounds are reversed")
    for start, end in sorted(intervals):
        lower, upper = (
            date.fromisoformat(start).toordinal(),
            date.fromisoformat(end).toordinal(),
        )
        if lower > next_day:
            break
        next_day = max(next_day, upper + 1)
        if next_day > last_day:
            return True
    return False
