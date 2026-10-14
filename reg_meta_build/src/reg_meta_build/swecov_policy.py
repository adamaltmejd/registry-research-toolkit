"""Authored SWECOV source routing, read by holdings accounting.

The generator reads the steward spec (`steward_spec.py`) instead; this model
stays while holdings accounting still reads `source_policy.toml`.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .steward_spec import (  # noqa: TC001 (Pydantic resolves them at runtime)
    FlavorDefinition,
    ProviderSlug,
    RegisterCoordinate,
    VariantCoordinate,
)

if TYPE_CHECKING:
    from pathlib import Path


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class RouteSplit(_PolicyModel):
    selector: str
    target: VariantCoordinate | None = Field(default=None, min_length=1)
    note: str = Field(min_length=1)

    @field_validator("selector")
    @classmethod
    def _selector(cls, value: str) -> str:
        if value.startswith("stem:") and value[5:]:
            return value
        if (match := re.fullmatch(r"period:(\d{4})-(\d{4})?", value)) and (
            match[2] is None or int(match[1]) <= int(match[2])
        ):
            return value
        raise ValueError(f"unknown or invalid route selector {value!r}")


class SourceRoute(_PolicyModel):
    category: str = Field(min_length=1)
    detail: str
    status: Literal["mapped", "split", "flavor", "lookup", "unmapped"]
    target: VariantCoordinate | None = Field(default=None, min_length=1)
    note: str | None = None
    unmapped_reason: str | None = None
    graft: RegisterCoordinate | None = Field(default=None, min_length=1)
    split: list[RouteSplit] = Field(default_factory=list)

    @model_validator(mode="after")
    def _shape(self) -> SourceRoute:
        if (self.status == "mapped") != (self.target is not None):
            raise ValueError("only mapped routes must name a target")
        if (self.status == "split") != bool(self.split):
            raise ValueError("only split routes must name selectors")
        if self.status == "unmapped":
            if not self.unmapped_reason or not self.unmapped_reason.strip():
                raise ValueError("unmapped routes require a nonblank unmapped_reason")
            if self.graft is not None:
                raise ValueError("unmapped routes cannot name a graft")
        elif self.unmapped_reason is not None:
            raise ValueError("only unmapped routes may name an unmapped_reason")
        if len({entry.selector for entry in self.split}) != len(self.split):
            raise ValueError("duplicate route selector")
        return self


class ProviderScope(_PolicyModel):
    holding_prefix: str = Field(min_length=1)
    provider: ProviderSlug = Field(min_length=1)


class RegisterScope(_PolicyModel):
    holding: str = Field(min_length=1)
    register_key: RegisterCoordinate = Field(alias="register", min_length=1)


class SourcePolicy(_PolicyModel):
    non_catalog_categories: dict[str, str]
    flavor_registers: list[RegisterCoordinate]

    @field_validator("non_catalog_categories")
    @classmethod
    def _nonblank_categories(cls, value: dict[str, str]) -> dict[str, str]:
        if any(not key.strip() or not reason.strip() for key, reason in value.items()):
            raise ValueError("non-catalog categories and reasons must be nonblank")
        return value

    @field_validator("flavor_registers")
    @classmethod
    def _registers(cls, value: list[str]) -> list[str]:
        if len(set(value)) != len(value):
            raise ValueError("duplicate flavor register")
        return value

    route: list[SourceRoute]
    flavor: list[FlavorDefinition]
    provider_scope: list[ProviderScope]
    register_scope: list[RegisterScope]

    @model_validator(mode="after")
    def _unique(self) -> SourcePolicy:
        for label, keys in (
            ("route", [(entry.category, entry.detail) for entry in self.route]),
            (
                "flavor coordinate",
                [
                    (entry.provider, entry.register_key, entry.variant)
                    for entry in self.flavor
                ],
            ),
            (
                "flavor variant slug",
                [
                    (entry.provider, entry.register_key, entry.variant_slug)
                    for entry in self.flavor
                ],
            ),
            ("provider scope", [entry.holding_prefix for entry in self.provider_scope]),
            ("register scope", [entry.holding for entry in self.register_scope]),
        ):
            if len(set(keys)) != len(keys):
                raise ValueError(f"duplicate {label}")
        provider_names: dict[str, str] = {}
        register_names: dict[tuple[str, str], str] = {}
        for entry in self.flavor:
            if (
                provider_names.setdefault(entry.provider, entry.provider_name)
                != entry.provider_name
            ):
                raise ValueError(f"conflicting provider name {entry.provider!r}")
            key = entry.provider, entry.register_key
            if (
                register_names.setdefault(key, entry.register_name)
                != entry.register_name
            ):
                raise ValueError(f"conflicting register name {key!r}")
        return self


def load_source_policy(path: Path) -> SourcePolicy:
    import tomllib

    try:
        return SourcePolicy.model_validate(
            tomllib.loads(path.read_text(encoding="utf-8"))
        )
    except (OSError, ValueError) as exc:
        raise SystemExit(f"invalid SWECOV source policy {path}: {exc}") from exc
