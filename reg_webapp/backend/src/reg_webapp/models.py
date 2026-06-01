"""Webapp-local Pydantic response models.

These are reg_webapp's OWN response models — NOT reg_schema models and NOT
reg_meta dataclasses (§9.6). reg_meta's library surface is plain dataclasses;
the backend wraps its domain types in per-endpoint Pydantic models (the only
place a 1:1 wrapper remains). reg_schema models are used directly only for
project_data-shaped responses (A5.1b+).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class StewardInfo(BaseModel):
    """Deployment identity + branding, from ``steward.toml`` (§9.1)."""

    id: str
    name: str
    long_name: str


class RegMetaInfo(BaseModel):
    """reg_meta build provenance, read from the DB ``import_manifest``."""

    schema_version: str = Field(
        description="Schema version of the reg_meta DB build (e.g. '5.1.0')."
    )
    import_date: str = Field(
        description="UTC timestamp the reg_meta DB was built/imported."
    )


class WebappInfo(BaseModel):
    """Package versions for the deployed backend + its reg_meta dependency."""

    version: str = Field(description="Installed reg_webapp package version.")
    reg_meta_version: str = Field(
        description=(
            "Installed reg_meta package version — distinct from "
            "reg_meta.schema_version, which is the DB build."
        )
    )


class ContextResponse(BaseModel):
    """``GET /api/context`` — deployment identity, branding, build info (§9.5).

    No git sha (decision: no new provenance dep). The reg_meta block reflects
    the DB the backend booted against; the webapp block reflects the installed
    packages.
    """

    steward: StewardInfo
    reg_meta: RegMetaInfo
    webapp: WebappInfo
