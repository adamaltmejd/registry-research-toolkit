"""Deployment config contracts that are easy to drift in prose-only changes."""

from __future__ import annotations

import tomllib
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]


def _toml(path: str) -> dict:
    return tomllib.loads((_ROOT / path).read_text(encoding="utf-8"))


def _text(path: str) -> str:
    return (_ROOT / path).read_text(encoding="utf-8")


def test_swecov_steward_hostname_is_data_swecov() -> None:
    steward = _toml("reg_webapp/stewards/swecov/steward.toml")

    assert steward["hostname"] == "data.swecov.se"


def test_swecov_fly_app_selects_swecov_steward() -> None:
    fly = _toml("reg_webapp/fly.swecov.toml")

    assert fly["app"] == "reg-webapp-swecov"
    assert fly["build"]["dockerfile"] == "Dockerfile"
    assert fly["build"]["args"]["REG_META_FLAVOR"] == "swecov"
    assert fly["build"]["args"]["REG_WEBAPP_STEWARD"] == "swecov"
    assert fly["env"]["REG_WEBAPP_STEWARD"] == "swecov"
    assert fly["env"]["REG_WEBAPP_FAIL_ON_STEWARD_DRIFT"] == "1"


def test_swecov_deploy_allows_slow_catalog_index_boot() -> None:
    fly = _toml("reg_webapp/fly.swecov.toml")
    workflow = _text(".github/workflows/container-build.yml")
    entrypoint = _text("reg_webapp/docker-entrypoint.sh")
    swecov_deploy = workflow.split("\n  deploy-swecov:", 1)[1].split(
        "\n  edge-deploy:",
        1,
    )[0]

    assert fly["env"]["REG_WEBAPP_SMOKE_READY_DEADLINE"] == "420"
    assert fly["http_service"]["checks"][0]["grace_period"] == "420s"
    assert "--wait-timeout 10m0s" in swecov_deploy
    assert "REG_WEBAPP_SMOKE_READY_DEADLINE" in entrypoint
    assert '--ready-deadline "$SMOKE_READY_DEADLINE"' in entrypoint


def test_edge_workers_are_split_by_hostname() -> None:
    global_worker = _text("reg_webapp/edge/wrangler.jsonc")
    swecov_worker = _text("reg_webapp/edge/wrangler.swecov.jsonc")

    assert '"pattern": "catalog.swecov.se/*"' in global_worker
    assert '"pattern": "data.swecov.se/*"' not in global_worker
    assert '"pattern": "data.swecov.se/*"' in swecov_worker
    assert '"pattern": "catalog.swecov.se/*"' not in swecov_worker


def test_swecov_fly_jobs_use_swecov_scoped_token() -> None:
    workflow = _text(".github/workflows/container-build.yml")
    swecov_build = workflow.split("\n  build-swecov-image:", 1)[1].split(
        "\n  deploy:", 1
    )[0]
    swecov_deploy = workflow.split("\n  deploy-swecov:", 1)[1].split(
        "\n  edge-deploy-swecov:",
        1,
    )[0]

    assert "secrets.FLY_API_TOKEN_SWECOV" in swecov_build
    assert "secrets.FLY_API_TOKEN }}" not in swecov_build
    assert "secrets.FLY_API_TOKEN_SWECOV" in swecov_deploy
    assert "secrets.FLY_API_TOKEN }}" not in swecov_deploy


def test_swecov_image_job_resolves_release_artifact() -> None:
    workflow = _text(".github/workflows/container-build.yml")
    swecov_build = workflow.split("\n  build-swecov-image:", 1)[1].split(
        "\n  deploy:",
        1,
    )[0]

    assert 'gh release view "$REG_META_TAG" --json assets' in swecov_build
    assert 'asset.get("name") == "reg_meta_swecov.db.zst"' in swecov_build
    assert "secrets.SWECOV_REG_META_DB_ZST" not in swecov_build


def test_global_image_job_does_not_receive_swecov_manifest_secret() -> None:
    workflow = _text(".github/workflows/container-build.yml")
    global_build = workflow.split("\n  build-image:", 1)[1].split(
        "\n  build-swecov-image:",
        1,
    )[0]

    assert "SWECOV_REG_META_DB_ZST" not in global_build
