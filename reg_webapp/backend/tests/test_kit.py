"""`POST /api/kit` — generation-kit build (A5.2c, §8).

See DESIGN.md → Kit-build surface. Covers: a valid project → 200
``application/zip`` containing exactly the three kit files; the dereferenced
``project_data.codes.json`` (classifications + per-source ad-hoc keyspaces); the
materialized ``display_name`` in the kit's ``project_data.json`` + namespaced-block
preservation; determinism (same spec → byte-identical archive); the kit-only
``panel_inheritance_unresolvable`` gate (and its three inheritance sources); and
the validation gate (structural / semantic errors → 422).

Runs against the slugged ``catalog_db`` fixture: ``scb/lisa/kon`` (categorical,
value set ``{1:Man, 2:Kvinna}``, delivery column ``Kon``, variant
``individer-15plus`` with NO panel_template), ``scb/rams/syss`` (variant
``standard`` carrying a composite panel_entity_key + ``period`` panel_time_key),
and ``class/sun2020`` (one canonical code ``1``/``Man``).
"""

from __future__ import annotations

import io
import json
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.kit import KIT_FILES


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def unthrottled_client(catalog_db):
    """Rate-limit raised so the concurrency smoke test isn't 429'd."""
    with TestClient(create_app(rate_limit_per_minute=100_000)) as c:
        yield c


def _spec(*, bindings: list[dict] | None = None, **extra) -> dict:
    """A valid single-source project on ``scb/lisa/individer-15plus`` at 2018.
    Defaults to the ad-hoc-coded ``kon`` binding (categorical, no value_set)."""
    spec = {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "kit-test",
        "sources": [
            {
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": bindings
                or [{"variable": "scb/lisa/kon", "type": "categorical"}],
            }
        ],
    }
    spec.update(extra)
    return spec


def _kit(content: bytes) -> dict[str, str]:
    """Unpack the kit ZIP → {filename: text}."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        return {name: archive.read(name).decode("utf-8") for name in archive.namelist()}


# ── Archive shape ────────────────────────────────────────────────────────────


def test_valid_project_returns_zip(client):
    resp = client.post("/api/kit", json=_spec())
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/zip")
    assert "attachment" in resp.headers["content-disposition"]
    assert "kit.zip" in resp.headers["content-disposition"]
    assert zipfile.is_zipfile(io.BytesIO(resp.content))


def test_zip_contains_exactly_the_kit_files(client):
    files = _kit(client.post("/api/kit", json=_spec()).content)
    assert set(files) == set(KIT_FILES)


def test_kit_is_deterministic(client):
    """Same validated spec → byte-identical archive (fixed ZIP timestamps + sorted
    JSON), mirroring the /bundle determinism property."""
    a = client.post("/api/kit", json=_spec()).content
    b = client.post("/api/kit", json=_spec()).content
    assert a == b


# ── project_data.json ────────────────────────────────────────────────────────


def test_display_name_is_materialized(client):
    """A binding with no explicit display_name gets the reg_meta default
    (delivery_column_name 'Kon') written into the kit's project_data.json — the
    reg_meta-free consumer can't resolve one itself."""
    files = _kit(client.post("/api/kit", json=_spec()).content)
    project = json.loads(files["project_data.json"])
    binding = project["sources"][0]["bindings"][0]
    assert binding["display_name"] == "Kon"


def test_explicit_display_name_is_preserved(client):
    files = _kit(
        client.post(
            "/api/kit",
            json=_spec(
                bindings=[
                    {
                        "variable": "scb/lisa/kon",
                        "type": "categorical",
                        "display_name": "Sex",
                    }
                ]
            ),
        ).content
    )
    project = json.loads(files["project_data.json"])
    assert project["sources"][0]["bindings"][0]["display_name"] == "Sex"


def test_namespaced_block_survives(client):
    """The kit's project_data.json is built from the RAW dict, so a steward block
    (``swecov``) survives — a typed ``extra='ignore'`` body would drop it."""
    spec = _spec()
    spec["swecov"] = {"sentinel": "SWECOV_XYZ"}
    files = _kit(client.post("/api/kit", json=spec).content)
    project = json.loads(files["project_data.json"])
    assert project["swecov"] == {"sentinel": "SWECOV_XYZ"}


# ── project_data.codes.json ──────────────────────────────────────────────────


def test_sources_codes_dereferenced_for_adhoc_binding(client):
    """A categorical binding with no value_set → its resolved state's value-set
    codes under ``sources[source.name][binding FQID]``."""
    files = _kit(client.post("/api/kit", json=_spec()).content)
    codes = json.loads(files["project_data.codes.json"])
    assert codes["sources"]["lisa-2018"]["scb/lisa/kon"] == [
        {"code": "1", "label": "Man"},
        {"code": "2", "label": "Kvinna"},
    ]


def test_classification_codes_dereferenced_for_value_set(client):
    """A categorical binding with ``value_set: class/sun2020`` → the
    classification's **canonical** code list under ``classifications[value_set]``.
    The fixture links code '1'/'Man' to sun2020 as canonical (`is_valid=1`) and
    code '2'/'Kvinna' as an observed-but-non-canonical (`is_valid=0`) noise row —
    so this also pins that the noise row is FILTERED out of the kit's canonical
    list (else mock generation would sample a code the steward excluded)."""
    files = _kit(
        client.post(
            "/api/kit",
            json=_spec(
                bindings=[
                    {
                        "variable": "scb/lisa/kon",
                        "type": "categorical",
                        "value_set": "class/sun2020",
                    }
                ]
            ),
        ).content
    )
    codes = json.loads(files["project_data.codes.json"])
    assert codes["classifications"]["class/sun2020"] == [{"code": "1", "label": "Man"}]
    # A value_set'd binding contributes to classifications, NOT sources.
    assert "lisa-2018" not in codes["sources"]


def test_datetime_binding_is_422(client):
    """A `datetime` binding is valid in reg_schema but the local generator's
    COLUMN_TYPES excludes it, so a kit built with one would fail at
    `reg-mockdata generate`. Kit-build gates it up front with an actionable 422."""
    resp = client.post(
        "/api/kit",
        json=_spec(
            bindings=[
                {
                    "variable": "scb/lisa/kon",
                    "type": "datetime",
                    "datetime_format": "%Y-%m-%d %H:%M:%S",
                }
            ]
        ),
    )
    assert resp.status_code == 422
    assert "datetime" in resp.json()["detail"]


def test_materialized_display_name_collision_is_422(client):
    """An explicit `display_name` that collides with another binding's RESOLVED
    default (both → "Kon") is invisible to the structural layer (it checks only
    explicit names) but is caught when kit-build re-runs structural on the
    materialized spec — the reg_meta-free consumer can't disambiguate two columns
    named "Kon"."""
    resp = client.post(
        "/api/kit",
        json=_spec(
            bindings=[
                # Resolves its default display_name to the delivery column "Kon".
                {"variable": "scb/lisa/kon", "type": "categorical"},
                # Same source/register, explicitly named "Kon" → collides with the
                # first binding's materialized default. The collision is caught at
                # re-validation, BEFORE the single-delivery-column gate, so the
                # multi-column `lonfink` is fine to use here.
                {
                    "variable": "scb/lisa/lonfink",
                    "type": "numeric",
                    "numeric_subtype": "double",
                    "display_name": "Kon",
                },
            ]
        ),
    )
    assert resp.status_code == 422
    assert "display_name_collision" in resp.json()["detail"]


def test_blank_display_name_is_materialized(client):
    """An explicit blank `display_name` (`""`) is an unusable column header the
    generator rejects; kit-build treats it as unset and fills the reg_meta default
    ("Kon") rather than shipping a kit with an empty header."""
    files = _kit(
        client.post(
            "/api/kit",
            json=_spec(
                bindings=[
                    {
                        "variable": "scb/lisa/kon",
                        "type": "categorical",
                        "display_name": "   ",
                    }
                ]
            ),
        ).content
    )
    project = json.loads(files["project_data.json"])
    assert project["sources"][0]["bindings"][0]["display_name"] == "Kon"


def test_blank_display_name_normalized_before_structural_gate(client):
    """A blank `display_name` is stripped BEFORE the first structural pass, so a
    panel column-ref to the value that WOULD be materialized resolves. Pre-fix, the
    first `validate_structural(raw)` saw the blank as an explicit "" → kept
    `all_have_display` True → flagged the panel ref as `entity_key_unknown_column`
    before normalization ran."""
    resp = client.post(
        "/api/kit",
        json=_panel_spec(
            source={
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                # Blank name → materializes to the delivery column "Kon".
                "bindings": [
                    {
                        "variable": "scb/lisa/kon",
                        "type": "categorical",
                        "display_name": "",
                    }
                ],
            },
            panel={
                "panel_id": "p1",
                "entity_key": "Kon",
                "time_key": 2018,
                "members": [{"source": "lisa-2018"}],
            },
        ),
    )
    assert resp.status_code == 200


def test_duplicate_binding_fqid_is_422_any_type(client):
    """Two bindings sharing a `variable` FQID in one source are unrepresentable in
    the FQID-keyed kit/stats contract for EVERY type (not just ad-hoc categorical) —
    gated before branching on type/value_set."""
    resp = client.post(
        "/api/kit",
        json=_spec(
            bindings=[
                {
                    "variable": "scb/lisa/kon",
                    "type": "categorical",
                    "value_set": "class/sun2020",
                    "display_name": "A",
                },
                {
                    "variable": "scb/lisa/kon",
                    "type": "categorical",
                    "value_set": "class/sun2020",
                    "display_name": "B",
                },
            ]
        ),
    )
    assert resp.status_code == 422
    assert "more than once" in resp.json()["detail"]


def test_multi_column_binding_is_422(client):
    """A binding that resolves to several delivery columns over its period (here the
    merged monthly family `lonfink` at annual grain → many month columns) with no
    `representation` to disambiguate can't map to one output column → 422."""
    resp = client.post(
        "/api/kit",
        json=_spec(
            bindings=[
                {
                    "variable": "scb/lisa/lonfink",
                    "type": "numeric",
                    "numeric_subtype": "double",
                }
            ]
        ),
    )
    assert resp.status_code == 422
    assert "multiple delivery columns" in resp.json()["detail"]


def test_codes_keyspace_is_total_even_when_empty(client):
    """Every categorical no-value_set binding gets a key even with no resolved
    codes (``scb/rams/syss`` has no value set) — so the consumer's lookup never
    KeyErrors."""
    files = _kit(
        client.post(
            "/api/kit",
            json={
                "schema_version": "2.0.0",
                "steward": "ifau",
                "reg_meta_version": "5.1.0",
                "name": "kit-test",
                "sources": [
                    {
                        "name": "rams-2018",
                        "register_variant": "scb/rams/standard",
                        "period": 2018,
                        "bindings": [
                            {"variable": "scb/rams/syss", "type": "categorical"}
                        ],
                    }
                ],
            },
        ).content
    )
    codes = json.loads(files["project_data.codes.json"])
    assert codes["sources"]["rams-2018"]["scb/rams/syss"] == []


def test_same_fqid_collision_in_one_source_is_422(client):
    """Two ad-hoc categorical bindings sharing the same `variable` FQID within one
    source collide in the binding-FQID-keyed `sources` keyspace (structurally legal —
    `display_name_collision` only catches EXPLICIT same names). Rather than silently
    drop one binding's codes, kit-build fails loudly with a 422."""
    resp = client.post(
        "/api/kit",
        json=_spec(
            bindings=[
                {
                    "variable": "scb/lisa/kon",
                    "type": "categorical",
                    "display_name": "Sex A",
                },
                {
                    "variable": "scb/lisa/kon",
                    "type": "categorical",
                    "display_name": "Sex B",
                },
            ]
        ),
    )
    assert resp.status_code == 422
    assert "more than once" in resp.json()["detail"]


def test_non_categorical_binding_has_no_codes(client):
    """An id/numeric/etc. binding carries no codes at all."""
    files = _kit(
        client.post(
            "/api/kit",
            json=_spec(
                bindings=[
                    {
                        "variable": "scb/lisa/kon",
                        "type": "id",
                        "id_subtype": "integer",
                    }
                ]
            ),
        ).content
    )
    codes = json.loads(files["project_data.codes.json"])
    assert codes["sources"] == {}
    assert codes["classifications"] == {}


# ── README ───────────────────────────────────────────────────────────────────


def test_readme_names_project_and_run_command(client):
    files = _kit(client.post("/api/kit", json=_spec(name="my-project")).content)
    readme = files["README.md"]
    assert "my-project" in readme
    assert "reg-mockdata generate" in readme


# ── Panel inheritance (the kit-only panel_inheritance_unresolvable check) ─────


def _panel_spec(*, source: dict, panel: dict) -> dict:
    return {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "panel-test",
        "sources": [source],
        "panels": [panel],
    }


def test_panel_inheritance_unresolvable_is_422(client):
    """A panel member on the lisa variant (NO panel_template), with no override
    and no panel default, can't resolve an effective entity/time key → 422."""
    resp = client.post(
        "/api/kit",
        json=_panel_spec(
            source={
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            },
            panel={"panel_id": "p1", "members": [{"source": "lisa-2018"}]},
        ),
    )
    assert resp.status_code == 422
    assert "panel_inheritance_unresolvable" in resp.json()["detail"]


def test_panel_inherits_from_variant_template(client):
    """The rams ``standard`` variant carries a panel_template (composite
    entity_key + ``period`` time_key), so a bare member inherits both → 200."""
    resp = client.post(
        "/api/kit",
        json=_panel_spec(
            source={
                "name": "rams-2018",
                "register_variant": "scb/rams/standard",
                "period": 2018,
                "bindings": [{"variable": "scb/rams/syss", "type": "categorical"}],
            },
            panel={"panel_id": "p1", "members": [{"source": "rams-2018"}]},
        ),
    )
    assert resp.status_code == 200


def test_panel_inherits_from_panel_level_default(client):
    """Panel-level entity_key + time_key satisfy inheritance even on the
    template-less lisa variant → 200. The entity_key column ref (`Kon`) must match
    a materialized display_name on the source (kit-build re-validates panel refs
    after defaulting); `time_key` is an int literal period, not a column ref."""
    resp = client.post(
        "/api/kit",
        json=_panel_spec(
            source={
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            },
            panel={
                "panel_id": "p1",
                "entity_key": "Kon",
                "time_key": 2018,
                "members": [{"source": "lisa-2018"}],
            },
        ),
    )
    assert resp.status_code == 200


def test_panel_ref_unresolved_after_defaulting_is_422(client):
    """A panel column ref that matches no materialized display_name (a typo) is
    caught when kit-build re-runs structural on the defaulted spec — structural
    skips the ref check pre-materialization because display_name is unset."""
    resp = client.post(
        "/api/kit",
        json=_panel_spec(
            source={
                "name": "lisa-2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            },
            panel={
                "panel_id": "p1",
                "entity_key": "Lopnrr",
                "time_key": 2018,
                "members": [{"source": "lisa-2018"}],
            },
        ),
    )
    assert resp.status_code == 422
    assert "entity_key_unknown_column" in resp.json()["detail"]


# ── Validation gate ──────────────────────────────────────────────────────────


def test_structurally_invalid_spec_is_422(client):
    spec = _spec()
    spec["sources"][0]["period"] = "notaperiod"
    resp = client.post("/api/kit", json=spec)
    assert resp.status_code == 422


def test_unresolvable_binding_is_422(client):
    """A binding FQID that doesn't resolve is a blocking semantic error — no kit
    is built (unlike /order, which best-efforts a manifest row)."""
    spec = _spec(bindings=[{"variable": "scb/lisa/nosuchvar", "type": "categorical"}])
    resp = client.post("/api/kit", json=spec)
    assert resp.status_code == 422
    assert "fqid_unresolved" in resp.json()["detail"]


def test_malformed_request_body_is_4xx(client):
    """A non-object JSON body is a malformed REQUEST (read_raw_json_object), not a
    validation failure."""
    resp = client.post(
        "/api/kit", content="[1, 2, 3]", headers={"content-type": "application/json"}
    )
    assert resp.status_code == 400


# ── Cross-thread concurrency (the locked per-request-open guard) ─────────────


def test_concurrent_kit_builds_no_cross_thread_error(unthrottled_client):
    """The kit opens a per-request reg_meta conn in the handler body — the same
    DB-backed write path the cross-thread P1 lived on. Drive it concurrently;
    every build must 200 and (pure function of input) return identical bytes."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda _: unthrottled_client.post("/api/kit", json=_spec()),
                range(40),
            )
        )
    codes = [r.status_code for r in results]
    assert all(c == 200 for c in codes), f"cross-thread failures: {codes}"
    assert len({r.content for r in results}) == 1, "non-deterministic kit bytes"
