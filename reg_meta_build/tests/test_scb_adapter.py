"""Validation of accepted CIS matrix declarations."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.cis2016_matrix import load_cis2014_matrix, load_cis2016_matrix

if TYPE_CHECKING:
    from pathlib import Path


# The pre-flip `_reparent_variable_alias` projection (the function A4.3a
# deleted) — used to prove the IR-carried IRVariableAlias rows are row-identical.
_OLD_REPARENT_SQL = (
    "SELECT DISTINCT vi.variable_id, vi.register_variant_id, "
    "       vab.delivery_column_name "
    "FROM variable_alias_build vab "
    "JOIN variable_instance vi ON vi.cvid = vab.cvid "
    "WHERE vi.variable_id IS NOT NULL"
)


# befolkningsframskrivningar, the one entry in the SCB adapter's
# `_PROJECTION_REGISTERS`: (name, register_id, register_variant_id).
_PROJECTION_REG = ("PROGREG", 310, 3100)


def _cis2016_answer(
    key: str,
    slug: str,
    columns: list[str],
    definition: str,
    response: tuple[str, str],
    *,
    meaning_evidence: str | None = None,
) -> dict:
    answer = {
        "key": key,
        "slug": slug,
        "columns": columns,
        "label_en": definition.removesuffix("."),
        "definition_en": definition,
        "partner": {"key": "group_enterprises", "label_en": "Group enterprises"},
        "response": {"key": response[0], "label_en": response[1]},
        "source_pages": dict.fromkeys(columns, 23),
    }
    if meaning_evidence is not None:
        answer["meaning_evidence"] = meaning_evidence
    return answer


def _cis2016_payload() -> dict:
    return {
        "selector": {
            "register": "scb/testreg",
            "register_id": 1,
            "variant": "individer",
            "register_variant_id": 10,
            "edition": "2014 - 2016",
            "regver_id": 11529,
            "var_id": 15662,
            "cvid": 469456,
        },
        "evidence": {
            "document": "Synthetic CIS2016 concordance",
            "url": "https://example.test/cis2016.pdf#page=23",
            "sha256": "a" * 64,
            "question": "Question 18",
            "noted": "2026-09-13",
        },
        "question_label": "Typ av samarbetspartner geografiskt fördelat",
        "axes": [
            {"key": "partner", "label_en": "Cooperation partner"},
            {"key": "response", "label_en": "Location or response"},
        ],
        "answers": [
            _cis2016_answer(
                "group-enterprises-sweden",
                "cis2016-cooperation-group-enterprises-sweden",
                ["CO11"],
                "Cooperation with group enterprises in Sweden.",
                ("sweden", "Sweden"),
            ),
            _cis2016_answer(
                "group-enterprises-not-applicable",
                "cis2016-cooperation-group-enterprises-not-applicable",
                ["CONA1"],
                "Cooperation with group enterprises: not applicable.",
                ("not_applicable", "Not applicable"),
            ),
        ],
    }


def _cis2014_payload() -> dict:
    return {
        "selector": {
            "register": "scb/innovation-foretag",
            "register_id": 257,
            "variant": "_default",
            "register_variant_id": 553,
            "edition": "2012 - 2014",
            "regver_id": 7293,
            "var_id": 15662,
            "cvid": 400684,
        },
        "source_mode": "documented_blank",
        "evidence": {
            "document": "Synthetic CIS2014 concordance",
            "url": "https://example.test/cis2014.pdf#page=23",
            "sha256": "b" * 64,
            "question": (
                "VariabelRegister_Källa is Fråga 18 i enkäten "
                "Innovationsverksamhet 2012-2014; the native edition is "
                "2012 - 2014, while stale VariabelReferenstid says 2010–2012."
            ),
            "noted": "2026-09-14",
        },
        "question_label": "Typ av samarbetspartner geografiskt fördelat",
        "axes": [
            {"key": "partner", "label_en": "Cooperation partner"},
            {"key": "response", "label_en": "Location or response"},
        ],
        "answers": [
            _cis2016_answer(
                "group-enterprises-sweden",
                "cis2014-cooperation-group-enterprises-sweden",
                ["CO11"],
                "Cooperation with group enterprises in Sweden.",
                ("sweden", "Sweden"),
            ),
            _cis2016_answer(
                "group-enterprises-other-europe",
                "cis2014-cooperation-group-enterprises-other-europe",
                ["CO12"],
                "Cooperation with group enterprises elsewhere in Europe.",
                ("other_europe", "Other Europe"),
            ),
        ],
    }


class TestCis2014MatrixProjection:
    def test_invalid_mode_selector_and_answer_coordinates_fail_config(
        self, tmp_path: Path
    ) -> None:
        mutations = (
            lambda payload: payload.pop("source_mode"),
            lambda payload: payload["selector"].update(cvid=400685),
            lambda payload: payload["answers"][1].update(
                key=payload["answers"][0]["key"]
            ),
            lambda payload: payload["answers"][1].update(
                response={"key": "sweden", "label_en": "Sweden"}
            ),
        )
        for index, mutate in enumerate(mutations):
            payload = _cis2014_payload()
            mutate(payload)
            path = tmp_path / f"invalid-{index}.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with pytest.raises(RegMetaError) as exc:
                load_cis2014_matrix(path)
            assert exc.value.exit_code == EXIT_CONFIG
            assert exc.value.code == "cis2014_matrix_invalid"


class TestCis2016MatrixProjection:
    def test_duplicate_and_conflicting_answer_selectors_fail_config(
        self, tmp_path: Path
    ) -> None:
        for name, mutate in (
            (
                "duplicate-column",
                lambda payload: payload["answers"][1].update(columns=["CO11"]),
            ),
            (
                "duplicate-coordinate",
                lambda payload: payload["answers"][1].update(
                    response={"key": "sweden", "label_en": "Sweden"}
                ),
            ),
        ):
            payload = _cis2016_payload()
            mutate(payload)
            if name == "duplicate-column":
                payload["answers"][1]["source_pages"] = {"CO11": 23}
            path = tmp_path / f"{name}.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with pytest.raises(RegMetaError) as exc:
                load_cis2016_matrix(path)
            assert exc.value.exit_code == EXIT_CONFIG
            assert exc.value.code == "cis2016_matrix_invalid"
