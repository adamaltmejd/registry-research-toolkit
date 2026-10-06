"""A documented-blank answer matrix at the build-catalog boundary.

Synthetic fixture, derived from the deleted
``test_convert_matrix.py::test_blank_matrix_activation_preserves_checked_donor_and_coding``:
SCB register 257 (variant 553) delivers one blank-column source instance (var_id
15662, CVID 400684, edition "2012 - 2014") whose value set is the single code
``1``/``Yes``. The register's curation TOML activates a reviewed
``documented_blank`` matrix evidence file (the deleted test's ``_matrix(blank=True)``
payload, with its two free-text evidence strings reworded to generic fixture wording)
that names two answers, CO11 and CO12, with decimal storage and both flags false.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import PIPE, var_row
from _curation_toml_boundary_support import build_scb_catalog
from _pipeline_catalog_support import report_issues

if TYPE_CHECKING:
    from pathlib import Path

SELECTOR = {
    "register": "scb/innovation-foretag",
    "register_id": 257,
    "variant": "_default",
    "register_variant_id": 553,
    "edition": "2012 - 2014",
    "regver_id": 7293,
    "var_id": 15662,
    "cvid": 400684,
}
EVIDENCE = {
    "selector": SELECTOR,
    "evidence": {
        "document": "Reviewed fixture",
        "url": "https://example.test/evidence",
        "sha256": "a" * 64,
        "question": "Question 18",
        "noted": "2026-09-13",
    },
    "question_label": "Partner location",
    "axes": [{"key": key, "label_en": key.title()} for key in ("partner", "response")],
    "answers": [
        {
            "key": key,
            "slug": f"answer-{key}",
            "columns": [column],
            "label_en": key.title(),
            "definition_en": f"Definition of {key}.",
            "partner": {"key": "group", "label_en": "Group"},
            "response": {"key": key, "label_en": key.title()},
            "source_pages": {column: 23},
        }
        for key, column in (("sweden", "CO11"), ("abroad", "CO12"))
    ],
    "source_mode": "documented_blank",
    "answer_facts": {
        "data_type": "decimal",
        "data_type_evidence": "Reviewed fixture storage type",
        "is_identifier": False,
        "is_sensitive": False,
        "flag_evidence": "Reviewed fixture adjacent wave declares both flags 0",
    },
}


def _curation() -> dict[str, str]:
    selector = ", ".join(
        f"{key} = {json.dumps(value)}" for key, value in SELECTOR.items()
    )
    return {
        "registers/scb/innovation-foretag/answers.json": json.dumps(EVIDENCE),
        "registers/scb/innovation-foretag.toml": (
            '[register]\nprovider = "scb"\nslug = "innovation-foretag"\n'
            'native_id = "257"\n'
            '[[variant]]\nnative_id = "257.553"\nslug = "_default"\n'
            '[[representation.matrix]]\nsource_mode = "documented_blank"\n'
            'evidence_file = "registers/scb/innovation-foretag/answers.json"\n'
            f"selector = {{ {selector} }}\n"
        ),
    }


def test_blank_matrix_answers_inherit_the_donor_code_list(tmp_path: Path) -> None:
    """Each declared answer becomes a variable under the donor's var_id with the
    reviewed storage facts and the blank donor's complete code list; the blank
    donor itself publishes no state."""
    built = build_scb_catalog(
        tmp_path,
        curation=_curation(),
        registerinformation_rows=[
            var_row(
                cvid=400684,
                var_id=15662,
                colname="",
                varname="Partner location",
                register=("Innovation", 257, 553),
                regver_id=7293,
                versionname="2012 - 2014",
                year="2014",
                data_type="varchar",
            )
        ],
        vardemangder_rows=[PIPE.join(["Svar", "1", "1", "Yes", "400684", "7001"])],
        valid_dates_rows=[PIPE.join(["7001", "2012-01-01", "2014-12-31"])],
    )
    assert built.result["status"] == "diagnostic_complete"
    assert report_issues(built.report) == []
    with sqlite3.connect(built.db) as conn:
        assert conn.execute(
            "SELECT v.slug, v.provider_key, v.name, v.is_identifier, v.is_sensitive, "
            "s.delivery_column_name, s.data_type, s.valid_from, s.valid_to, s.pooled "
            "FROM variable_state s JOIN variable v USING (variable_id) "
            "ORDER BY s.delivery_column_name"
        ).fetchall() == [
            (
                "answer-sweden",
                "15662",
                "Sweden",
                0,
                0,
                "CO11",
                "decimal",
                "2012-01-01",
                "2014-12-31",
                1,
            ),
            (
                "answer-abroad",
                "15662",
                "Abroad",
                0,
                0,
                "CO12",
                "decimal",
                "2012-01-01",
                "2014-12-31",
                1,
            ),
        ]
        assert conn.execute(
            "SELECT s.delivery_column_name, c.code, c.label FROM variable_state s "
            "JOIN value_set_member m USING (value_set_id) "
            "JOIN value_code c USING (code_id) ORDER BY s.delivery_column_name"
        ).fetchall() == [("CO11", "1", "Yes"), ("CO12", "1", "Yes")]
