"""precheck_slugs' drift advisory on a text provider key.

Every other precheck-slugs claim runs through the CLI in `cases/cli/precheck-slugs/`.
This one needs a variable keyed by a text provider key (an SOS variable name) whose
column drifts, which the SCB-only CLI artifact does not deliver.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _fqid_slug_support import write_text_file as _write
from _slugged_db import build_slugged_db

from reg_meta_build.fqid_slugs import populate_variable_slugs, precheck_slugs

if TYPE_CHECKING:
    from pathlib import Path


def test_drifting_advisory_tolerates_nonnumeric_provider_key(tmp_path: Path):
    # `variable.provider_key` is TEXT — SOS ships a merged variable name, not
    # a numeric var_id. The advisory must report it raw, not `int()` it (that
    # would crash the otherwise non-fatal precheck on a non-SCB provider).
    d = tmp_path / "slugs"
    d.mkdir()
    _write(d / "scb.toml", "")
    conn = build_slugged_db()
    vid = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name) "
        "VALUES (1, 'BefolkningPerKommun', 'Befolkning')"
    ).lastrowid
    for yr, col in (("2000", "BefKom"), ("2010", "BefKommun")):
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, 10, ?, ?, 'int', ?)",
            (vid, f"{yr}-01-01", f"{yr}-12-31", col),
        )
    conn.commit()
    populate_variable_slugs(conn, d)
    result = precheck_slugs(conn, d)  # must not raise on the non-numeric key
    hit = [r for r in result.drifting_variables if r[2] == "BefolkningPerKommun"]
    assert len(hit) == 1
    assert hit[0][3] == "befolkning"  # name basis (cols collide-free, drift)
