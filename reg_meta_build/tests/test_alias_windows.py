from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta_build.alias_windows import (
    load_alias_windows,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_alias_window_loader_rejects_invalid_noted_date(tmp_path: Path) -> None:
    path = tmp_path / "alias_windows.toml"
    path.write_text(
        '[[alias]]\nvariable = "scb/testreg/test-variable"\n'
        'variant = "test-variant"\ncolumn = "AEBUY"\n'
        'source_editions = ["2018"]\nevidence = "held"\nnoted = "soon"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_alias_windows(path)

    assert "YYYY-MM-DD" in exc_info.value.message
