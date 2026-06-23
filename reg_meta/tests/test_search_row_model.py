"""`_row_to_model` boundary fail-fast (#701).

`_row_to_model` is the ONLY place the dict-pipeline search rows cross into the
typed contract, dispatching on `row["type"]`. An unrecognized `type` is an
internal invariant break, so it raises `RegMetaError` rather than producing an
untyped/None row — this locks that fail-fast (no DB needed; the mapping is pure).
"""

from __future__ import annotations

import pytest
from reg_meta.errors import EXIT_NOT_FOUND, RegMetaError
from reg_meta.queries import _row_to_model


def test_row_to_model_unknown_type_fails_fast() -> None:
    with pytest.raises(RegMetaError) as exc:
        _row_to_model({"type": "bogus", "fts_rank": 0})
    assert exc.value.code == "unknown_search_row_type"
    assert exc.value.exit_code == EXIT_NOT_FOUND
