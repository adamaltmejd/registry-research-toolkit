"""Co-delivered parallel columns refuse an identifier flag that differs between them."""

from __future__ import annotations

import pytest
from _curation_compile_support import (
    pooled_parallel_fixture as _pooled_parallel_fixture,
)
from reg_meta_build.curation_tree import (
    load_register_files,
)
from reg_meta_build.source_curation import acknowledgement_evidence_sha256
from reg_meta_build.source_records import value_field


def _compile_pooled_parallel(path, records, naming):
    from reg_meta_build.curation_compile import compile_parallel_representations

    (register,) = load_register_files(path.parents[2])
    return compile_parallel_representations(register, records, naming)


# Kept until a source can author it: SCB Registerinformation records carry no
# identifier field, and every committed parallel entry is SCB.
@pytest.mark.parametrize("defect", ["identifier"])
def test_co_delivered_parallel_refuses_unsupported_common_quantity(tmp_path, defect):
    path, original, naming = _pooled_parallel_fixture(tmp_path, co_delivered=True)
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"identifier": value_field(index == 1)}
                )
            }
        )
        for index, record in enumerate(original)
    )
    # Pinned to the records as compiled, so only the identifier rule refuses.
    path.write_text(
        path.read_text().replace(
            acknowledgement_evidence_sha256(original),
            acknowledgement_evidence_sha256(records),
        )
    )
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert cases == ()
    assert diagnostics and all(d.severity == "error" for d in diagnostics)
