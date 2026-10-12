"""WHO ICD-11 simple tabulation → book rows (`parse_who_icd11_simple_tabulation`).

The fixture follows WHO's simple-tabulation readme: `ClassKind` is chapter, block or
category; chapters carry `ChapterNo`, blocks `BlockId`, categories `Code`; the
language file adds `TitleEN` and a `Parent` URI.
"""

from __future__ import annotations

import io
import zipfile

from _scripts_support import load_scripts_module

fetch = load_scripts_module("fetch_sos_classifications")

# The real file is tab-separated; "|" keeps the fixture readable.
_TABULATION = """\
Linearization URI|Parent|Code|BlockId|ChapterNo|ClassKind|Title|TitleEN
mms/1|mms|||01|chapter|Infektioner|Infections
mms/2|mms/1||BlockL1-1A0|01|block|- Tarm|- Gut
mms/3|mms/2||BlockL2-1A0|01|block|- - Bakt|- - Bact
mms/4|mms/3|1A00||01|category|- - - Kolera|- - - Cholera
mms/5|mms/4|1A00.0||01|category|- - - - Typ|- - - - Type
""".replace("|", "\t")


def _zip(text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("SimpleTabulation-ICD-11-MMS-sv.txt", text)
    return buf.getvalue()


def test_chapters_and_blocks_take_who_identifiers_and_carry_the_parent_chain() -> None:
    # Fails if chapter/block rows are dropped again (1A00 loses its parent) or
    # a nested block resolves to its chapter instead of its enclosing block.
    rows = fetch.parse_who_icd11_simple_tabulation(_zip(_TABULATION))
    assert [(r.code, r.parent_code, r.label, r.label_en) for r in rows] == [
        ("01", "", "Infektioner", "Infections"),
        ("BlockL1-1A0", "01", "Tarm", "Gut"),
        ("BlockL2-1A0", "BlockL1-1A0", "Bakt", "Bact"),
        ("1A00", "BlockL2-1A0", "Kolera", "Cholera"),
        ("1A00.0", "1A00", "Typ", "Type"),
    ]
