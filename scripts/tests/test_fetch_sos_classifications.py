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

# The real file is tab-separated; "|" keeps the fixture readable. Its header ends
# in a version-stamp column that records leave out, and a CodingNote may hold
# unquoted line breaks and comes before Parent, as in WHO's file (1A00's note
# spans three lines here).
_TABULATION = """\
Linearization URI|Code|BlockId|ChapterNo|ClassKind|Title|TitleEN|CodingNote|Parent|Version:2026
mms/1|||01|chapter|Infektioner|Infections||mms
mms/2||BlockL1-1A0|01|block|- Tarm|- Gut||mms/1
mms/3||BlockL2-1A0|01|block|- - Bakt|- - Bact||mms/2
mms/4|1A00||01|category|- - - Kolera|- - - Cholera|Första stycket.\r\n\r\nAndra stycket.|mms/3
mms/5|1A00.0||01|category|- - - - Typ|- - - - Type||mms/4
mms/6|||X|chapter|Tillägg|Extension||mms
mms/7|||X|block|- Utan id|- Without id||mms/6
mms/8|XA00||X|category|- - Kod|- - Code||mms/7
""".replace("|", "\t")


def _zip(text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("SimpleTabulation-ICD-11-MMS-sv.txt", text)
    return buf.getvalue()


def test_chapters_and_blocks_take_who_identifiers_and_carry_the_parent_chain() -> None:
    # Fails if chapter/block rows are dropped again (1A00 loses its parent), a
    # nested block resolves to its chapter instead of its enclosing block, a
    # CodingNote's line breaks split 1A00's record (unknown ClassKind), or a
    # block without a BlockId is coded or stops passing XA00 to its chapter.
    rows = fetch.parse_who_icd11_simple_tabulation(_zip(_TABULATION))
    assert [(r.code, r.parent_code, r.label, r.label_en) for r in rows] == [
        ("01", "", "Infektioner", "Infections"),
        ("BlockL1-1A0", "01", "Tarm", "Gut"),
        ("BlockL2-1A0", "BlockL1-1A0", "Bakt", "Bact"),
        ("1A00", "BlockL2-1A0", "Kolera", "Cholera"),
        ("1A00.0", "1A00", "Typ", "Type"),
        ("X", "", "Tillägg", "Extension"),
        ("XA00", "X", "Kod", "Code"),
    ]
