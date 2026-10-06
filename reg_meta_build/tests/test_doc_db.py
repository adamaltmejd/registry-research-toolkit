from __future__ import annotations

from reg_meta_build.doc_db import parse_frontmatter


def test_parse_frontmatter_folded_block_scalar() -> None:
    """A ``>-`` folded scalar joins continuation lines with single spaces."""
    text = (
        "---\n"
        "variable: KU2AstSNI2002B\n"
        "display_name: >-\n"
        "  Summa inkomst föranledd av\n"
        "  förtidspension/sjukbidrag/sjukersättning/aktivitetsersättning\n"
        "tags:\n"
        "  - type/variable\n"
        'source: "lisa-bakgrundsfakta-1990-2017"\n'
        "---\n\nBody.\n"
    )
    meta, body = parse_frontmatter(text)
    assert meta["display_name"] == (
        "Summa inkomst föranledd av "
        "förtidspension/sjukbidrag/sjukersättning/aktivitetsersättning"
    )
    # Keys after the block scalar and existing behavior must still parse.
    assert meta["variable"] == "KU2AstSNI2002B"
    assert meta["tags"] == ["type/variable"]
    assert meta["source"] == "lisa-bakgrundsfakta-1990-2017"
    assert body == "Body.\n"


def test_parse_frontmatter_literal_block_scalar() -> None:
    """A ``|-`` literal scalar joins continuation lines with newlines."""
    text = "---\nvariable: Test\nnotes: |-\n  line one\n  line two\n---\n\nBody.\n"
    meta, _ = parse_frontmatter(text)
    assert meta["notes"] == "line one\nline two"
    assert meta["variable"] == "Test"


def test_parse_frontmatter_plain_multiline_flow_scalar() -> None:
    """A plain (no ``>``/``|``) wrapped value folds its continuation lines.

    Regression: panache emits a bare indented continuation for a long value with
    no quote-forcing characters; the parser must fold it, not truncate to the
    first physical line.
    """
    text = (
        "---\n"
        "variable: Test\n"
        "display_name: Ett långt namn som är\n"
        "  väldigt långt och bryts över rader\n"
        "tags:\n"
        "  - type/variable\n"
        "---\n\nBody.\n"
    )
    meta, body = parse_frontmatter(text)
    assert (
        meta["display_name"]
        == "Ett långt namn som är väldigt långt och bryts över rader"
    )
    # Keys after the plain-flow scalar and existing behavior must still parse.
    assert meta["variable"] == "Test"
    assert meta["tags"] == ["type/variable"]
    assert body == "Body.\n"


def test_parse_frontmatter_committed_lisa_shape() -> None:
    """Golden-style: a real committed lisa ``>-`` frontmatter parses fully.

    Mirrors reg_meta_build/docs/lisa/KU2AstSNI2007G.md so a change in the
    generator's block-scalar output surfaces here.
    """
    text = (
        "---\n"
        "variable: KU2AstSNI2007G\n"
        "display_name: >-\n"
        "  Näringsgrenstillhörighet enligt SNI2007 (arbetsställe, näst största förvärvskälla),\n"
        "  grov nivå\n"
        "tags:\n"
        "  - topic/identifier\n"
        "  - type/variable\n"
        'source: "lisa-bakgrundsfakta-1990-2017"\n'
        "---\n\nBody.\n"
    )
    meta, _ = parse_frontmatter(text)
    assert meta["display_name"] == (
        "Näringsgrenstillhörighet enligt SNI2007 (arbetsställe, näst största "
        "förvärvskälla), grov nivå"
    )
    assert meta["variable"] == "KU2AstSNI2007G"
    assert meta["tags"] == ["topic/identifier", "type/variable"]
    assert meta["source"] == "lisa-bakgrundsfakta-1990-2017"


def test_parse_frontmatter_same_line_scalars_and_list_unchanged() -> None:
    """Same-line quoted/unquoted scalars and simple lists parse as before."""
    text = (
        "---\n"
        "variable: Test\n"
        'display_name: "Quoted name"\n'
        "tags:\n"
        "  - topic/identifier\n"
        "  - type/variable\n"
        "---\n\nBody.\n"
    )
    meta, body = parse_frontmatter(text)
    assert meta["variable"] == "Test"
    assert meta["display_name"] == "Quoted name"
    assert meta["tags"] == ["topic/identifier", "type/variable"]
    assert body == "Body.\n"
