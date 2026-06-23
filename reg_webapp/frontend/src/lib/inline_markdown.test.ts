import { describe, expect, it } from "vitest";
import { type InlineSegment, parseInlineMarkdown } from "./inline_markdown";

// The parser returns DATA segments only (never HTML). These tests pin the SAFE
// emphasis grammar (#672): `**…**` → strong (the FTS highlight delimiter), `*…*`
// and `_…_` → em, everything else literal. The key safety invariant is that nothing
// is escaped or HTML-ized here — special chars survive verbatim in segment `text`;
// the auto-escape happens at the Svelte interpolation boundary in DocMentionsPanel.
// (A matched span's delimiters ARE dropped — the marker is replaced by the element.)

/** Re-join every segment's text (= the input minus the paired emphasis delimiters). */
function joined(segments: InlineSegment[]): string {
  return segments.map((s) => s.text).join("");
}

describe("parseInlineMarkdown", () => {
  it("passes plain text through as a single null-emphasis segment", () => {
    expect(parseInlineMarkdown("just plain words")).toEqual([
      { text: "just plain words", emphasis: null },
    ]);
  });

  it("maps `**term**` to a single strong (FTS highlight) segment", () => {
    expect(parseInlineMarkdown("**term**")).toEqual([
      { text: "term", emphasis: "strong" },
    ]);
  });

  it("maps `*it*` and `_it_` to em segments", () => {
    expect(parseInlineMarkdown("*it*")).toEqual([
      { text: "it", emphasis: "em" },
    ]);
    expect(parseInlineMarkdown("_it_")).toEqual([
      { text: "it", emphasis: "em" },
    ]);
  });

  it("splits leading/trailing literal text around a span", () => {
    expect(parseInlineMarkdown("a **b** c")).toEqual([
      { text: "a ", emphasis: null },
      { text: "b", emphasis: "strong" },
      { text: " c", emphasis: null },
    ]);
  });

  it("handles multiple and adjacent spans in one string", () => {
    expect(parseInlineMarkdown("**x** plain *y* _z_")).toEqual([
      { text: "x", emphasis: "strong" },
      { text: " plain ", emphasis: null },
      { text: "y", emphasis: "em" },
      { text: " ", emphasis: null },
      { text: "z", emphasis: "em" },
    ]);
    // Adjacent (no separator) spans.
    expect(parseInlineMarkdown("**a**_b_")).toEqual([
      { text: "a", emphasis: "strong" },
      { text: "b", emphasis: "em" },
    ]);
  });

  it("preserves `<`, `>`, `&` verbatim in segment text (escape boundary intact)", () => {
    // No HTML is produced here; special chars ride through as literal text so the
    // downstream Svelte interpolation escapes them. A `<b>` must NOT become markup.
    expect(parseInlineMarkdown("foo <b>bar</b> & baz")).toEqual([
      { text: "foo <b>bar</b> & baz", emphasis: null },
    ]);
    // Special chars survive inside an emphasis span too.
    expect(parseInlineMarkdown("**<i>hi</i>**")).toEqual([
      { text: "<i>hi</i>", emphasis: "strong" },
    ]);
  });

  it("leaves unbalanced / orphan markers as literal text", () => {
    for (const orphan of ["a ** b", "*a", "a_", "**unterminated", "_lonely"]) {
      const segments = parseInlineMarkdown(orphan);
      expect(joined(segments)).toBe(orphan);
      expect(segments.every((s) => s.emphasis === null)).toBe(true);
    }
  });

  it("pairs interleaved single stars left-to-right (non-nested grammar)", () => {
    // `* and *` between two orphan stars reads as one em span — a deliberate,
    // SAFE consequence of left-to-right pairing on an inherently ambiguous string.
    // The output is still auto-escaped data, never markup; we pin it so the
    // behavior is intentional, not accidental.
    expect(parseInlineMarkdown("orphan ** and *partial")).toEqual([
      { text: "orphan *", emphasis: null },
      { text: " and ", emphasis: "em" },
      { text: "partial", emphasis: null },
    ]);
  });

  it("does not treat empty markers `****` or `**` as a span", () => {
    // A span needs at least one inner char; bare/empty delimiters stay literal.
    expect(parseInlineMarkdown("****")).toEqual([
      { text: "****", emphasis: null },
    ]);
    expect(parseInlineMarkdown("**")).toEqual([{ text: "**", emphasis: null }]);
  });

  it("keeps intra-word `_` literal (snake_case identifiers don't italicize)", () => {
    // CommonMark flanking: an `_` flanked by word chars opens/closes nothing, so
    // SCB snake_case column/variable names render verbatim as ONE plain segment.
    expect(parseInlineMarkdown("value_set_version")).toEqual([
      { text: "value_set_version", emphasis: null },
    ]);
    expect(parseInlineMarkdown("bost_omr_kod")).toEqual([
      { text: "bost_omr_kod", emphasis: null },
    ]);
  });

  it("still highlights `**…**` inside a snake_case run, underscores intact", () => {
    // The `**` arm wins the alternation and is unaffected by the `_` flanking guard;
    // the surrounding `_` underscores survive as literal text.
    expect(parseInlineMarkdown("lev_**kommun**_kod")).toEqual([
      { text: "lev_", emphasis: null },
      { text: "kommun", emphasis: "strong" },
      { text: "_kod", emphasis: null },
    ]);
  });

  it("still italicizes a standalone or punctuation-flanked `_word_`", () => {
    expect(parseInlineMarkdown("_word_")).toEqual([
      { text: "word", emphasis: "em" },
    ]);
    expect(parseInlineMarkdown("a _two words_ b")).toEqual([
      { text: "a ", emphasis: null },
      { text: "two words", emphasis: "em" },
      { text: " b", emphasis: null },
    ]);
    // Flanked by punctuation (not word chars) → still emphasis.
    expect(parseInlineMarkdown("(_x_)")).toEqual([
      { text: "(", emphasis: null },
      { text: "x", emphasis: "em" },
      { text: ")", emphasis: null },
    ]);
  });

  it("returns an empty list for an empty string", () => {
    expect(parseInlineMarkdown("")).toEqual([]);
  });

  it("drops only paired delimiters; literal text (incl. orphans + special chars) survives verbatim", () => {
    // Re-joining segment text reproduces the input MINUS the paired emphasis
    // delimiters — and nothing else is altered (no escaping, no marker loss for
    // orphans, no HTML-izing).
    const cases: [input: string, joined: string][] = [
      ["", ""],
      ["plain", "plain"],
      ["**a** b *c* _d_ e", "a b c d e"], // paired delimiters stripped
      ["a ** b _c", "a ** b _c"], // pure orphans (no inner pair) survive verbatim
      ["…**match**… 24-char window", "…match… 24-char window"],
      ["<script>alert(1)</script>", "<script>alert(1)</script>"], // never escaped/HTML-ized
    ];
    for (const [input, expected] of cases) {
      expect(joined(parseInlineMarkdown(input))).toBe(expected);
    }
  });
});
