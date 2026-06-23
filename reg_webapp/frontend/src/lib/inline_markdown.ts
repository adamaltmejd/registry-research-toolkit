/**
 * Pure, dependency-free tokenizer for the SAFE inline-emphasis subset rendered in
 * the "Mentioned in documentation" snippets (`DocMentionsPanel.svelte`, #672). NO
 * runes, NO markdown library — unit-testable in isolation (`inline_markdown.test.ts`).
 *
 * The input snippet mixes TWO emphasis sources, both spelled with `**`/`*`/`_`:
 *   • the FTS5 `snippet()` highlight delimiter `**…**` wrapping the matched term
 *     (set in `reg_meta/src/reg_meta/doc_queries.py`), and
 *   • literal markdown emphasis from the converted source body.
 * Both are tokenized identically here — the CONSUMER decides the element (the panel
 * renders `strong` as a `<mark>` "matched term", `em` as `<em>`).
 *
 * This returns DATA ONLY — never HTML. The caller renders each segment's `text`
 * through normal Svelte interpolation (auto-escaped); NEVER `{@html}`. So special
 * characters (`<`, `>`, `&`) ride through verbatim in `text` and the escape boundary
 * stays intact (republication policy; see reg_webapp/DESIGN.md → Docs library
 * endpoints).
 *
 * Grammar: non-nested, left-to-right pairing. `**x**` → strong; `*x*` and `_x_` →
 * em. UNBALANCED / orphan markers stay literal (`a ** b`, `*a`, `a_` render verbatim).
 *
 * INTRA-WORD `_` STAYS LITERAL (CommonMark flanking rule): an `_` opens/closes
 * emphasis only when it is NOT flanked by a word char (letter/number). This keeps
 * snake_case identifiers from SCB register docs (`value_set_version`, `bost_omr_kod`)
 * rendering verbatim instead of italicizing their middle token. `*`/`**` keep the
 * CommonMark allowance for intra-word use (not the hazard here) and are left as-is.
 */

export type Emphasis = "strong" | "em";

export interface InlineSegment {
  text: string;
  /** The emphasis to render, or `null` for plain text. */
  emphasis: Emphasis | null;
}

// Match an emphasis span OR a `**` opener that has no closer downgrades to text:
//   **x**  → strong   (non-greedy, at least one char between the delimiters)
//   *x*    → em
//   _x_    → em        (only when the `_` pair is NOT flanked by word chars)
// `[^*]` / `[^_]` inner classes keep the pairing non-nested and left-to-right and
// stop a `*` span from swallowing a `**` delimiter. An unmatched marker simply
// doesn't match the pattern and falls through to the literal text run.
// The `_` arm carries CommonMark flanking guards — a preceding/following letter or
// number (`u` flag → å/ä/ö count) disqualifies the marker, so intra-word `_` in
// snake_case identifiers stays literal. The guards are zero-width, so `[^_]` (and
// `[^*]`) behave identically under `u`.
const SPAN =
  /\*\*([^*]+?)\*\*|\*([^*]+?)\*|(?<![\p{L}\p{N}])_([^_]+?)_(?![\p{L}\p{N}])/gu;

/**
 * Split a raw snippet into ordered emphasis segments. Empty input → `[]`;
 * marker-free input → a single plain segment. A matched span's delimiters are
 * dropped (the marker is presentation, replaced by the element); everything else —
 * literal runs, special chars, and ORPHAN/unbalanced markers — survives verbatim in
 * a segment's `text` (so the input is reproduced minus only the paired delimiters,
 * and the auto-escape boundary stays intact).
 */
export function parseInlineMarkdown(input: string): InlineSegment[] {
  if (!input) return [];

  const segments: InlineSegment[] = [];
  let lastIndex = 0;
  SPAN.lastIndex = 0;

  for (let match = SPAN.exec(input); match !== null; match = SPAN.exec(input)) {
    if (match.index > lastIndex) {
      segments.push({
        text: input.slice(lastIndex, match.index),
        emphasis: null,
      });
    }
    // Group 1 = `**strong**`; groups 2/3 = `*em*` / `_em_`.
    const [strong, emStar, emUnderscore] = [match[1], match[2], match[3]];
    if (strong !== undefined) {
      segments.push({ text: strong, emphasis: "strong" });
    } else {
      segments.push({
        text: (emStar ?? emUnderscore) as string,
        emphasis: "em",
      });
    }
    lastIndex = SPAN.lastIndex;
  }

  if (lastIndex < input.length) {
    segments.push({ text: input.slice(lastIndex), emphasis: null });
  }
  return segments;
}
