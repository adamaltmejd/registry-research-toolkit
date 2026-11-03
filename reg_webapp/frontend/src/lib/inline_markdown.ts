/** Safe inline emphasis tokenizer. Returns text, never HTML. */

export type Emphasis = "strong" | "em";

export interface InlineSegment {
  text: string;
  /** The emphasis to render, or `null` for plain text. */
  emphasis: Emphasis | null;
}

// Markdown escapes only ASCII punctuation. Consume an escape as one token before
// looking for emphasis, so escaped delimiters cannot open or close a span.
const PUNCTUATION = String.raw`[\x21-\x2f\x3a-\x40\x5b-\x60\x7b-\x7e]`;
const ESCAPE = String.raw`\\${PUNCTUATION}`;
const UNESCAPE = new RegExp(String.raw`\\(${PUNCTUATION})`, "gu");
// The plain arm excludes backslashes: otherwise backtracking could reinterpret an
// escaped closing marker as a delimiter after consuming only its backslash.
const STAR_TEXT = String.raw`(?:${ESCAPE}|\\(?!${PUNCTUATION})|[^*\\])+?`;
const UNDERSCORE_TEXT = String.raw`(?:${ESCAPE}|\\(?!${PUNCTUATION})|[^_\\])+?`;
const SPAN = new RegExp(
  String.raw`(${ESCAPE})|\*\*(${STAR_TEXT})\*\*|\*(${STAR_TEXT})\*|(?<![\p{L}\p{N}])_(${UNDERSCORE_TEXT})_(?![\p{L}\p{N}])`,
  "gu",
);

/**
 * Pair non-nested emphasis left to right. Unbalanced markers and intra-word
 * underscores stay literal; backslashes before non-punctuation survive unchanged.
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
    const [escaped, strong, emStar, emUnderscore] = [
      match[1],
      match[2],
      match[3],
      match[4],
    ];
    segments.push({
      text: (escaped ?? strong ?? emStar ?? emUnderscore).replace(
        UNESCAPE,
        "$1",
      ),
      emphasis:
        escaped !== undefined ? null : strong !== undefined ? "strong" : "em",
    });
    lastIndex = SPAN.lastIndex;
  }

  if (lastIndex < input.length) {
    segments.push({ text: input.slice(lastIndex), emphasis: null });
  }
  return segments;
}
