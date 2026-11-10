/** Safe inline emphasis and link tokenizer. Returns text, never HTML. */

type Emphasis = "strong" | "em";

interface InlineSegment {
  text: string;
  /** The emphasis to render, or `null` for plain text. */
  emphasis: Emphasis | null;
  /** An `https` link target; any other scheme, `http` too, stays literal text
   * (Operator lane brief 2026-11-10, PR #2391). */
  href?: string;
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
// `[text](https://…)`, https only: the text holds no bracket and no nested
// span, the target no whitespace or parenthesis.
const LINK_TEXT = String.raw`(?:${ESCAPE}|\\(?!${PUNCTUATION})|[^\[\]\\])+?`;
const LINK = String.raw`\[(${LINK_TEXT})\]\((https:\/\/[^\s()]+)\)`;
const SPAN = new RegExp(
  String.raw`(${ESCAPE})|${LINK}|\*\*(${STAR_TEXT})\*\*|\*(${STAR_TEXT})\*|(?<![\p{L}\p{N}])_(${UNDERSCORE_TEXT})_(?![\p{L}\p{N}])`,
  "gu",
);

/**
 * Pair non-nested emphasis and links left to right. Unbalanced markers and intra-word
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
    const [escaped, linkText, href, strong, emStar, emUnderscore] = [
      match[1],
      match[2],
      match[3],
      match[4],
      match[5],
      match[6],
    ];
    segments.push({
      text: (escaped ?? linkText ?? strong ?? emStar ?? emUnderscore).replace(
        UNESCAPE,
        "$1",
      ),
      emphasis:
        escaped !== undefined || linkText !== undefined
          ? null
          : strong !== undefined
            ? "strong"
            : "em",
      ...(href !== undefined ? { href } : {}),
    });
    lastIndex = SPAN.lastIndex;
  }

  if (lastIndex < input.length) {
    segments.push({ text: input.slice(lastIndex), emphasis: null });
  }
  return segments;
}
