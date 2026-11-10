import { parseInlineMarkdown } from "./inline_markdown";

/** Inline Markdown text as React nodes: emphasis and https links, never HTML. */
/* oxlint-disable react/no-array-index-key -- Segments of an immutable string have no independent ids. */
export function InlineMarkdown({ text }: { text: string }) {
  return parseInlineMarkdown(text).map((segment, index) =>
    segment.href ? (
      <a key={index} className="link" href={segment.href}>
        {segment.text}
      </a>
    ) : segment.emphasis === "strong" ? (
      <strong key={index}>{segment.text}</strong>
    ) : segment.emphasis === "em" ? (
      <em key={index}>{segment.text}</em>
    ) : (
      segment.text
    ),
  );
}
