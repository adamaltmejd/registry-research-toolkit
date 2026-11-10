import { describe, expect, it } from "vitest";
import { parseInlineMarkdown } from "./inline_markdown";

describe("inline markdown links", () => {
  // The steward description is deployment config rendered as an <a href>: a
  // link arm that accepted any scheme would turn `javascript:` text into a
  // live link, and the site links only https (Operator lane brief, 2026-11-10).
  // Fails if the target is not limited to https.
  it.each([
    [
      "See [SWECOV](https://swecov.se/data).",
      [
        { text: "See ", emphasis: null },
        { text: "SWECOV", emphasis: null, href: "https://swecov.se/data" },
        { text: ".", emphasis: null },
      ],
    ],
    [
      "[x](http://example.se)",
      [{ text: "[x](http://example.se)", emphasis: null }],
    ],
    [
      "[x](javascript:alert(1))",
      [{ text: "[x](javascript:alert(1))", emphasis: null }],
    ],
  ])("parses %j", (input, expected) => {
    expect(parseInlineMarkdown(input)).toEqual(expected);
  });
});
