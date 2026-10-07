import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { render } from "vitest-browser-svelte";
import Tag from "./Tag.svelte";

// Tag: labels render through every consumer; this file keeps the glyph slot,
// the status pairing the accent-vs-status rule mandates, which must stay hidden
// from the a11y tree.
describe("Tag", () => {
  const label = createRawSnippet(() => ({ render: () => "<span>VAR</span>" }));

  it("renders a leading glyph for status tones, hidden from a11y", async () => {
    const glyph = createRawSnippet(() => ({ render: () => "<span>✕</span>" }));
    const { container } = await render(Tag, {
      tone: "error",
      glyph,
      children: label,
    });
    const glyphEl = container.querySelector(".glyph");
    expect(glyphEl).not.toBeNull();
    expect(glyphEl).toHaveAttribute("aria-hidden", "true");
  });
});
