import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import Tag from "./Tag.svelte";

// Tag: the load-bearing hooks are the tone class (drives all three color
// sub-systems) and the mono face. The glyph slot is the status pairing the
// accent-vs-status rule mandates.
describe("Tag", () => {
  const label = createRawSnippet(() => ({ render: () => "<span>VAR</span>" }));

  it("renders its label", async () => {
    await render(Tag, { children: label });
    await expect.element(page.getByText("VAR")).toBeVisible();
  });

  it("applies the categorical tone class", async () => {
    const { container } = await render(Tag, { tone: "var", children: label });
    expect(container.querySelector(".tag")).toHaveClass("tone-var");
  });

  it("defaults to the neutral tone", async () => {
    const { container } = await render(Tag, { children: label });
    expect(container.querySelector(".tag")).toHaveClass("tone-neutral");
  });

  it("mono-faces a code-like tag", async () => {
    const { container } = await render(Tag, { mono: true, children: label });
    expect(container.querySelector(".tag")).toHaveClass("mono");
  });

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
