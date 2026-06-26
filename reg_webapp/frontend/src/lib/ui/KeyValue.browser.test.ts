import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import KeyValue from "./KeyValue.svelte";

// KeyValue: the contract is (1) a <dl> of term/description rows, (2) the per-row
// mono flag mono-faces an identifier value, (3) a snippet overrides the rows
// form for richer values.
describe("KeyValue", () => {
  it("renders label/value rows as a description list", async () => {
    await render(KeyValue, {
      rows: [
        { label: "Unit", value: "person" },
        { label: "Slug", value: "kon", mono: true },
      ],
    });
    await expect.element(page.getByText("Unit")).toBeVisible();
    await expect.element(page.getByText("person")).toBeVisible();
    await expect.element(page.getByText("Slug")).toBeVisible();
    await expect.element(page.getByText("kon")).toBeVisible();
  });

  it("mono-faces a flagged value", async () => {
    const { container } = render(KeyValue, {
      rows: [{ label: "Slug", value: "kon", mono: true }],
    });
    const monoDd = container.querySelector("dd.mono");
    expect(monoDd).not.toBeNull();
    expect(monoDd?.textContent).toBe("kon");
  });

  it("renders a children snippet instead of rows when provided", async () => {
    const children = createRawSnippet(() => ({
      render: () => '<div class="kv-row">custom</div>',
    }));
    await render(KeyValue, { rows: [{ label: "x", value: "y" }], children });
    await expect.element(page.getByText("custom")).toBeVisible();
    // rows are ignored when a snippet is passed.
    expect(page.getByText("x").query()).toBeNull();
  });
});
