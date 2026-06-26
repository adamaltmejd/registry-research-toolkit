import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import KeyValue from "./KeyValue.svelte";

// KeyValue: the contract is (1) a <dl> of term/description rows, (2) the per-row
// mono flag mono-faces an identifier value, (3) the per-row `value` snippet
// renders rich content INSIDE the component-owned `.kv-row dd` (so it stays
// structurally owned/styled, unlike a caller-authored whole-row escape hatch).
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

  it("renders duplicate-label rows without crashing", async () => {
    // Keyed-each by index (not label) — two rows sharing a label must both render
    // rather than throw "Cannot have duplicate keys".
    const { container } = render(KeyValue, {
      rows: [
        { label: "Type", value: "integer" },
        { label: "Type", value: "string" },
      ],
    });
    expect(container.querySelectorAll(".kv-row")).toHaveLength(2);
    await expect.element(page.getByText("integer")).toBeVisible();
    await expect.element(page.getByText("string")).toBeVisible();
  });

  it("renders rich content via the value snippet inside the component-owned dd (Fix 4)", async () => {
    // The per-row `value` snippet replaces only the <dd> content; KeyValue keeps
    // owning the `.kv-row`/<dt>/<dd> structure, so a rich value (a Tag, a link)
    // stays inside the scoped layout instead of an unstyled caller-authored row.
    const value = createRawSnippet<[{ label: string }]>((getRow) => ({
      render: () => `<span class="rich">${getRow().label}-tag</span>`,
    }));
    const { container } = render(KeyValue, {
      rows: [{ label: "Status" }],
      value,
    });
    // The rich span is structurally owned: it lives inside `.kv-row dd`.
    const rich = container.querySelector(".kv-row dd .rich");
    expect(rich).not.toBeNull();
    expect(rich?.textContent).toBe("Status-tag");
    // The component still owns the term (exact: "Status" is a substring of the
    // rich value "Status-tag").
    await expect
      .element(page.getByText("Status", { exact: true }))
      .toBeVisible();
  });
});
