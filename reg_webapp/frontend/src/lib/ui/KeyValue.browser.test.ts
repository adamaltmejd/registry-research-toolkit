import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import KeyValue from "./KeyValue.svelte";

// KeyValue: rows render through consumers (SourceEditor's source card); this
// file keeps the one case they never reach, duplicate labels.
describe("KeyValue", () => {
  it("renders duplicate-label rows without crashing", async () => {
    // Keyed-each by index (not label) — two rows sharing a label must both render
    // rather than throw "Cannot have duplicate keys".
    const { container } = await render(KeyValue, {
      rows: [
        { label: "Type", value: "integer" },
        { label: "Type", value: "string" },
      ],
    });
    expect(container.querySelectorAll(".kv-row")).toHaveLength(2);
    await expect.element(page.getByText("integer")).toBeVisible();
    await expect.element(page.getByText("string")).toBeVisible();
  });
});
