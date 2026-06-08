import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { Source } from "./project_data";
import SourceEditor from "./SourceEditor.svelte";

// Scenario 4 (#201): an opened spec can be malformed. SourceEditor must render a
// guard instead of crashing on a non-array `bindings` (the draft stays verbatim
// for serialize/validate).
describe("SourceEditor malformed-bindings guard", () => {
  it("renders an alert (not a crash) when bindings is a non-array", async () => {
    const source = {
      name: "bad",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: "oops",
    } as unknown as Source;
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

    const alert = page.getByRole("alert");
    await expect.element(alert).toBeVisible();
    await expect.element(alert).toHaveTextContent(/bindings\s+are malformed/);
  });

  it("renders the empty state (no alert) for a well-formed source with no bindings", async () => {
    const source = {
      name: "ok",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [],
    } as Source;
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

    await expect.element(page.getByText("No bindings yet.")).toBeVisible();
    expect(page.getByRole("alert").query()).toBeNull();
  });
});
