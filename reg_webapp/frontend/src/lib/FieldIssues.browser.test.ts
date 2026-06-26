import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import FieldIssues from "./FieldIssues.svelte";
import type { ValidationIssue } from "./validation";

// Scenario 5 (#201): the inline per-field highlight primitive. The parent has
// already filtered the issue list to this field's pointer, so FieldIssues is
// purely presentational — these assert the two states it can be in.
describe("FieldIssues", () => {
  it("renders nothing when there are no issues for the field", async () => {
    await render(FieldIssues, { issues: [] });
    // Empty list → the whole `<ul role="alert">` is absent (not just hidden).
    expect(page.getByRole("alert").query()).toBeNull();
  });

  it("highlights a matching issue with its code label, message, and level", async () => {
    const issue: ValidationIssue = {
      code: "invalid_period",
      level: "error",
      message: "Period 2099-13 is not a valid month.",
      path: "/sources/0/period",
    };
    await render(FieldIssues, { issues: [issue] });

    await expect.element(page.getByRole("alert")).toBeVisible();
    // The raw code is mapped to a friendly label for display — pin the literal so
    // a broken/removed `invalid_period` mapping is actually caught (asserting via
    // codeLabel() would be circular: the component renders the same function).
    await expect.element(page.getByText("Invalid period")).toBeVisible();
    await expect.element(page.getByText(issue.message)).toBeVisible();
    // The issue's level drives the styling hook (`li.issue.<level>`). Only the
    // level class is meaningful — `issue` is hardcoded on every <li>.
    await expect.element(page.getByRole("listitem")).toHaveClass(/\berror\b/);
    // The redundant, NON-COLOR severity cue (WCAG 1.4.1): a visible level word —
    // it's what carries severity to AT since the glyph is aria-hidden.
    await expect.element(page.getByText("Error")).toBeVisible();
  });

  it("renders the matching non-color severity word for each level", async () => {
    // The visible level word is the load-bearing WCAG 1.4.1 cue (the glyph is
    // aria-hidden), so pin it per level — and confirm an unknown level degrades to
    // the "Info" cue (forward-compat), never the brand accent. The last case uses a
    // level outside the typed union (which the server could add), so it's cast: the
    // forward-compat degrade is a runtime guard the static type can't reach.
    const cases: { level: string; word: string }[] = [
      { level: "error", word: "Error" },
      { level: "warning", word: "Warning" },
      { level: "info", word: "Info" },
      { level: "totally_new_level", word: "Info" },
    ];
    for (const { level, word } of cases) {
      const issue = {
        code: "invalid_period",
        level,
        message: "An issue.",
        path: "/sources/0/period",
      } as ValidationIssue;
      const { unmount } = render(FieldIssues, { issues: [issue] });
      await expect.element(page.getByText(word, { exact: true })).toBeVisible();
      unmount();
    }
  });
});
