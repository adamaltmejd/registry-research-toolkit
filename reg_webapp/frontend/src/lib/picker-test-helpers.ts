// Shared assertions for the representation/column-picker browser tests
// (`RepresentationPicker`, `BindingLeafView`, `ConceptGroupView`). These two
// checks — "the picker's single Apply/commit button is disabled" and "a staged
// single-column row shows its '1 Column' tag" — recur across all three suites;
// extracted here so the button-name regex and the exact tag text have one home.
//
// Browser-only: imports `page` from `vitest/browser`, so only import this from
// `*.browser.test.ts` files (the browser-mode project), never a jsdom `*.test.ts`.
import { expect } from "vitest";
import { page } from "vitest/browser";
import { projectStore } from "./project_store.svelte";

/**
 * The picker footer renders ONE commit button whose accessible name follows the
 * staged-diff shape ("Add to project" | "Remove from project" | "Apply changes").
 * Tests grab it by this alternation regardless of the current label.
 */
const APPLY_BUTTON_NAME = /Add to project|Remove from project|Apply changes/;

/** Assert the footer's Apply/commit button is present and disabled. */
export async function expectApplyDisabled(): Promise<void> {
  await expect
    .element(page.getByRole("button", { name: APPLY_BUTTON_NAME }))
    .toBeDisabled();
}

/** The compact tag a staged single-column row shows (#1115). */
const STAGED_ADD_COLUMN_LABEL = "1 Column";

/** Assert the "1 Column" staged-add tag is visible. */
export async function expectStagedAddColumnVisible(): Promise<void> {
  await expect
    .element(page.getByText(STAGED_ADD_COLUMN_LABEL, { exact: true }))
    .toBeVisible();
}

// ── The source-period correction (Y-15) ──────────────────────────────────────
//
// The same review lives on the leaf and the group page, so its three accessible
// names have one home here too. `exact`: a source name is a free-form string and
// one draft name is routinely a PREFIX of another (`LISA` / `LISA_2`), which a
// substring match would silently resolve to two elements.

/** The "Change source period" button of the source named `sourceName`. */
export function changeSourcePeriod(sourceName: string) {
  return page.getByRole("button", {
    name: `Change source period for ${sourceName}`,
    exact: true,
  });
}

/** The open review's Apply button for the source named `sourceName`. */
export function applySourcePeriod(sourceName: string) {
  return page.getByRole("button", {
    name: `Apply source period for ${sourceName}`,
    exact: true,
  });
}

/** The open review's staleness alert — the notice a review that no longer describes
 * the draft raises (and the one a mere page narrowing must NOT). */
export function staleReviewAlert() {
  return page.getByRole("alert").filter({ hasText: "changed since" });
}

/** The open review's period entry (absent when no review is open). */
export function newPeriodInput() {
  return page.getByRole("textbox", { name: "New period" });
}

/** The draft's sources as `[name, period, [binding variables]]` — what a
 * correction must move on ONE source and leave alone on every other. */
export function sourceShapes(): unknown[][] {
  return (projectStore.draft?.sources ?? []).map((source) => [
    source.name,
    source.period,
    source.bindings.map((binding) => binding.variable),
  ]);
}
