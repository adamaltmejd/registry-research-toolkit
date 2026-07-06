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

/**
 * The picker footer renders ONE commit button whose accessible name follows the
 * staged-diff shape ("Add to project" | "Remove from project" | "Apply changes").
 * Tests grab it by this alternation regardless of the current label.
 */
export const APPLY_BUTTON_NAME =
  /Add to project|Remove from project|Apply changes/;

/** Assert the footer's Apply/commit button is present and disabled. */
export async function expectApplyDisabled(): Promise<void> {
  await expect
    .element(page.getByRole("button", { name: APPLY_BUTTON_NAME }))
    .toBeDisabled();
}

/** The compact tag a staged single-column row shows (#1115). */
export const STAGED_ADD_COLUMN_LABEL = "1 Column";

/** Assert the "1 Column" staged-add tag is visible. */
export async function expectStagedAddColumnVisible(): Promise<void> {
  await expect
    .element(page.getByText(STAGED_ADD_COLUMN_LABEL, { exact: true }))
    .toBeVisible();
}
