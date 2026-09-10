import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import ConfirmDialog from "./ConfirmDialog.svelte";

// ConfirmDialog: the contract is (1) nothing in the document while closed, (2) an
// alertdialog named by `title` and described by the `description` snippet when open,
// (3) the `actions` snippet renders the answers, and (4) Escape is a way out that
// reports through `onOpenChange`, so a caller whose truth lives in a store can undo
// the question it asked. The modal machinery itself is Bits UI's AlertDialog.

const text = (t: string) =>
  createRawSnippet(() => ({ render: () => `<p>${t}</p>` }));
const button = (t: string) =>
  createRawSnippet(() => ({
    render: () => `<button type="button">${t}</button>`,
  }));

describe("ConfirmDialog", () => {
  it("renders nothing while closed", async () => {
    await render(ConfirmDialog, {
      open: false,
      title: "Remove LISA and its 2 columns?",
      description: text("Every column goes with it."),
      actions: button("Remove source"),
    });

    expect(page.getByRole("alertdialog").query()).toBeNull();
    expect(
      page.getByRole("button", { name: "Remove source" }).query(),
    ).toBeNull();
  });

  it("is an alertdialog carrying its question, its cost and its answers", async () => {
    await render(ConfirmDialog, {
      open: true,
      title: "Remove LISA and its 2 columns?",
      description: text("Every column goes with it."),
      actions: button("Remove source"),
    });

    const dialog = page.getByRole("alertdialog");
    await expect.element(dialog).toBeVisible();
    // The title NAMES the dialog for assistive tech, not just the eye.
    await expect
      .element(dialog)
      .toHaveAccessibleName("Remove LISA and its 2 columns?");
    await expect
      .element(dialog)
      .toHaveAccessibleDescription("Every column goes with it.");
    await expect
      .element(dialog.getByRole("button", { name: "Remove source" }))
      .toBeVisible();
  });

  it("reports Escape through onOpenChange so the caller can drop the question", async () => {
    let reported: boolean | null = null;
    await render(ConfirmDialog, {
      open: true,
      onOpenChange: (open: boolean) => {
        reported = open;
      },
      title: "Remove LISA and its 2 columns?",
      description: text("Every column goes with it."),
      actions: button("Remove source"),
    });

    await expect.element(page.getByRole("alertdialog")).toBeVisible();
    await page
      .getByRole("alertdialog")
      .element()
      .dispatchEvent(
        new KeyboardEvent("keydown", { key: "Escape", bubbles: true }),
      );

    await expect.poll(() => reported).toBe(false);
  });
});
