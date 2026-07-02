import { beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import BindingEditor from "./BindingEditor.svelte";
import type { Binding } from "./project_data";
import { projectStore } from "./project_store.svelte";

// #991/#993: BindingEditor is the READ-ONLY cart binding row — it DISPLAYS the
// picked variable (+ pinned representation) and offers delete only. No variable
// picker, no type <select>, no display_name input, no Advanced disclosure.

beforeEach(() => {
  // Seed a fresh draft with one source + one binding so removeBinding has a target
  // and the stable-id mirror resolves.
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
  projectStore.applyStagedDiff({
    adds: [
      {
        registerVariant: "scb/lisa/v1",
        period: 2020,
        binding: {
          variable: "scb/lisa/kon",
          type: "categorical",
          representation: "Kon",
        },
      },
    ],
  });
});

describe("BindingEditor read-only cart row", () => {
  it("displays the variable + pinned representation, with no editing affordances", async () => {
    const binding = projectStore.draft?.sources?.[0].bindings?.[0] as Binding;
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
    });

    await expect
      .element(page.getByText("scb/lisa/kon", { exact: true }))
      .toBeVisible();
    // The pinned representation (delivery column) is shown as a chip.
    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();

    // No picker / type select / display_name input / Advanced disclosure.
    expect(
      page.getByRole("button", { name: "Pick variable" }).query(),
    ).toBeNull();
    expect(page.getByRole("combobox").query()).toBeNull();
    expect(page.getByRole("textbox").query()).toBeNull();
    expect(page.getByText("Advanced").query()).toBeNull();
  });

  it("shows the '(no variable)' fallback for a binding without a variable", async () => {
    const binding = { type: "opaque" } as unknown as Binding;
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
    });

    await expect.element(page.getByText("(no variable)")).toBeVisible();
  });

  it("removes the binding through the store when 'Remove binding' is clicked", async () => {
    // Add a second binding so the source survives after one is removed.
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/v1",
          period: 2020,
          binding: {
            variable: "scb/lisa/adeldag",
            type: "opaque",
            representation: "AdelDag",
          },
        },
      ],
    });
    const binding = projectStore.draft?.sources?.[0].bindings?.[0] as Binding;
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
    });

    await page.getByRole("button", { name: "Remove binding" }).click();

    // The store dropped binding 0 (kon); adeldag survives.
    expect(
      projectStore.draft?.sources?.[0].bindings?.map((b) => b.variable),
    ).toEqual(["scb/lisa/adeldag"]);
  });
});
