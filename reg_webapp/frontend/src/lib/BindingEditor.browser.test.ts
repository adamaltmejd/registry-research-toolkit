import { beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import BindingEditor from "./BindingEditor.svelte";
import { bindingFieldsFromResolution } from "./catalog";
import type { Binding } from "./project_data";
import { projectStore } from "./project_store.svelte";

// #991/#993: BindingEditor is the READ-ONLY cart column row — it DISPLAYS the
// delivery column ordered (and the variable it came from) and offers delete only.
// No variable picker, no type <select>, no display_name input, no Advanced
// disclosure. Y-75: the row LEADS with the column name, because that is what the
// researcher ordered and what lands in the extract.

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
        // A fixture standing for a PICK is built by the picker's own mapping, so the
        // pinned-column row cannot drift from what a pick writes (fixtures standing
        // for a hand-authored file stay literals below).
        binding: bindingFieldsFromResolution(
          "scb/lisa/kon",
          { kind: "derived", type: "categorical" },
          "Kon",
          { pinRepresentation: true },
        ),
      },
    ],
  });
});

describe("BindingEditor read-only cart row", () => {
  it("leads with the pinned delivery column and links the FQID to the catalog", async () => {
    const binding = projectStore.draft?.sources?.[0].bindings?.[0] as Binding;
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
    });

    // The pinned representation IS the delivery column name — it leads the row…
    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    // …and the variable it came from is a link to its catalog subject page.
    await expect
      .element(page.getByRole("link", { name: "scb/lisa/kon" }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");

    // No picker / type select / display_name input / Advanced disclosure.
    expect(
      page.getByRole("button", { name: "Pick variable" }).query(),
    ).toBeNull();
    expect(page.getByRole("combobox").query()).toBeNull();
    expect(page.getByRole("textbox").query()).toBeNull();
    expect(page.getByText("Advanced").query()).toBeNull();
  });

  it("leads with an explicit display_name where a file sets one", async () => {
    // A hand-authored spec may set an explicit `display_name`; reg_schema makes it
    // that binding's output column name, so it is the column the row leads with.
    const binding = {
      variable: "scb/lisa/adeldag",
      type: "opaque",
      display_name: "AdelDag",
      representation: null,
    } as unknown as Binding;
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
    });

    await expect
      .element(page.getByText("AdelDag", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "scb/lisa/adeldag" }))
      .toBeVisible();
  });

  it("leads with the variable FQID for a pick that names no column", async () => {
    // The ORDINARY pick, through the picker's own mapping: ONE delivery column at
    // the (variant, period) leaves `representation` null and no `display_name`, so
    // the file names no column and the resolved default ("Kon") stays in the
    // catalog this cart never reads. A project_data.json authored outside this app
    // carries neither field either — one branch, so one test. The row leads with
    // what the file DOES say rather than a placeholder; nothing here invents a name.
    const binding = bindingFieldsFromResolution(
      "scb/lisa/kon",
      { kind: "derived", type: "categorical" },
      "Kon",
    );
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
    });

    expect(page.getByText("Kon", { exact: true }).query()).toBeNull();
    expect(document.body.textContent).not.toContain("(no column name)");
    await expect
      .element(page.getByRole("link", { name: "scb/lisa/kon" }))
      .toBeVisible();
    // …and the delete button is named by it, so the controls list disambiguates.
    await expect
      .element(page.getByRole("button", { name: "Remove column scb/lisa/kon" }))
      .toBeVisible();
  });

  it("shows the '(no variable)' fallback, unlinked, for a binding without a variable", async () => {
    const binding = { type: "opaque" } as unknown as Binding;
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
    });

    await expect.element(page.getByText("(no variable)")).toBeVisible();
    // No variable, no subject page — the row must not link to the catalog root.
    expect(page.getByRole("link").query()).toBeNull();
  });

  it("removes one column through the store on the click, with no confirmation", async () => {
    // Y-75: a single column is a row the researcher is pointing at — only dropping
    // a whole SOURCE (the register and every column under it) asks first.
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

    await page.getByRole("button", { name: "Remove column" }).click();

    expect(page.getByRole("alertdialog").query()).toBeNull();
    // The store dropped binding 0 (kon); adeldag survives.
    expect(
      projectStore.draft?.sources?.[0].bindings?.map((b) => b.variable),
    ).toEqual(["scb/lisa/adeldag"]);
  });
});
