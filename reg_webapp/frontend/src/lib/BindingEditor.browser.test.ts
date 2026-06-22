import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { CatalogNode, StatesResponse } from "./api";
import { getCatalogNode } from "./api";
import BindingEditor from "./BindingEditor.svelte";
import type { Binding } from "./project_data";
import { projectStore } from "./project_store.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  // BindingEditor only opens the picker in variable mode (getCatalogNode); the
  // variant fetch is never reached, so it doesn't need stubbing here.
  return { ...actual, getCatalogNode: vi.fn() };
});

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  // projectStore is a module singleton — start each test from a fresh draft with
  // one source + one empty binding for the mutator to write into.
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
  projectStore.addSource();
  projectStore.addBinding(0);
});

// Scenario 1 end-to-end (#201): a missing `register_variant` input and a pick that
// didn't prefill type were both real bugs. This drives the full pick → store path
// and asserts the derived type + display_name land on the binding.
describe("BindingEditor derive-on-pick", () => {
  it("writes the derived type + display_name to the binding when a variable is picked", async () => {
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) =>
      params
        ? ({
            states: [
              {
                delivery_column_name: "Lon",
                data_type: "int",
                value_set_id: null,
                value_set: null,
                value_set_version_label: "",
                valid_from: "2010-01-01",
                valid_to: "2020-12-31",
              },
            ],
          } as unknown as StatesResponse)
        : ({
            kind: "register",
            fqid: "scb/lisa",
            children: [{ kind: "binding", fqid: "scb/lisa/lon", name: "Lön" }],
          } as unknown as CatalogNode),
    );

    // The prop mirrors the store's empty binding; the pick writes THROUGH the
    // store mutator, which is what the assertion below reads.
    const binding: Binding = { variable: "", type: "" };
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
      registerPrefix: "scb/lisa",
      period: "2020",
      variant: "v1",
      issues: [],
    });

    // Open the inline picker, then pick the scoped variable. The picker's pick rows
    // are now Bits UI Command options (role="option"); "Pick variable" stays a
    // <button> (it's BindingEditor's open toggle, not a picker row). See #689 /
    // CatalogPicker.browser.test.ts header.
    await page.getByRole("button", { name: "Pick variable" }).click();
    await page.getByRole("option", { name: /Lön/ }).click();

    // The pick funnels through onPickVariable → projectStore.applyPickedBinding.
    await vi.waitFor(() => {
      const b = projectStore.draft?.sources[0].bindings[0];
      expect(b?.variable).toBe("scb/lisa/lon");
      expect(b?.type).toBe("numeric");
      expect(b?.display_name).toBe("Lon");
    });
  });

  // B2.1/B2.3: picking with the period UNSET must NOT silently dress the opaque
  // fallback as a real type — the binding row shows an honest "unresolved" marker.
  it("shows an unresolved marker when picked with the period unset", async () => {
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) =>
      params
        ? // resolve branch is unreachable with period null (the picker shortcuts)
          ({ states: [] } as unknown as StatesResponse)
        : ({
            kind: "register",
            fqid: "scb/lisa",
            children: [{ kind: "binding", fqid: "scb/lisa/lon", name: "Lön" }],
          } as unknown as CatalogNode),
    );

    const binding: Binding = { variable: "", type: "" };
    await render(BindingEditor, {
      sourceIndex: 0,
      bindingIndex: 0,
      binding,
      registerPrefix: "scb/lisa",
      period: null, // ← period unset
      variant: "v1",
      issues: [],
    });

    await page.getByRole("button", { name: "Pick variable" }).click();
    await page.getByRole("option", { name: /Lön/ }).click();

    // The binding stays opaque, and the marker explains why (set the period).
    await vi.waitFor(() => {
      expect(projectStore.draft?.sources[0].bindings[0]?.type).toBe("opaque");
    });
    await expect
      .element(page.getByText(/Set the source period to resolve type/))
      .toBeInTheDocument();
  });
});
