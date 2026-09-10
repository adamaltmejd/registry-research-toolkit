import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { StatesResponse, VariableStateModel } from "./api";
import { getCatalogNode } from "./api";
import BindingEditor from "./BindingEditor.svelte";
import { bindingFieldsFromResolution } from "./catalog";
import { resetCatalogNames } from "./catalog_names.svelte";
import type { Binding } from "./project_data";
import { projectStore } from "./project_store.svelte";

// #991/#993: BindingEditor is the READ-ONLY cart column row — it DISPLAYS the
// delivery column ordered (and the variable it came from) and offers delete only.
// No variable picker, no type <select>, no display_name input, no Advanced
// disclosure. Y-75: the row LEADS with the column name, because that is what the
// researcher ordered and what lands in the extract. Y-80: where the FILE names no
// column — the ordinary pick — that name is RESOLVED from the catalog at the
// source's (variant, period), through the same leaf resolve the picker runs.

// Stub the leaf resolve; keep the rest of api.ts real (the types + path helpers
// `catalog.ts` uses) — the partial-mock pattern the catalog views use.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getCatalogNode: vi.fn() };
});

/** One `?period`-resolved state, minimal: the resolve reads only the delivery
 * column and the era that ranks it. */
function state(
  column: string | null,
  validFrom: string,
  validTo: string,
): VariableStateModel {
  return {
    state_id: 1,
    variant: "v1",
    variant_label: null,
    register_variant_id: 1,
    valid_from: validFrom,
    valid_to: validTo,
    data_type: "int",
    data_length: null,
    delivery_column_name: column,
    source_register_text: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    value_set_summary: null,
    is_identifier: false,
    classification_slug: null,
    classification_conformance: null,
  } as VariableStateModel;
}

/** The `?period`+`?variant` resolve payload for a leaf. */
function resolved(...states: VariableStateModel[]): StatesResponse {
  return { states } as unknown as StatesResponse;
}

/** The row under test, always at the source's (`v1`, 2020) — the coordinate the
 * owning card passes down. */
function renderRow(binding: Binding, period: string | null = "2020") {
  return render(BindingEditor, {
    sourceIndex: 0,
    bindingIndex: 0,
    binding,
    variant: "v1",
    period,
  });
}

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
  // The name cache is a session singleton: reset it so each case's stubbed resolve
  // is the one its row reads.
  resetCatalogNames();
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getCatalogNode).mockResolvedValue(
    resolved(state("Kon", "2010-01-01", "9999-12-31")),
  );
});

describe("BindingEditor read-only cart row", () => {
  it("leads with the pinned delivery column and links the FQID to the catalog", async () => {
    const binding = projectStore.draft?.sources?.[0].bindings?.[0] as Binding;
    await renderRow(binding);

    // The pinned representation IS the delivery column name — it leads the row…
    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    // …and the variable it came from is a link to its catalog subject page.
    await expect
      .element(page.getByRole("link", { name: "scb/lisa/kon" }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    // A file that names its own column asks the catalog nothing.
    expect(vi.mocked(getCatalogNode).mock.calls).toHaveLength(0);

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
    await renderRow(binding);

    await expect
      .element(page.getByText("AdelDag", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "scb/lisa/adeldag" }))
      .toBeVisible();
  });

  it("leads with display_name over a pinned representation when a file sets both", async () => {
    // reg_schema makes `display_name` the binding's OUTPUT column name, so it is
    // what the extract delivers even where a `representation` also pins the source
    // column the value is taken from.
    const binding = {
      variable: "scb/lisa/kon",
      type: "categorical",
      display_name: "Sex",
      representation: "Kon",
    } as unknown as Binding;
    await renderRow(binding);

    await expect.element(page.getByText("Sex", { exact: true })).toBeVisible();
    expect(page.getByText("Kon", { exact: true }).query()).toBeNull();
    // And it is what the delete button calls the row.
    await expect
      .element(page.getByRole("button", { name: "Remove column Sex" }))
      .toBeVisible();
  });

  it("resolves the column name from the catalog for a pick that names none", async () => {
    // The ORDINARY pick, through the picker's own mapping: ONE delivery column at
    // the (variant, period) leaves `representation` null and no `display_name`, so
    // the file names no column and the name is the reg_meta default — which lives
    // in the catalog, at THIS source's (variant, period).
    const binding = bindingFieldsFromResolution(
      "scb/lisa/kon",
      { kind: "derived", type: "categorical" },
      "Kon",
    );
    await renderRow(binding);

    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "scb/lisa/kon" }))
      .toBeVisible();
    // Resolved at the SOURCE's coordinate, not the variable's whole history…
    expect(vi.mocked(getCatalogNode).mock.calls[0]).toEqual([
      "scb/lisa/kon",
      { period: "2020", variant: "v1" },
    ]);
    // …and read, never written: the draft still names no column.
    expect(binding.representation).toBeNull();
    expect("display_name" in binding).toBe(false);
  });

  it("leads a rename within the period with the current name, the earlier ones in mono after it", async () => {
    // A source period spanning a rename resolves to several names for one column.
    // The picker's own rows lead with the surviving column and name the superseded
    // ones quietly; the cart row says it the same way.
    vi.mocked(getCatalogNode).mockResolvedValue(
      resolved(
        state("DINF", "1981-01-01", "1983-12-31"),
        state("DINF83", "1984-01-01", "1985-12-31"),
        state("DINF86", "1990-01-01", "9999-12-31"),
      ),
    );
    const binding = bindingFieldsFromResolution(
      "scb/lisa/disponibel-inkomst",
      { kind: "derived", type: "numeric" },
      null,
    );
    await renderRow(binding, "1981..2020");

    await expect
      .element(page.getByText("DINF86", { exact: true }))
      .toBeVisible();
    const hint = document.querySelector<HTMLElement>(".rename-hint");
    expect(hint?.textContent).toBe("was DINF, DINF83");
    // The columns are identifiers and take the machine face; the word introducing
    // them is copy and does not.
    const superseded = document.querySelector<HTMLElement>(".superseded");
    expect(superseded?.textContent).toBe("DINF, DINF83");
    expect(getComputedStyle(superseded as HTMLElement).fontFamily).toContain(
      "mono",
    );
    expect(getComputedStyle(hint as HTMLElement).fontFamily).not.toContain(
      "mono",
    );
  });

  it("keeps the FQID alone when nothing there resolves to a column", async () => {
    // Offline, or a variable outside this steward's catalog: the row must stay
    // readable and must not invent a name.
    vi.mocked(getCatalogNode).mockRejectedValue(new Error("offline"));
    const binding = bindingFieldsFromResolution(
      "scb/lisa/kon",
      { kind: "derived", type: "categorical" },
      "Kon",
    );
    await renderRow(binding);

    await expect
      .element(page.getByRole("button", { name: "Remove column scb/lisa/kon" }))
      .toBeVisible();
    expect(page.getByText("Kon", { exact: true }).query()).toBeNull();
    expect(document.body.textContent).not.toContain("(no column name)");
    await expect
      .element(page.getByRole("link", { name: "scb/lisa/kon" }))
      .toBeVisible();
  });

  it("asks the catalog nothing for a source with no period", async () => {
    // A period is what the resolve resolves AT; without one there is nothing to
    // ask, so the row shows its FQID rather than issuing a request that must fail.
    const binding = bindingFieldsFromResolution(
      "scb/lisa/kon",
      { kind: "derived", type: "categorical" },
      "Kon",
    );
    await renderRow(binding, null);

    await expect
      .element(page.getByRole("link", { name: "scb/lisa/kon" }))
      .toBeVisible();
    expect(vi.mocked(getCatalogNode).mock.calls).toHaveLength(0);
  });

  it("resolves one (fqid, period, variant) ONCE however many rows ask", async () => {
    // A hundred-column cart must not re-issue a request per row or per render.
    const binding = bindingFieldsFromResolution(
      "scb/lisa/kon",
      { kind: "derived", type: "categorical" },
      "Kon",
    );
    const first = await renderRow(binding);
    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();
    first.unmount();
    await renderRow(binding);
    await expect.element(page.getByText("Kon", { exact: true })).toBeVisible();

    expect(vi.mocked(getCatalogNode).mock.calls).toHaveLength(1);
  });

  it("shows the '(no variable)' fallback, unlinked, for a binding without a variable", async () => {
    const binding = { type: "opaque" } as unknown as Binding;
    await renderRow(binding);

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
    await renderRow(binding);

    await page.getByRole("button", { name: "Remove column" }).click();

    expect(page.getByRole("alertdialog").query()).toBeNull();
    // The store dropped binding 0 (kon); adeldag survives.
    expect(
      projectStore.draft?.sources?.[0].bindings?.map((b) => b.variable),
    ).toEqual(["scb/lisa/adeldag"]);
  });
});
