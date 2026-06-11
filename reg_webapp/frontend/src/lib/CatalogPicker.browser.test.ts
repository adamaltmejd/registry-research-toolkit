import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { CatalogNode, StatesResponse, VariantsResponse } from "./api";
import { getCatalogNode, getRegisterVariants } from "./api";
import CatalogPicker from "./CatalogPicker.svelte";

// Stub only the two catalog GETs; keep the rest of api.ts real (isCatalogNode,
// encodeFqid, types) so the picker's derive helpers run against real data shapes.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getCatalogNode: vi.fn(), getRegisterVariants: vi.fn() };
});

// A register node whose `binding` children are the pickable variables.
function registerNode(
  fqid: string,
  ...leaves: { fqid: string; name: string }[]
): CatalogNode {
  return {
    kind: "register",
    fqid,
    children: leaves.map((l) => ({
      kind: "binding",
      fqid: l.fqid,
      name: l.name,
    })),
  } as unknown as CatalogNode;
}
function statesResponse(rows: Array<Record<string, unknown>>): StatesResponse {
  return { states: rows } as unknown as StatesResponse;
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getRegisterVariants).mockReset();
});

describe("CatalogPicker", () => {
  // ── Scenario 2: open / scope ───────────────────────────────────────────────
  it("variable mode scopes to the register prefix and hints when no period is set", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      registerNode("scb/lisa", { fqid: "scb/lisa/lon", name: "Lön" }),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    // The scope (the register prefix) is shown in the title…
    await expect
      .element(page.getByText("scb/lisa", { exact: true }))
      .toBeVisible();
    // …no period → the auto-fill hint…
    await expect
      .element(page.getByText(/Set the source period to auto-fill/))
      .toBeVisible();
    // …and the list is the register's binding children only.
    await expect
      .element(page.getByRole("button", { name: /Lön/ }))
      .toBeVisible();
    // The scoped browse fetch hit exactly the register prefix.
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa");
  });

  it("variant mode lists the register's variants and emits the picked slug", async () => {
    const onpickVariant = vi.fn();
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [{ slug: "v2019", name: "2019 vintage" }],
    } as unknown as VariantsResponse);
    await render(CatalogPicker, {
      mode: "variant",
      register: "scb/lisa",
      onpickVariant,
      oncancel: vi.fn(),
    });

    await expect.element(page.getByText(/Pick a variant of/)).toBeVisible();
    await page.getByRole("button", { name: /v2019/ }).click();
    expect(onpickVariant).toHaveBeenCalledWith("v2019");
  });

  // ── Scenario 1: derive-on-pick prefill ─────────────────────────────────────
  it("derives type + display_name from the resolved state on pick", async () => {
    const onpickVariable = vi.fn();
    // Browse (no params) → register node; resolve (with params) → one numeric state.
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) =>
      params
        ? statesResponse([
            {
              delivery_column_name: "Lon",
              data_type: "int",
              value_set_id: null,
              value_set: null,
              value_set_version_label: "",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            },
          ])
        : registerNode("scb/lisa", { fqid: "scb/lisa/lon", name: "Lön" }),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: "2020",
      variant: "v1",
      onpickVariable,
      oncancel: vi.fn(),
    });

    await page.getByRole("button", { name: /Lön/ }).click();

    // `int` storage type → numeric; the delivery column is the display_name
    // default. A single-representation derive now explicitly clears
    // `representation: null` (the shared resolveBindingAt path), so a re-pick of a
    // multi-rep variable can't leave a stale representation behind.
    await vi.waitFor(() =>
      expect(onpickVariable).toHaveBeenCalledWith({
        variable: "scb/lisa/lon",
        type: "numeric",
        displayNameDefault: "Lon",
        representation: null,
        // The picker emits the ground-truth resolution kind so the consumer never
        // re-infers status from value tells.
        resolution: "derived",
      }),
    );
    // The resolve carried the source's (period, variant).
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/lon", {
      period: "2020",
      variant: "v1",
    });
  });

  it("opens a representation chooser when a concept has >1 co-existing column", async () => {
    const onpickVariable = vi.fn();
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) =>
      params
        ? statesResponse([
            {
              delivery_column_name: "Ssyk3",
              data_type: "",
              value_set_id: 1,
              value_set: [{}, {}, {}],
              value_set_version_label: "3-digit",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            },
            {
              delivery_column_name: "Ssyk4",
              data_type: "",
              value_set_id: 2,
              value_set: [{}, {}, {}, {}],
              value_set_version_label: "4-digit",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            },
          ])
        : registerNode("scb/lisa", { fqid: "scb/lisa/ssyk", name: "SSYK" }),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: "2020",
      variant: "v1",
      onpickVariable,
      oncancel: vi.fn(),
    });

    await page.getByRole("button", { name: /SSYK/ }).click();

    // Ambiguous → defer to the chooser instead of emitting immediately.
    await expect
      .element(page.getByRole("group", { name: "Pick a representation" }))
      .toBeVisible();
    expect(onpickVariable).not.toHaveBeenCalled();

    // Choosing a column emits it as `representation` with a categorical type.
    await page.getByRole("button", { name: /Ssyk4/ }).click();
    expect(onpickVariable).toHaveBeenCalledWith({
      variable: "scb/lisa/ssyk",
      type: "categorical",
      displayNameDefault: "Ssyk4",
      representation: "Ssyk4",
      // A chooser pick is always a concrete derive.
      resolution: "derived",
    });
  });

  // ── Scenario 3: type-to-filter the variable list ───────────────────────────
  it("autofocuses the filter and narrows the variable list (name + fqid, folded)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      registerNode(
        "scb/lisa",
        { fqid: "scb/lisa/kon", name: "Kön" },
        { fqid: "scb/lisa/agi_2019", name: "AGI 2019" },
        { fqid: "scb/lisa/agi_2020", name: "AGI 2020" },
      ),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    const filter = page.getByRole("textbox", { name: "Filter variables" });
    await expect.element(filter).toBeVisible();
    // The filter is focused on open (the authoring blocker this PR fixes).
    await vi.waitFor(() =>
      expect(filter.element()).toBe(document.activeElement),
    );

    // Diacritic-blind, case-insensitive: "kon" surfaces "Kön", drops the agi rows.
    await filter.fill("kon");
    await expect
      .element(page.getByRole("button", { name: /Kön/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("button", { name: /AGI 2019/ }))
      .not.toBeInTheDocument();
    // Result count reflects the narrowed list.
    await expect.element(page.getByText("1 of 3")).toBeVisible();

    // Matching on the FQID slug, not just the display name.
    await filter.fill("agi_2020");
    await expect
      .element(page.getByRole("button", { name: /AGI 2020/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("button", { name: /Kön/ }))
      .not.toBeInTheDocument();
  });

  it("ranks an exact match first even when it sorts late alphabetically", async () => {
    // Incoming alphabetical order puts "Kön" LAST — only prefix-priority
    // ranking can hoist it to row 1 (the lead's target-hunt requirement).
    vi.mocked(getCatalogNode).mockResolvedValue(
      registerNode(
        "scb/lisa",
        {
          fqid: "scb/lisa/anstku",
          name: "Antal anställda enligt kontrolluppgift",
        },
        {
          fqid: "scb/lisa/dispke",
          name: "Disponibel inkomst per konsumtionsenhet",
        },
        { fqid: "scb/lisa/kefam", name: "Konsumtionsenheter, familj" },
        { fqid: "scb/lisa/kon", name: "Kön" },
      ),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    await page.getByRole("textbox", { name: "Filter variables" }).fill("kon");

    // "Kön" must be the FIRST pick row (folded-exact on slug `kon` + name `kon`),
    // despite sorting last alphabetically — proves the ranking, not a coincidence.
    // Buttons in document order: [0] = Cancel (picker head), [1] = first pick row.
    const firstPick = page.getByRole("button").nth(1);
    await expect.element(firstPick).toBeVisible();
    await vi.waitFor(() =>
      expect(firstPick.element().textContent).toContain("Kön"),
    );
  });
});
