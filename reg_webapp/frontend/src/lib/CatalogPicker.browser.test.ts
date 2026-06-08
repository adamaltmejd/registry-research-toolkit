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
  leaf: { fqid: string; name: string },
): CatalogNode {
  return {
    kind: "register",
    fqid,
    children: [{ kind: "binding", fqid: leaf.fqid, name: leaf.name }],
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

    // `int` storage type → numeric; the delivery column is the display_name default.
    await vi.waitFor(() =>
      expect(onpickVariable).toHaveBeenCalledWith({
        variable: "scb/lisa/lon",
        type: "numeric",
        displayNameDefault: "Lon",
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
    });
  });
});
