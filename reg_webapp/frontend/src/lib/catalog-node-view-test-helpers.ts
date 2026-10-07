// Register-node fixtures and Y-83 add-flow helpers shared by the split
// CatalogNodeView.{register,add-columns,add-eras}.browser.test.ts files.
// Browser-only: imports `vitest/browser`.
import { expect, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { CatalogNode, StatesResponse, VariableStateModel } from "./api";
import { getCatalogNode } from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";
import { variant, variantsResponse } from "./variants-test-helpers";

/** One `(variant, column)` delivery on a register child (Y-82), over one era per
 * `[from, to]` pair — `to` null is a still-delivered window. Two or more eras is an
 * INTERRUPTED column (Y-104): the wire carries each era in `windows`, and `coverage`
 * is the span over them, which cannot express the gap. NO eras is the boundless
 * delivery `VariableDelivery` documents — an alias spelling on a variant with no
 * states of its own. */
export function delivery(
  variant: string,
  column: string,
  ...eras: [string, string | null][]
) {
  const last = eras.at(-1);
  return {
    variant,
    column,
    coverage: {
      coverage_from: eras[0]?.[0] ?? null,
      coverage_to: last?.[1] ?? null,
      open_ended: last?.[1] === null,
      state_count: eras.length,
    },
    windows: eras.map(([from, to]) => ({
      valid_from: from,
      valid_to: to ?? "9999-12-31",
    })),
  };
}

// A register node whose children carry Y-82 `deliveries` — the LISA shape the
// ticket names: `forvink-ers` is delivered as `ForvErs` (a name its slug does not
// contain), `forversnetto` as `ForvErsNetto`, `disp` under TWO columns across a
// rename, and `arbetsstalle` under a SECOND variant. `variants` selects how many
// variants the register is delivered by: one (no chips) or both (chips).
export function columnedRegisterNode(variants: 1 | 2): CatalogNode {
  const children = [
    {
      kind: "binding",
      fqid: "scb/lisa/kon",
      name: "Kön",
      deliveries: [delivery("individer-15plus", "Kon", ["2018-01-01", null])],
    },
    {
      kind: "binding",
      fqid: "scb/lisa/forvink-ers",
      name: "Förvärvsinkomst",
      deliveries: [
        delivery("individer-15plus", "ForvErs", ["1990-01-01", "2021-12-31"]),
      ],
    },
    {
      kind: "binding",
      fqid: "scb/lisa/forversnetto",
      name: "Förvärvsinkomst netto",
      deliveries: [
        delivery("individer-15plus", "ForvErsNetto", ["2011-01-01", null]),
      ],
    },
    {
      kind: "binding",
      fqid: "scb/lisa/disp",
      name: "Disponibel inkomst",
      deliveries: [
        delivery("individer-15plus", "CDISP", ["1968-01-01", "2019-12-31"]),
        delivery("individer-15plus", "CDISP5", ["2020-01-01", null]),
      ],
    },
  ];
  if (variants === 2) {
    children.push({
      kind: "binding",
      fqid: "scb/lisa/arbetsstalle",
      name: "Arbetsställe",
      deliveries: [delivery("arbetsstallen", "ArbstNr", ["2005-01-01", null])],
    });
  }
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children,
  } as unknown as CatalogNode;
}

/** The variants the Y-82 fixtures are delivered by, NAMED — the chips read as
 * these words, not as the slugs that key them. */
export function lisaVariants() {
  return variantsResponse(
    variant("individer-15plus", { name: "Individer, 15 år och äldre" }),
    variant("individer-16plus", { name: "Individer, 16 år och äldre" }),
    variant("arbetsstallen", { name: "Arbetsställen" }),
  );
}

// One variable delivered under a DIFFERENT column by each variant — the ticket's
// `disp` shape (`CDISP` then `CDISP5`), split so that no single variant ships
// both. `kon` is delivered by both variants under one name, so the register has
// two chips whichever way the lens goes.
export function splitColumnRegisterNode(): CatalogNode {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    children: [
      {
        kind: "binding",
        fqid: "scb/lisa/kon",
        name: "Kön",
        deliveries: [
          delivery("individer-15plus", "Kon", ["1990-01-01", null]),
          delivery("individer-16plus", "Kon", ["1990-01-01", null]),
        ],
      },
      {
        kind: "binding",
        fqid: "scb/lisa/disp",
        name: "Disponibel inkomst",
        deliveries: [
          delivery("individer-16plus", "CDISP", ["1968-01-01", "2019-12-31"]),
          delivery("individer-15plus", "CDISP5", ["2020-01-01", null]),
        ],
      },
    ],
  } as unknown as CatalogNode;
}

/** Toggle a variant chip by the NAME it reads as — clicking the chip label, as a
 * pointer does: the checkbox it wraps is visually hidden (present for the keyboard
 * and assistive tech). Waits for the name first: the chips read slugs until the
 * register's variants land. EXACT: the list's own delivery-column ticks (Y-83) are
 * checkboxes too, and a chip's slug is a prefix of a column name often enough
 * (`foretag` / `ForetagNr`) that a substring match is ambiguous. */
export async function clickVariantChip(name: string): Promise<void> {
  const checkbox = page.getByRole("checkbox", { name, exact: true });
  await expect.element(checkbox).toBeInTheDocument();
  checkbox.element().closest<HTMLLabelElement>("label")?.click();
}

// ── Y-83: adding delivery columns straight from the register list ────────────
// The register page is an authoring surface now: each delivery column carries a
// tick, and one action adds every ticked column to the project through the same
// staged add → resolve → commit stack the variable pages use.

/** One state of a variable, as the `?period` resolve returns it, so the committed
 * binding carries a real type. */
export function columnState(
  variant: string,
  column: string,
  { id = 1, from = "1990-01-01", to = "9999-12-31" } = {},
): VariableStateModel {
  return {
    warning_ids: [],
    state_id: String(id),
    period_scope: "intervals",
    variant,
    variant_label: null,
    register_variant_id: "1",
    valid_from: from,
    valid_to: to,
    data_type: "int",
    data_length: null,
    delivery_column_name: column,
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: "7",
    value_set: null,
    is_identifier: false,
    classifications: [],
  };
}

/** The browse GET returns the register node, and the staged adds' `?period` GETs
 * resolve one binding each — the ONLY reads an Add makes, now that the list's own
 * `deliveries` carry the exact eras (Y-104). */
export function mockRegisterAndResolve(node: CatalogNode): void {
  vi.mocked(getCatalogNode).mockImplementation(async (fqid, params) => {
    if (!params?.period) {
      return node;
    }
    const variant = typeof params.variant === "string" ? params.variant : "";
    return {
      states: [columnState(variant, fqid.split("/").at(-1) ?? "")],
    } as unknown as StatesResponse;
  });
}

export async function renderRegister(fqidPath = "scb/lisa") {
  return await render(CatalogNodeView, {
    fqidPath,
    regMetaVersion: "test",
    steward: "global",
    windowMinYear: 1960,
    vintageYear: 2024,
  });
}

/** Tick a delivery column by the name the list shows it under. */
export async function tickColumn(name: string): Promise<void> {
  await page.getByRole("checkbox", { name, exact: true }).click();
}
