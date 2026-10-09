// Register-node fixtures and Y-83 add-flow helpers shared by the split
// CatalogNodeView.{register,add-columns,add-eras}.browser.test.ts files.
// Browser-only: imports `vitest/browser`.
import { expect, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  RegisterShow,
  VariableChild,
  VariableDeliveryModel,
  VariableStateModel,
  VariantModel,
} from "./api";
import { getShow, getStates } from "./api";
import CatalogNodeView from "./CatalogNodeView.svelte";
import { variant } from "./variants-test-helpers";

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
): VariableDeliveryModel {
  const last = eras.at(-1);
  return {
    variant,
    column,
    period_scope: "intervals",
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

/** A register's variable child delivered as `deliveries`. */
export function variableChild(
  fqid: string,
  name: string,
  deliveries: VariableDeliveryModel[] = [],
): VariableChild {
  return { fqid, name, coverage: null, deliveries };
}

/** A register `show` node (`scb/lisa`, "LISA"); `fields` overrides any part of it.
 * Its one variant is `_default` unless `fields.variants` names others: the register
 * page reads its variants off the node itself. */
export function registerShow(
  fields: Partial<Omit<RegisterShow, "kind">>,
): RegisterShow {
  return {
    kind: "register",
    fqid: "scb/lisa",
    name: "LISA",
    purpose: null,
    tags: [],
    groups: [],
    variants: [variant("_default")],
    children: [],
    ...fields,
  };
}

// A register node whose children carry Y-82 `deliveries` — the LISA shape the
// ticket names: `forvink-ers` is delivered as `ForvErs` (a name its slug does not
// contain), `forversnetto` as `ForvErsNetto`, `disp` under TWO columns across a
// rename, and `arbetsstalle` under a SECOND variant. `variants` selects how many
// variants the register is delivered by: one (no chips) or both (chips).
export function columnedRegisterNode(variants: 1 | 2): RegisterShow {
  const children = [
    variableChild("scb/lisa/kon", "Kön", [
      delivery("individer-15plus", "Kon", ["2018-01-01", null]),
    ]),
    variableChild("scb/lisa/forvink-ers", "Förvärvsinkomst", [
      delivery("individer-15plus", "ForvErs", ["1990-01-01", "2021-12-31"]),
    ]),
    variableChild("scb/lisa/forversnetto", "Förvärvsinkomst netto", [
      delivery("individer-15plus", "ForvErsNetto", ["2011-01-01", null]),
    ]),
    variableChild("scb/lisa/disp", "Disponibel inkomst", [
      delivery("individer-15plus", "CDISP", ["1968-01-01", "2019-12-31"]),
      delivery("individer-15plus", "CDISP5", ["2020-01-01", null]),
    ]),
  ];
  if (variants === 2) {
    children.push(
      variableChild("scb/lisa/arbetsstalle", "Arbetsställe", [
        delivery("arbetsstallen", "ArbstNr", ["2005-01-01", null]),
      ]),
    );
  }
  return registerShow({ children });
}

/** The variants the Y-82 fixtures are delivered by, NAMED — the chips read as
 * these words, not as the slugs that key them. */
export function lisaVariants(): VariantModel[] {
  return [
    variant("individer-15plus", { name: "Individer, 15 år och äldre" }),
    variant("individer-16plus", { name: "Individer, 16 år och äldre" }),
    variant("arbetsstallen", { name: "Arbetsställen" }),
  ];
}

/** `node` carrying the named LISA variants (`lisaVariants`). */
export function withLisaVariants(node: RegisterShow): RegisterShow {
  return { ...node, variants: lisaVariants() };
}

// One variable delivered under a DIFFERENT column by each variant — the ticket's
// `disp` shape (`CDISP` then `CDISP5`), split so that no single variant ships
// both. `kon` is delivered by both variants under one name, so the register has
// two chips whichever way the lens goes.
export function splitColumnRegisterNode(): RegisterShow {
  return registerShow({
    children: [
      variableChild("scb/lisa/kon", "Kön", [
        delivery("individer-15plus", "Kon", ["1990-01-01", null]),
        delivery("individer-16plus", "Kon", ["1990-01-01", null]),
      ]),
      variableChild("scb/lisa/disp", "Disponibel inkomst", [
        delivery("individer-16plus", "CDISP", ["1968-01-01", "2019-12-31"]),
        delivery("individer-15plus", "CDISP5", ["2020-01-01", null]),
      ]),
    ],
  });
}

/** Toggle a variant chip by the NAME it reads as — clicking the chip label, as a
 * pointer does: the checkbox it wraps is visually hidden (present for the keyboard
 * and assistive tech). Waits for the chip first. EXACT: the list's own
 * delivery-column ticks (Y-83) are checkboxes too, and a chip's slug is a prefix of
 * a column name often enough (`foretag` / `ForetagNr`) that a substring match is
 * ambiguous. */
export async function clickVariantChip(name: string): Promise<void> {
  const checkbox = page.getByRole("checkbox", { name, exact: true });
  await expect.element(checkbox).toBeInTheDocument();
  checkbox.element().closest<HTMLLabelElement>("label")?.click();
}

// ── Y-83: adding delivery columns straight from the register list ────────────
// The register page is an authoring surface now: each delivery column carries a
// tick, and one action adds every ticked column to the project through the same
// staged add → resolve → commit stack the variable pages use.

/** One state of a variable, as a `period` states read returns it, so the
 * committed binding carries a real type. */
export function columnState(
  variant: string,
  column: string,
  { id = 1, from = "1990-01-01", to = "9999-12-31" } = {},
): VariableStateModel {
  return {
    warning_ids: [],
    state_id: String(id),
    period_scope: "intervals",
    period_token: null,
    variant,
    variant_label: null,
    variant_family: null,
    variant_family_label: null,
    register_variant_id: "1",
    valid_from: from,
    valid_to: to,
    coding_window_from: null,
    name: null,
    definition: null,
    description: null,
    operational_definition: null,
    measurement_unit: null,
    data_type: "int",
    data_length: null,
    delivery_column_name: column,
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: "7",
    value_set: null,
    value_set_summary: null,
    is_identifier: false,
    classifications: [],
  };
}

/** `show` returns the register node, and each staged add's `period` states read
 * resolves one binding — the ONLY reads an Add makes, now that the list's own
 * `deliveries` carry the exact eras (Y-104). */
export function mockRegisterAndResolve(node: RegisterShow): void {
  vi.mocked(getShow).mockResolvedValue(node);
  vi.mocked(getStates).mockImplementation(async (fqid, params) => [
    columnState(
      typeof params?.variant === "string" ? params.variant : "",
      fqid.split("/").at(-1) ?? "",
    ),
  ]);
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
