import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { BindingNodeData, VariableStateModel } from "./api";
import {
  getBindingDimensions,
  getBindingLineageWarnings,
  getCatalogNode,
  getDocsForVariable,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";

// #638 PR2b: the variant (population) choice moved from a POST-click modal into
// the picker, where it GATES "Add to project". This guards the wiring: a node
// whose visible states are ≥2 co-existing variants renders the proactive selector
// AND keeps Add disabled until a population is picked; a single-variant node shows
// no selector and Add is enabled (seed permitting). The plan classification itself
// (`buildAddPlan` → choose-variant / segments) is exhaustively unit-tested in
// catalog.test.ts — this is the BindingLeafView render/gate layer.
//
// Each case renders with NO `?period` (so `states` = the embedded `node.states`,
// no resolve fetch): a NO-period node with ≥2 variants is `choose-variant` by
// design (ambiguous without a time bound), which is exactly the gate path. The
// four catalog GETs the leaf + its sibling panels (dimensions / lineage warnings /
// docs) drive are stubbed so nothing hits a real fetch; the panels are independent
// failure domains, so an empty/rejecting stub never blanks the picker under test.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
    getBindingDimensions: vi.fn(),
    getBindingLineageWarnings: vi.fn(),
    getDocsForVariable: vi.fn(),
  };
});

/** A minimal VariableStateModel — only the fields the add planner reads. */
function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v",
    register_variant_id: 1,
    valid_from: "1992-01-01",
    valid_to: "9999-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    ...over,
  };
}

/** A minimal BindingNode leaf carrying `states`; the embedded edge arms are empty
 * (no succession/related/lineage panels) so only the picker under test renders. */
function node(states: VariableStateModel[]): BindingNodeData {
  return {
    kind: "binding",
    fqid: "scb/lisa/kon",
    name: "Kön",
    definition: null,
    description: null,
    measurement_unit: null,
    is_identifier: false,
    is_sensitive: false,
    register_id: 1,
    variable_id: 1,
    source_register_id: null,
    source_register_text: null,
    states,
    same_as: [],
    related_to: [],
    lineage: [],
    succession_chain: [],
    via_same_as: null,
  } as unknown as BindingNodeData;
}

/** Two register variants co-existing over the same window — `choose-variant` when
 * no period bounds them (the gate path). */
const coexisting = [
  state({ state_id: 1, variant: "individer" }),
  state({ state_id: 2, variant: "arbetsstallen" }),
];

/** One variant → `segments` (no population choice). */
const single = [state({ state_id: 1, variant: "individer" })];

/** A DIFFERENT pair of co-existing variants — node B for the membership-gate
 * test. Its variants do NOT overlap node A's (`individer`/`arbetsstallen`), so a
 * pick made against A is a non-member of B's options. */
const coexistingB = [
  state({ state_id: 3, variant: "foretag" }),
  state({ state_id: 4, variant: "regioner" }),
];

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  // The sibling panels' fetches: resolve empty so they render nothing and never
  // hit a real network (they're independent failure domains regardless).
  vi.mocked(getBindingDimensions).mockReset();
  vi.mocked(getBindingDimensions).mockResolvedValue({
    binding: "scb/lisa/kon",
    dimensions: [],
  } as never);
  vi.mocked(getBindingLineageWarnings).mockReset();
  vi.mocked(getBindingLineageWarnings).mockResolvedValue({
    binding: "scb/lisa/kon",
    lineage_warnings: [],
  } as never);
  vi.mocked(getDocsForVariable).mockReset();
  vi.mocked(getDocsForVariable).mockResolvedValue({
    results: [],
    total_count: 0,
  } as never);
  // No `?period` — the embedded states drive the plan. Reset to a sentinel first
  // so `navigate` isn't a no-op against a prior test's URL.
  window.history.pushState({}, "", "/__reset__");
  router.navigate("/catalog/scb/lisa/kon");
  // A fresh draft so the seed is present (Add isn't seed-gated under test).
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

const SEED = { regMetaVersion: "reg_meta/v1.0.0", steward: "global" } as const;

describe("BindingLeafView add gate (#638 PR2b)", () => {
  it("≥2 co-existing variants render the population selector and gate Add until picked", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexisting),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The proactive selector is present (the prompt line + a button per variant).
    await expect.element(page.getByText(/pick one to add/i)).toBeVisible();
    const individer = page.getByRole("button", { name: /individer/ });
    const arbetsstallen = page.getByRole("button", { name: /arbetsstallen/ });
    await expect.element(individer).toBeVisible();
    await expect.element(arbetsstallen).toBeVisible();

    // Add is GATED — no population picked yet.
    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeDisabled();

    // Picking a population marks it pressed and ungates Add.
    await individer.click();
    await expect.element(individer).toHaveAttribute("aria-pressed", "true");
    await expect.element(add).toBeEnabled();
  });

  it("a stale pick is non-member of the new plan's options → Add re-gated until a current option is picked", async () => {
    // The gate is MEMBERSHIP-based: Add is enabled only when `addVariant` is one
    // of the CURRENT plan's options. This guards the scenario the membership gate
    // exists for — the plan's options change UNDER the same component instance
    // (here via `rerender`), leaving `addVariant` holding a now-absent variant. A
    // value-based gate (`addVariant !== null`) would gate-PASS that stale pick;
    // the membership gate must re-disable Add.
    const { rerender } = render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexisting),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const add = page.getByRole("button", { name: "Add to project" });

    // Pick one of node A's variants → Add ungated.
    await page.getByRole("button", { name: /individer/ }).click();
    await expect.element(add).toBeEnabled();

    // The plan's options change under the same instance: node B has DIFFERENT
    // co-existing variants (`foretag`/`regioner`) that do NOT include the picked
    // `individer`. The stale pick is now a non-member of B's options.
    await rerender({ node: node(coexistingB) });

    // Add is RE-GATED — the stale pick can't gate-pass against B's options.
    await expect.element(add).toBeDisabled();
    // The selector now offers B's populations.
    const foretag = page.getByRole("button", { name: /foretag/ });
    await expect.element(foretag).toBeVisible();

    // Picking one of B's CURRENT options ungates Add again.
    await foretag.click();
    await expect.element(add).toBeEnabled();
  });

  it("a single-variant node shows no selector and Add is enabled", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // No `choose-variant` selector — Add is the only actionable control.
    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeEnabled();
    expect(
      document.body.querySelector('[aria-label="Pick a register variant"]'),
    ).toBeNull();
  });

  it("Add stays seed-gated (disabled) until the deployment seed is present", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      // Empty seed (pre-/api/context) → Add disabled regardless of the plan.
      regMetaVersion: "",
      steward: "",
      vintageYear: 2024,
    });
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeDisabled();
  });
});
