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

/** A DIFFERENT leaf that SHARES one of node A's variant slugs (`individer`) — for
 * the leaf-identity invalidation test. Because `individer` is still a member of
 * this plan's options, the MEMBERSHIP gate alone would let a stale pick gate-pass;
 * only the `fqidPath`-tracked reset re-gates Add. `node()` carries its own
 * `fqid`, so the leaf change is expressed via the `fqidPath` prop. */
const coexistingSharingVariant = [
  state({ state_id: 5, variant: "individer" }),
  state({ state_id: 6, variant: "regioner" }),
];

/** Two co-existing variants where the PICKED variant (`individer`) ALSO needs a
 * representation choice: within it two columns share the same window but carry
 * GENUINELY DIFFERENT codings (distinct `value_set_version_label`), so
 * `representationsCollapse` is false → `needsRepChoice`. Picking `individer` then
 * Add opens the rep chooser (the population→rep two-step that exposes the
 * divergence bug). `arbetsstallen` is a plain single-column variant. */
const coexistingWithRepChoice = [
  state({
    state_id: 1,
    variant: "individer",
    delivery_column_name: "Kon",
    value_set_version_label: "1-siffrig",
    value_set: [{ code: "1", label: "Man" }],
  }),
  state({
    state_id: 2,
    variant: "individer",
    delivery_column_name: "KonDetalj",
    value_set_version_label: "2-siffrig",
    value_set: [{ code: "01", label: "Man" }],
  }),
  state({ state_id: 3, variant: "arbetsstallen", delivery_column_name: "Sni" }),
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

  it("a leaf-identity change re-gates Add even when the new plan shares the picked variant", async () => {
    // The robustness fix: the reset `$effect` tracks `fqidPath` (the leaf
    // identity), so navigating to a DIFFERENT leaf re-invalidates the add state
    // INDEPENDENT of the parent's `{#key route.fqidPath}` remount. The membership
    // gate alone is NOT enough here: node B shares the picked variant (`individer`),
    // so the stale pick is still a MEMBER of B's options and would gate-pass —
    // only the leaf-identity reset clears `addVariant` and re-disables Add.
    const { rerender } = render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexisting),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const add = page.getByRole("button", { name: "Add to project" });

    // Pick `individer` on leaf A → Add ungated.
    await page.getByRole("button", { name: /individer/ }).click();
    await expect.element(add).toBeEnabled();

    // Navigate to a DIFFERENT leaf (new `fqidPath`) whose plan STILL offers
    // `individer` — so the stale pick remains a member of the options.
    await rerender({
      fqidPath: "scb/lisa/sysstatus",
      node: node(coexistingSharingVariant),
    });

    // Add is RE-GATED — the leaf-identity reset cleared the pick despite it being
    // a valid member of the new plan's options.
    await expect.element(add).toBeDisabled();
    // Re-picking a current option ungates Add again.
    await page.getByRole("button", { name: /individer/ }).click();
    await expect.element(add).toBeEnabled();
  });

  it("switching population invalidates an in-flight rep prompt (no stale-variant commit)", async () => {
    // The divergence the fix closes: pick variant A → Add (opens A's rep chooser)
    // → WITHOUT choosing, pick variant B. The rep chooser must DISAPPEAR
    // (`addPrompt` cleared) — otherwise choosing a rep would commit A's segments
    // while the UI shows B selected (wrong variant committed).
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexistingWithRepChoice),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // Pick population A (`individer`) → its segment needs a rep choice.
    await page.getByRole("button", { name: /individer/ }).click();
    await page.getByRole("button", { name: "Add to project" }).click();

    // The representation chooser is open for A.
    const repChooser = page.getByRole("group", {
      name: "Pick a representation",
    });
    await expect.element(repChooser).toBeVisible();

    // Switch population to B WITHOUT choosing a rep → the in-flight rep prompt
    // (holding A's segments) must be discarded.
    await page.getByRole("button", { name: /arbetsstallen/ }).click();
    await expect.element(repChooser).not.toBeInTheDocument();
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

  it("demotes Sensitive / Identifier into a 'Technical details' disclosure (#638 PR4)", async () => {
    // The structural flags are backend metadata — kept available but behind the
    // collapsed disclosure rather than in the prominent definition meta.
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The disclosure renders with a visible "Technical details" summary, collapsed
    // by default. The demoted rows are in the DOM but NOT visible while collapsed,
    // so assert STRUCTURE (inside the disclosure), not visibility.
    await expect.element(page.getByText("Technical details")).toBeVisible();
    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure).not.toBeNull();
    expect(disclosure?.open).toBe(false);
    // Sensitive + Identifier live INSIDE the disclosure, not the prominent meta.
    expect(disclosure?.textContent).toContain("Sensitive");
    expect(disclosure?.textContent).toContain("Identifier");
    // ...and are NOT in any other (prominent) meta block on the page.
    const promptMeta = [...document.querySelectorAll("dl.meta")].filter(
      (dl) => !dl.closest("details.tech-details"),
    );
    for (const dl of promptMeta) {
      expect(dl.textContent).not.toContain("Sensitive");
      expect(dl.textContent).not.toContain("Identifier");
    }
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
