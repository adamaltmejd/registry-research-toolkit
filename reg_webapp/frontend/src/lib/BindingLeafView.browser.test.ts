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
 * (no succession/related/lineage panels) so only the picker under test renders.
 * `over` lets a case add fields the #670 member-identity path reads (`fqid`,
 * `name`, `group`). */
function node(
  states: VariableStateModel[],
  over: Partial<BindingNodeData> = {},
): BindingNodeData {
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
    ...over,
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

/** A single visible state carrying STRUCTURAL fields (data_type + delivery column)
 * so StatesView's single-state detail renders its own "Technical details"
 * disclosure (#638 PR4) — distinct from the description's Sensitive/Identifier one.
 * The default `single` fixture has both fields null, so that path is never exercised
 * there. */
const singleWithStructural = [
  state({
    state_id: 1,
    variant: "individer",
    data_type: "char",
    data_length: "1",
    delivery_column_name: "Kon",
  }),
];

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

  it("demotes a single state's Data type / Delivery column into its own 'Technical details' disclosure (#638 PR4)", async () => {
    // The StatesView single-state detail moves the STRUCTURAL fields (data_type +
    // delivery column) behind their own collapsed disclosure, keeping Variant /
    // Valid / Value-set version prominent. The default `single` fixture has both
    // structural fields null (disclosure omitted), so this fixture supplies them.
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(singleWithStructural),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The "Technical details" summary is visible (two of them render — assert the
    // summary text exists, then disambiguate by content below). The demoted rows
    // are in the DOM but NOT visible while collapsed, so assert STRUCTURE.
    await expect
      .element(page.getByText("Technical details").first())
      .toBeVisible();

    // Disambiguation hazard (#638 PR4): TWO `details.tech-details` render here —
    // the description's (Sensitive/Identifier) AND the state detail's (Data type/
    // Delivery column). Select the state's by its content, not by index.
    const stateTech = [
      ...document.querySelectorAll<HTMLDetailsElement>("details.tech-details"),
    ].find((d) => d.textContent?.includes("Data type"));
    expect(stateTech).toBeDefined();
    // Collapsed by default, with both structural rows inside (content stays in the
    // DOM while collapsed).
    expect(stateTech?.open).toBe(false);
    expect(stateTech?.textContent).toContain("Data type");
    expect(stateTech?.textContent).toContain("Delivery column");
    // The user-facing state fields stay PROMINENT — in the state detail's own
    // meta block, NOT inside any tech-details disclosure. (`.state-detail` is the
    // single-state container; its prominent `dl.meta` is the one not nested in a
    // disclosure.)
    const stateDetail = stateTech?.closest(".state-detail");
    const promptMeta = [
      ...(stateDetail?.querySelectorAll("dl.meta") ?? []),
    ].find((dl) => !dl.closest("details.tech-details"));
    expect(promptMeta).toBeDefined();
    const promptText = promptMeta?.textContent ?? "";
    expect(promptText).toContain("Variant");
    expect(promptText).toContain("Valid");
    expect(promptText).toContain("Value-set version");
    // ...and those prominent labels are NOT inside the structural disclosure.
    expect(stateTech?.textContent).not.toContain("Variant");
    expect(stateTech?.textContent).not.toContain("Value-set version");
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

describe("BindingLeafView period-scoped value-set history (#744)", () => {
  it("uses the period subset for Add while rendering full history with outside-period collapse", async () => {
    const inA = state({
      state_id: 10,
      variant: "individer",
      value_set_id: 10,
      value_set_version_label: "In-period A",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const inB = state({
      state_id: 11,
      variant: "individer",
      value_set_id: 11,
      value_set_version_label: "In-period B",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const outside = state({
      state_id: 12,
      variant: "outside-population",
      value_set_id: 12,
      value_set_version_label: "Outside period",
      valid_from: "1990-01-01",
      valid_to: "1990-12-31",
    });
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: [inA, inB],
    } as never);
    router.navigate("/catalog/scb/lisa/kon?period=2007..2008");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([inA, inB, outside]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    await expect
      .element(page.getByText(/pick one to add/i))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeEnabled();
  });

  it("uses a period-only scope when a variant modifier is active", async () => {
    const inA = state({
      state_id: 20,
      variant: "individer",
      value_set_id: 20,
      value_set_version_label: "In-period A",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const inB = state({
      state_id: 21,
      variant: "individer",
      value_set_id: 21,
      value_set_version_label: "In-period B",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const samePeriodOtherVariant = state({
      state_id: 22,
      variant: "other-population",
      value_set_id: 22,
      value_set_version_label: "Same-period other variant",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const outside = state({
      state_id: 23,
      variant: "outside-population",
      value_set_id: 23,
      value_set_version_label: "Outside period",
      valid_from: "1990-01-01",
      valid_to: "1990-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(
      async (_fqid, params) =>
        ({
          states: params?.variant
            ? [inA, inB]
            : [inA, inB, samePeriodOtherVariant],
        }) as never,
    );
    router.navigate(
      "/catalog/scb/lisa/kon?period=2007..2008&variant=individer",
    );

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([inA, inB, samePeriodOtherVariant, outside]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".vs-label")].some(
        (el) => el.textContent === "Same-period other variant",
      ),
    ).toBe(true);
  });
});

describe("BindingLeafView member identity (#670)", () => {
  // A grouped member: its `node.group` references a register concept group, and
  // /dimensions returns that group with the member carrying distinguishing
  // facets. The qualifier reads the member's facet labels; the link targets the
  // group subject route built from `node.group`.
  const groupedFqid = "scb/lisa/agi1astsni2007g";
  const groupedNode = node(single, {
    fqid: groupedFqid,
    name: "Näringsgren, största förvärvskälla",
    group: { provider: "scb", register: "lisa", key: "naringsgren" },
  });
  const groupedDimensions = {
    binding: groupedFqid,
    dimensions: [
      {
        key: "naringsgren",
        label: "Näringsgren, största förvärvskälla",
        source: "edge",
        axes: ["source", "edition"],
        members: [
          {
            fqid: groupedFqid,
            name: "Näringsgren, största förvärvskälla",
            facets: [
              { axis: "source", value: "agi", label: "AGI" },
              { axis: "edition", value: "sni2007", label: "2007 SNI edition" },
            ],
          },
          {
            fqid: "scb/lisa/ku1astsni2002g",
            name: "Näringsgren, största förvärvskälla",
            facets: [
              { axis: "source", value: "ku", label: "KU" },
              { axis: "edition", value: "sni2002", label: "2002 SNI edition" },
            ],
          },
        ],
      },
    ],
  };

  it("renders the member qualifier and a 'member of ⟨label⟩' link with the correct href", async () => {
    vi.mocked(getBindingDimensions).mockResolvedValue(
      groupedDimensions as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The qualifier is THIS member's facet labels (canonical group's facets).
    await expect
      .element(page.getByText("AGI · 2007 SNI edition"))
      .toBeVisible();

    // The context link targets the group subject route from `node.group`.
    const link = page.getByRole("link", {
      name: "Näringsgren, största förvärvskälla",
    });
    await expect.element(link).toBeVisible();
    await expect
      .element(link)
      .toHaveAttribute("href", "/catalog/group/scb/lisa/naringsgren");
  });

  it("resolves the qualifier + link from the RESOLVED target when opened via a same_as alias (#670 Codex P2)", async () => {
    // The alias bug: a grouped leaf opened through a `same_as` alias keeps
    // `node.fqid` = the requested ALIAS, but /dimensions keys its members on the
    // RESOLVED target (the last `via_same_as` hop). Matching on `node.fqid` would
    // miss the faceted member and fall back to the alias slug; the fix matches on
    // the resolved fqid so the facet qualifier + group link resolve.
    const aliasFqid = "scb/lisa/inkjan-alias";
    const resolvedFqid = "scb/rams/inkjan";
    const aliasNode = node(single, {
      fqid: aliasFqid,
      name: "Näringsgren, största förvärvskälla",
      group: { provider: "scb", register: "lisa", key: "naringsgren" },
      via_same_as: [resolvedFqid],
    });
    const aliasDimensions = {
      binding: aliasFqid,
      dimensions: [
        {
          key: "naringsgren",
          label: "Näringsgren, största förvärvskälla",
          source: "edge",
          axes: ["source", "edition"],
          members: [
            {
              // Keyed on the RESOLVED target, NOT the requested alias.
              fqid: resolvedFqid,
              name: "Näringsgren, största förvärvskälla",
              facets: [
                { axis: "source", value: "agi", label: "AGI" },
                {
                  axis: "edition",
                  value: "sni2007",
                  label: "2007 SNI edition",
                },
              ],
            },
          ],
        },
      ],
    };
    vi.mocked(getBindingDimensions).mockResolvedValue(aliasDimensions as never);

    render(BindingLeafView, {
      fqidPath: aliasFqid,
      node: aliasNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The qualifier is the RESOLVED member's facet labels — NOT the alias slug.
    await expect
      .element(page.getByText("AGI · 2007 SNI edition"))
      .toBeVisible();
    expect(
      document.querySelector(".member-identity code.qualifier.slug"),
    ).toBeNull();

    // The "member of ⟨label⟩" link resolves (containment fallback now matches the
    // resolved member); its href comes from `node.group`.
    const link = page.getByRole("link", {
      name: "Näringsgren, största förvärvskälla",
    });
    await expect.element(link).toBeVisible();
    await expect
      .element(link)
      .toHaveAttribute("href", "/catalog/group/scb/lisa/naringsgren");
  });

  it("a grouped member with no facets renders the slug qualifier as a code identifier (#670 M10)", async () => {
    // M10's exact case: an edge group (`axes: []`) whose members carry NO facets,
    // so the slug — the only differentiator — is the fallback qualifier, rendered
    // as a technical identifier (mono `<code>`), not a human facet label.
    const edgeDimensions = {
      binding: groupedFqid,
      dimensions: [
        {
          key: "naringsgren",
          label: "Näringsgren, största förvärvskälla",
          source: "edge",
          axes: [],
          members: [
            { fqid: groupedFqid, name: "Näringsgren", facets: [] },
            { fqid: "scb/lisa/ku1astsni", name: "Näringsgren", facets: [] },
          ],
        },
      ],
    };
    vi.mocked(getBindingDimensions).mockResolvedValue(edgeDimensions as never);

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The slug qualifier is present in the IDENTITY ROW, rendered as <code>.
    // Scope to `.member-identity` — the same slug also appears in the
    // DimensionsPanel member list (a facetless edge group renders each member's
    // leaf slug), so a bare `getByText` is ambiguous now that the identity row
    // and the panel both render once /dimensions resolves (the flicker gate).
    const slug = page.getByText("agi1astsni2007g").first();
    await expect.element(slug).toBeVisible();
    const slugEl = document.querySelector(
      ".member-identity code.qualifier.slug",
    );
    expect(slugEl?.textContent).toBe("agi1astsni2007g");
    // ...and the "member of ⟨label⟩" link still renders alongside it.
    await expect
      .element(
        page.getByRole("link", { name: "Näringsgren, största förvärvskälla" }),
      )
      .toBeVisible();
  });

  it("renders no identity row while /dimensions is loading (no transient slug flicker)", async () => {
    // The flicker fix: the identity row gates on a RESOLVED fetch (`!dimLoading
    // && !dimError`), not just `!error`. While /dimensions is in flight,
    // `dimGroups` is [] — deriving the qualifier then would hit the slug fallback
    // (a grouped member has no facets yet) and the row would FLASH the slug, then
    // flip to the facet label once loaded. With the gate, the header (node.name)
    // renders but NO `.member-identity` row appears until the fetch resolves.
    vi.mocked(getBindingDimensions).mockReturnValue(new Promise(() => {}));

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The header renders immediately (the leaf is never blanked).
    await expect
      .element(
        page.getByRole("heading", {
          name: "Näringsgren, största förvärvskälla",
          level: 2,
        }),
      )
      .toBeVisible();
    // No identity row while loading — no transient slug, no "member of" link.
    expect(document.querySelector(".member-identity")).toBeNull();
    expect(
      document.querySelector(".member-identity code.qualifier.slug"),
    ).toBeNull();
  });

  it("an ungrouped variable renders neither qualifier nor group link", async () => {
    // The default beforeEach stubs `getBindingDimensions` → empty dimensions, and
    // the plain `node()` fixture has no `group`. So a normal variable page shows
    // no member-identity row at all (today's behavior).
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The page renders (header present), but no member-identity affordances.
    await expect
      .element(page.getByRole("heading", { name: "Kön", level: 2 }))
      .toBeVisible();
    expect(document.querySelector(".member-identity")).toBeNull();
    await expect.element(page.getByText(/member of/)).not.toBeInTheDocument();
  });

  it("degrades gracefully when the dimensions fetch errors (header survives, no qualifier/link)", async () => {
    // The dimensions fetch is an independent failure domain: an error must NOT
    // blank the leaf — the header (node.name) still renders, the qualifier/link
    // are simply omitted (additive). DimensionsPanel surfaces the inline error.
    vi.mocked(getBindingDimensions).mockRejectedValue(new Error("dims down"));

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The leaf is NOT blanked — the concept header (node.name) still renders.
    await expect
      .element(
        page.getByRole("heading", {
          name: "Näringsgren, största förvärvskälla",
          level: 2,
        }),
      )
      .toBeVisible();
    // No qualifier/link on error (both gate on a resolved fetch).
    expect(document.querySelector(".member-identity")).toBeNull();
    // The DimensionsPanel section still surfaces the error inline (shared resource).
    await expect
      .element(page.getByText(/Failed to load dimensions/))
      .toBeVisible();
  });
});
