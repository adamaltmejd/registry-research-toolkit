import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import {
  getDocsForVariable,
  getGraph,
  getLineage,
  getStates,
  getValues,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import {
  leaf,
  pickerStates,
  SEED,
  state,
} from "./binding-leaf-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Split from BindingLeafView.browser.test.ts by contract surface: inferred gap attribution.
// Siblings: BindingLeafView{,.graph,.rows,.technical-details,.provenance,.period}.browser.test.ts.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getWarnings: vi.fn().mockResolvedValue([]),
    getStates: vi.fn(),
    getGraph: vi.fn(),
    getLineage: vi.fn(),
    getDocsForVariable: vi.fn(),
    getValues: vi.fn(),
  };
});

beforeEach(() => {
  vi.mocked(getStates).mockReset();
  vi.mocked(getStates).mockImplementation(async (_fqid, params) => {
    const variant =
      typeof params?.variant === "string" ? params.variant : undefined;
    return pickerStates.filter(
      (s) => variant === undefined || s.variant === variant,
    );
  });
  // The graph fetch: an EMPTY graph by default (no nodes) → the picker uses the list
  // itself and the header derives no qualifier. Member-identity cases override it.
  vi.mocked(getGraph).mockReset();
  vi.mocked(getGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  } as never);
  vi.mocked(getLineage).mockReset();
  vi.mocked(getLineage).mockResolvedValue({
    edges: [],
    warnings: [],
    registers: [],
  });
  vi.mocked(getValues).mockReset();
  vi.mocked(getValues).mockResolvedValue({
    items: [],
    next_cursor: null,
    total: 0,
  });
  vi.mocked(getDocsForVariable).mockReset();
  vi.mocked(getDocsForVariable).mockResolvedValue({
    items: [],
    next_cursor: null,
    total: 0,
    register_ingested: true,
  });
  // No `?period` — the embedded states drive the plan.
  window.history.pushState({}, "", "/__reset__");
  router.navigate("/catalog/scb/lisa/kon");
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("BindingLeafView representation picker (#678)", () => {
  it("marks resolution-only gaps as inferred and unattributed", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf([
        state({
          state_id: "2",
          variant: "individer",
          variant_label: "Individuals",
          delivery_column_name: "AliasA",
          valid_from: "2021-07-01",
          valid_to: "2021-12-31",
          period_token: "HT2021",
          provenance: "inferred:resolution-gap",
        }),
      ]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure?.open).toBe(false);
    await expect
      .element(page.getByText("Inferred intervals"))
      .not.toBeVisible();

    await page.getByText("Technical details", { exact: true }).click();

    await expect.element(page.getByText("Inferred intervals")).toBeVisible();
    expect(disclosure?.textContent).toContain(
      "Interval 2021-07-01 – 2021-12-31",
    );
    expect(disclosure?.textContent).toContain(
      "Attribution Unattributed; retained by catalog resolution",
    );
    expect(disclosure?.textContent).not.toContain("Corrected deliveries");
    expect(disclosure?.textContent).not.toContain("Provider-documented");
  });
});
