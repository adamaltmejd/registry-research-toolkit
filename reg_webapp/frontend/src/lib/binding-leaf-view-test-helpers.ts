// Shared fixtures for the BindingLeafView.*.browser.test.ts split (state/node
// builders, picker states, SEED). Used by every BindingLeafView browser suite.
import { vi } from "vitest";
import {
  getDocsForVariable,
  getGraph,
  getLineage,
  getStates,
  getValues,
  type VariableShow,
  type VariableStateModel,
} from "./api";
import { variableState } from "./api-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

/** A minimal VariableStateModel — only the fields the add planner reads. */
export function state(over: Partial<VariableStateModel>): VariableStateModel {
  return variableState({
    valid_from: "1992-01-01",
    valid_to: "9999-12-31",
    ...over,
  });
}

/** A minimal `variable` show: metadata only (its states are a separate prop).
 * `over` lets a case add fields the #670 member-identity path reads (`fqid`,
 * `name`, `group`). */
export function node(over: Partial<VariableShow> = {}): VariableShow {
  return {
    kind: "variable",
    fqid: "scb/lisa/kon",
    name: "Kön",
    definition: null,
    description: null,
    measurement_unit: null,
    operational_definition: null,
    is_identifier: false,
    is_sensitive: false,
    deprecated: false,
    group: null,
    source_register_text: null,
    same_as: [],
    tags: [],
    ...over,
  };
}

/** The leaf's `node` + whole-history `states` props, as CatalogNodeView passes
 * them. */
export function leaf(
  states: VariableStateModel[],
  over: Partial<VariableShow> = {},
): { node: VariableShow; states: VariableStateModel[] } {
  return { node: node(over), states };
}

/** One variant, no delivery column → the picker enumerates zero rows. */
export const single = [state({ state_id: "1", variant: "individer" })];

/** Picker rows: two distinct (variant, delivery column) representations, each
 * with a finite window. `Kon` (individer, 2010–2015) and `Sni` (arbetsstallen,
 * 2018–2020) — two selectable rows over the full history. */
export const pickerStates = [
  state({
    state_id: "1",
    variant: "individer",
    delivery_column_name: "Kon",
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    value_set_version_label: "1-siffrig",
  }),
  state({
    state_id: "2",
    variant: "arbetsstallen",
    delivery_column_name: "Sni",
    valid_from: "2018-01-01",
    valid_to: "2020-12-31",
    value_set_version_label: "SNI 2007",
  }),
];

export const SEED = {
  regMetaVersion: "reg_meta/v1.0.0",
  steward: "global",
  windowMinYear: 1960,
} as const;

/** Each suite's `beforeEach`: the mocked reads answer empty (states from
 * `pickerStates` by variant), the route sits on `scb/lisa/kon` with no `?period`,
 * no project window, and a fresh project. The `vi.mock("./api")` factory that
 * makes these reads mocks stays in each file (vitest hoists it per file). */
export function resetLeaf(): void {
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
  });
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
}
