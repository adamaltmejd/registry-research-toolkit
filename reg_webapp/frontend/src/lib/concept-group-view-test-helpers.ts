// Shared fixtures, graph builders and renderGroup for the ConceptGroupView
// browser suites (ConceptGroupView{,.selection,.labels,.navigation,.filters,
// .succession}.browser.test.ts). Browser-only (renders via vitest-browser-svelte).

import { vi } from "vitest";
import { render } from "vitest-browser-svelte";
import type {
  ConceptGroupNodeData,
  GraphState,
  RelationshipGraph,
  StatesResponse,
  VariableGraphNode,
  VariableStateModel,
} from "./api";
import { getCatalogNode } from "./api";
import ConceptGroupView from "./ConceptGroupView.svelte";

export const SEED = { regMetaVersion: "1.0.0", steward: "global" } as const;

/** A minimal GraphState — only the fields `pickerRepresentations` reads. */
export function gstate(over: Partial<GraphState>): GraphState {
  return {
    state_id: "1",
    period_scope: "intervals",
    representation_run_id: 1,
    variant: "v",
    variant_label: null,
    delivery_column_name: null,
    value_set_version_label: "",
    value_set_id: null,
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    classification_slugs: [],
    ...over,
  };
}

/** A variable graph node carrying the given fqid + states — a variable's column
 * source. `definition`/`description` default null (the common parallel-column
 * sibling shape); pass them to seed the shared-concept-text dedup. */
export function vnode(
  fqid: string,
  states: GraphState[],
  meta: {
    definition?: string | null;
    description?: string | null;
    operationalDefinition?: string | null;
    label?: string;
  } = {},
): VariableGraphNode {
  return {
    kind: "variable",
    id: fqid,
    fqid,
    label: meta.label ?? fqid,
    group_key: null,
    group_label: null,
    facets: [],
    states,
    same_as: [],
    definition: meta.definition ?? null,
    description: meta.description ?? null,
    operational_definition: meta.operationalDefinition ?? null,
  };
}

export function graph(nodes: VariableGraphNode[]): RelationshipGraph {
  return { nodes, edges: [], focus_id: null };
}

export function node(
  overrides: Partial<ConceptGroupNodeData> = {},
): ConceptGroupNodeData {
  return {
    kind: "concept-group",
    provider: "scb",
    register: "rams",
    key: "ink",
    label: "Inkomst",
    source: "token",
    axes: [{ name: "month", label: "month" }],
    member: null,
    members: [
      {
        fqid: "scb/rams/inkjan",
        name: "Inkomst januari",
        facets: [{ axis: "month", value: "01", label: "januari" }],
        coverage: null,
      },
      {
        fqid: "scb/rams/inkfeb",
        name: "Inkomst februari",
        facets: [{ axis: "month", value: "02", label: "februari" }],
        coverage: null,
      },
    ],
    ...overrides,
  } as unknown as ConceptGroupNodeData;
}

export function vstate(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "individer",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    data_type: "int",
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classifications: [],
    ...over,
  };
}

export function statesResponse(states: VariableStateModel[]): StatesResponse {
  return { states } as unknown as StatesResponse;
}

export function mockResolveColumns(
  columnsByFqid: Record<string, readonly string[]>,
): void {
  vi.mocked(getCatalogNode).mockImplementation(async (fqid, params) => {
    const columns = columnsByFqid[fqid] ?? [];
    const variant =
      typeof params?.variant === "string" ? params.variant : "individer";
    return statesResponse(
      columns.map((column, index) =>
        vstate({
          state_id: String(index + 1),
          variant,
          delivery_column_name: column,
        }),
      ),
    );
  });
}

/** A two-member graph, ONE column each: inkjan delivers `Inkjan` (2010–2015),
 * inkfeb delivers `Inkfeb` (2018–2020). Each single-column member renders as ONE
 * compact row (no subheading). */
export function twoSingleColGraph(): RelationshipGraph {
  return graph([
    vnode("scb/rams/inkjan", [
      gstate({
        variant: "individer",
        delivery_column_name: "Inkjan",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
    ]),
    vnode("scb/rams/inkfeb", [
      gstate({
        variant: "individer",
        delivery_column_name: "Inkfeb",
        valid_from: "2018-01-01",
        valid_to: "2020-12-31",
      }),
    ]),
  ]);
}

export async function renderGroup(
  props: Partial<{
    provider: string;
    register: string;
    key: string;
    windowMinYear: number;
    windowMaxYear: number;
    vintageYear: number;
    enforcePeriodBounds: boolean;
  }> = {},
) {
  return await render(ConceptGroupView, {
    provider: "scb",
    register: "rams",
    key: "ink",
    regMetaVersion: SEED.regMetaVersion,
    steward: SEED.steward,
    windowMinYear: 1960,
    vintageYear: 2024,
    ...props,
  });
}
