// Shared fixtures for the BindingLeafView.*.browser.test.ts split (state/node
// builders, picker states, SEED). Used by every BindingLeafView browser suite.
import type {
  BindingNodeData,
  StatesResponse,
  VariableStateModel,
} from "./api";

/** A minimal VariableStateModel — only the fields the add planner reads. */
export function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "1992-01-01",
    valid_to: "9999-12-31",
    data_type: null,
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

/** A minimal BindingNode leaf carrying `states`; the embedded edge arms are empty
 * so only the picker under test renders. `over` lets a case add fields the #670
 * member-identity path reads (`fqid`, `name`, `group`). */
export function node(
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
    register_id: "1",
    variable_id: "1",
    source_register_id: null,
    source_register_text: null,
    states,
    same_as: [],
    lineage: [],
    succession_chain: [],
    via_same_as: null,
    ...over,
  } as unknown as BindingNodeData;
}

export function statesResponse(states: VariableStateModel[]): StatesResponse {
  return { states } as unknown as StatesResponse;
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
