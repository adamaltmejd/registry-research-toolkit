// Shared fixtures (axis + variable-state builders) for the split catalog unit tests:
// catalog.{browse,picker,picker-filters,value-sets}.test.ts.
import type { VariableStateModel } from "./api";

// #819: a group's axis is now `{name, label}`. Tests key on the stable name and
// don't assert the label here, so default the label to the name. Wraps the bare
// axis-name lists the helper tests build.
export function ax(...names: string[]): { name: string; label: string }[] {
  return names.map((name) => ({ name, label: name }));
}

// Minimal VariableStateModel — only the fields deriveType/distinctVersions read.
export function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "",
    valid_to: "",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    value_set_summary: null,
    is_identifier: false,
    classifications: [],

    ...over,
  };
}
