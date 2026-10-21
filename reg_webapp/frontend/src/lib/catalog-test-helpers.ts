// Shared fixtures (axis + variable-state builders) for the split catalog unit tests:
// catalog.{browse,picker,picker-filters,resolve,value-sets}.test.ts.
import type { VariableStateModel } from "./api";
import { variableState } from "./api-test-helpers";

// #819: a group's axis is now `{name, label}`. Tests key on the stable name and
// don't assert the label here, so default the label to the name. Wraps the bare
// axis-name lists the helper tests build.
export function ax(...names: string[]): { name: string; label: string }[] {
  return names.map((name) => ({ name, label: name }));
}

// Minimal VariableStateModel — only the fields deriveType/distinctVersions read.
export function state(over: Partial<VariableStateModel>): VariableStateModel {
  return variableState(over);
}
