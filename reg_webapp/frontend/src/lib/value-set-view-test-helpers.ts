// Shared fixtures for the ValueSetView browser test files (ValueSetView.browser.test.ts,
// ValueSetView.{conformance,isolate,period}.browser.test.ts): the stubbed-read code
// registry, the `coding` / `state` builders and the two-value-set kommun fixture.
import type { ValueSetMemberModel, VariableStateModel } from "./api";

export const CODES = new Map<string, ValueSetMemberModel[]>();

/** Register a coding's members with the stubbed read and return the SUMMARY the
 * state carries in their place. `stateId` registers a state's stored
 * classification-mismatch list instead of the value set's own membership. */
export function coding(
  valueSetId: string | number,
  codes: ValueSetMemberModel[],
  opts: {
    stateId?: string | number;
    partition?: "canonical" | "source_extensions" | "nonstandard" | "sentinels";
    integerRange?: { min: number; max: number };
  } = {},
): VariableStateModel["value_set_summary"] {
  CODES.set(
    `${valueSetId}:${opts.stateId ?? ""}:${opts.partition ?? (opts.stateId != null ? "nonstandard" : "source_extensions")}`,
    codes,
  );
  return { code_count: codes.length, integer_range: opts.integerRange ?? null };
}

// Minimal VariableStateModel — only the fields ValueSetView reads.
export function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "2000-01-01",
    valid_to: "2000-12-31",
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

    period_token: null,
    ...over,
  };
}

export function normalizedText(selector: string): string {
  return (
    document
      .querySelector(selector)
      ?.textContent?.replace(/\s+/g, " ")
      .trim() ?? ""
  );
}

// A two-value-set fixture mirroring kommun: one classification value set (links
// out, no codes), one plain value set (expandable codes).
export const classState = state({
  state_id: "1",
  value_set_id: "100",
  classifications: [
    {
      slug: "lkf2007",
      short_name: "lkf2007",
      name: "lkf2007",
      conformance: null,
    },
  ],
  value_set_version_label: "LKF",
  variant: "doda",
  valid_from: "2007-01-01",
  valid_to: "2010-12-31",
});
export const plainState = state({
  state_id: "2",
  value_set_id: "200",
  classifications: [],
  value_set_version_label: "Kommun historisk",
  variant: "fodda",
  valid_from: "1961-01-01",
  valid_to: "1967-12-31",
  value_set_summary: coding(200, [
    { code: "0114", label: "Upplands Väsby" },
    { code: "0115", label: "Vallentuna" },
  ]),
});
