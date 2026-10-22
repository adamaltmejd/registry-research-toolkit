// Register-child delivery fixtures shared by the study-window and ProjectEditor
// browser tests.
import type { VariableChild, VariableDeliveryModel } from "./api";

/** One `(variant, column)` delivery on a register child (Y-82), over one era per
 * `[from, to]` pair — `to` null is a still-delivered window. Two or more eras is an
 * INTERRUPTED column (Y-104): the wire carries each era in `windows`, and `coverage`
 * is the span over them, which cannot express the gap. NO eras is the boundless
 * delivery `VariableDelivery` documents — an alias spelling on a variant with no
 * states of its own. */
export function delivery(
  variant: string,
  column: string,
  ...eras: [string, string | null][]
): VariableDeliveryModel {
  const last = eras.at(-1);
  return {
    variant,
    column,
    period_scope: "intervals",
    coverage: {
      coverage_from: eras[0]?.[0] ?? null,
      coverage_to: last?.[1] ?? null,
      open_ended: last?.[1] === null,
      state_count: eras.length,
    },
    windows: eras.map(([from, to]) => ({
      valid_from: from,
      valid_to: to ?? "9999-12-31",
    })),
  };
}

/** A register's variable child delivered as `deliveries`. */
export function variableChild(
  fqid: string,
  name: string,
  deliveries: VariableDeliveryModel[] = [],
): VariableChild {
  return { fqid, name, coverage: null, deliveries };
}
