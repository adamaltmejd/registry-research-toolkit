/**
 * The "apply overlap to all" plan for the project's common study window (pure, no
 * runes). See reg_webapp/DESIGN.md → "Common study window": a window edit never
 * rewrites a source period, and this explicit action is the one rewrite there is —
 * every dated source takes the years its columns are delivered in inside the
 * window, the same full available intersection a catalog add defaults to
 * (`windowsAddPeriod`, every disjoint era kept). A source with no such years is
 * left exactly as it is.
 *
 * Availability is the register read the register list already makes
 * (`VariableDelivery.windows` per variable, variant and delivery column), so the
 * plan judges a source by the same eras the picker offered its columns under.
 */

import type { BindingChild } from "./api";
import {
  addWindowBounds,
  deliveryWindows,
  fqidSegments,
  registerPrefixOf,
  windowsAddPeriod,
} from "./catalog";
import { periodFromWire, periodToWire, periodWindowRelation } from "./period";
import {
  type Period,
  type SafeSource,
  type StudyWindow,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
} from "./project_data";

/** One source the plan rewrites: its identity (the store's period-change key) and
 * the period it moves from and to. */
export interface OverlapChange {
  sourceName: string;
  registerVariant: string;
  from: Period;
  to: Period;
}

/** One dated source the plan leaves alone — it keeps its period (and, when that
 * period is disjoint, keeps blocking the order). `no-overlap`: its columns are
 * delivered in no year of the window. `unknown-column`: the register read lists no
 * delivery for one of its columns at its variant, so its overlap cannot be worked
 * out, and narrowing to the columns that WERE found would silently drop the years
 * of the one that was not. */
export interface OverlapMiss {
  sourceName: string;
  period: Period;
  reason: "no-overlap" | "unknown-column";
}

export interface OverlapPlan {
  changes: OverlapChange[];
  misses: OverlapMiss[];
}

/** The registers whose availability the plan needs: one per DATED source (a
 * year-independent or unset period relates to no window, so it is never read). */
export function overlapRegisters(
  sources: readonly SafeSource[],
  window: StudyWindow,
): string[] {
  const registers = new Set<string>();
  for (const source of sources) {
    const rv = safeSourceRegisterVariant(source);
    if (
      fqidSegments(rv).length === 3 &&
      periodWindowRelation(safeSourcePeriod(source), window) !== null
    ) {
      registers.add(registerPrefixOf(rv));
    }
  }
  return [...registers].sort();
}

/** Plan the rewrite. `childrenByRegister` holds each `overlapRegisters` register's
 * binding children (the register read). A source is CHANGED when its columns'
 * delivery windows at its own variant reach the study window and the overlap
 * differs from its period; MISSED when they reach none of it, or when any one of
 * its columns has no delivery at that variant in the read. A binding pinned to a
 * delivery column (`representation`) contributes that column's eras only. */
export function planWindowOverlap(
  sources: readonly SafeSource[],
  window: StudyWindow,
  childrenByRegister: ReadonlyMap<string, readonly BindingChild[]>,
): OverlapPlan {
  const bounds = addWindowBounds(null, [window.from, window.to]);
  const plan: OverlapPlan = { changes: [], misses: [] };
  for (const source of sources) {
    const registerVariant = safeSourceRegisterVariant(source);
    const variant = fqidSegments(registerVariant)[2];
    const period = safeSourcePeriod(source);
    if (
      variant === undefined ||
      period === null ||
      periodWindowRelation(period, window) === null
    ) {
      continue;
    }
    const children =
      childrenByRegister.get(registerPrefixOf(registerVariant)) ?? [];
    const eras: { from: string; to: string }[] = [];
    let unknown = false;
    for (const binding of safeSourceBindings(source)) {
      const before = eras.length;
      const child = children.find((c) => c.fqid === binding.variable);
      const pinned =
        typeof binding.representation === "string"
          ? binding.representation
          : null;
      for (const delivery of child?.deliveries ?? []) {
        if (
          delivery.variant === variant &&
          delivery.period_scope !== "year_independent" &&
          (pinned === null || delivery.column === pinned)
        ) {
          for (const w of delivery.windows) {
            eras.push({ from: w.valid_from, to: w.valid_to });
          }
        }
      }
      unknown ||= eras.length === before;
    }
    const sourceName = safeSourceName(source);
    if (unknown || eras.length === 0) {
      plan.misses.push({ sourceName, period, reason: "unknown-column" });
      continue;
    }
    const wire = windowsAddPeriod(deliveryWindows(eras), bounds);
    if (wire === null) {
      plan.misses.push({ sourceName, period, reason: "no-overlap" });
      continue;
    }
    const next = periodFromWire(wire);
    if (periodToWire(next) !== periodToWire(period)) {
      plan.changes.push({
        sourceName,
        registerVariant,
        from: period,
        to: next,
      });
    }
  }
  return plan;
}
