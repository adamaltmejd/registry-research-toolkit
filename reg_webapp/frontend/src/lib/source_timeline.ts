import type { RegisterShow, VariantModel } from "./api";
import { deliveredYears, variantLabel } from "./catalog";
import { coverageMarks } from "./period";
import type { DraftBinding, ProjectStudyWindow } from "./project_data";

// The /project source timeline's data (DESIGN.md → Time; #1838): what a source's
// succession family delivers, read from the register node every source card
// already shares, so the drawing issues no request of its own.

type Years = ProjectStudyWindow;

/** The family's delivered years, every variable's deliveries under any member
 * together: the coverage row, and the span the period thumbs may reach. */
export function familyYears(
  register: RegisterShow,
  familyVariants: readonly string[],
  vintageYear: number,
): number[] {
  return deliveredYears(
    register.children.flatMap((child) =>
      child.deliveries
        .filter((d) => familyVariants.includes(d.variant))
        .flatMap((d) =>
          d.windows.map((w) => ({ from: w.valid_from, to: w.valid_to })),
        ),
    ),
    vintageYear,
  );
}

/** Which member delivers which years, for the coverage row's tooltip: "16 år
 * och äldre until 2009, 15 år och äldre from 2010". The family's shared label
 * is dropped from each member's name, since the card heading already says it.
 * Null for a family of one: there is no boundary to name. */
export function memberNote(
  register: RegisterShow,
  familyVariants: readonly string[],
  vintageYear: number,
): string | null {
  if (familyVariants.length < 2) return null;
  const members = familyVariants
    .map((slug) => ({
      variant: register.variants.find((v) => v.slug === slug),
      years: familyYears(register, [slug], vintageYear),
    }))
    .filter(
      (m): m is { variant: VariantModel; years: number[] } =>
        m.variant !== undefined && m.years.length > 0,
    )
    .sort((a, b) => a.years[0] - b.years[0]);
  if (members.length < 2) return null;
  const short = (v: VariantModel) => {
    const label = variantLabel(v);
    const family = v.variant_family_label ?? "";
    const rest =
      family && label.startsWith(family)
        ? label.slice(family.length).replace(/^[,\s]+/, "")
        : "";
    return rest || label;
  };
  return members
    .map((m, i) => {
      const first = m.years[0];
      const last = m.years.at(-1) ?? first;
      if (i === 0) return `${short(m.variant)} until ${last}`;
      if (i === members.length - 1) return `${short(m.variant)} from ${first}`;
      return `${short(m.variant)} ${first}–${last}`;
    })
    .join(", ");
}

/** One delivery column of a bound variable, over the family: its name and the
 * runs of years it is delivered in. */
export interface ColumnSpan {
  column: string;
  from: number;
  to: number;
  runs: Years[];
}

/** A binding's columns as spans: its variable's deliveries under the family's
 * members, one span per delivery column, oldest first. A binding pinned to a
 * `representation` draws that column only. Empty when the register does not
 * list the variable or it has no dated delivery. */
export function bindingSpans(
  register: RegisterShow,
  binding: DraftBinding,
  familyVariants: readonly string[],
  vintageYear: number,
): ColumnSpan[] {
  const variable = typeof binding.variable === "string" ? binding.variable : "";
  const pinned =
    typeof binding.representation === "string" ? binding.representation : "";
  const child = register.children.find((c) => c.fqid === variable);
  if (!child) return [];
  const byColumn = new Map<string, { from: string; to: string }[]>();
  for (const d of child.deliveries) {
    if (!familyVariants.includes(d.variant)) continue;
    const column = d.column ?? child.name ?? variable;
    if (pinned && column !== pinned) continue;
    const windows = d.windows.map((w) => ({
      from: w.valid_from,
      to: w.valid_to,
    }));
    byColumn.set(column, [...(byColumn.get(column) ?? []), ...windows]);
  }
  const spans: ColumnSpan[] = [];
  for (const [column, windows] of byColumn) {
    const years = deliveredYears(windows, vintageYear);
    if (years.length === 0) continue;
    const from = years[0];
    const to = years.at(-1) ?? from;
    spans.push({
      column,
      from,
      to,
      runs: coverageMarks({ from, to }, years).delivered,
    });
  }
  return spans.sort((a, b) => a.from - b.from);
}
