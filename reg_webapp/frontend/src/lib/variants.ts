/**
 * Variant folding for a register's two variant surfaces (Y-79): the compact
 * summary on the register page (`VariantsSummary`) and the
 * `/catalog/<provider>/<register>/variants` route (`VariantBrowser`).
 *
 * SCB delivers one `register_version` row per DELIVERY, so LISA's seven variants
 * carry 105 of them and each row repeats the previous one's description,
 * measurement information, population and object type with only the year moved
 * on. Folding is DISPLAY-only: consecutive versions whose text is identical once
 * their years are normalized collapse into ONE block carrying the run's year
 * range, and a genuinely changed text — or a delivery there is nothing to
 * compare — opens the next block. Nothing here reads or rewrites
 * `register_version` content, and the delivered order is the block order.
 *
 * Split in two so the register page pays only for what it shows: `groupVariants`
 * walks the variant rows and their version NAMES (cheap — it's what the summary
 * table needs), while `foldVersions` walks a variant's version BODIES and is
 * called only by the route page.
 */
import type { VariantsResponse } from "./api";
import { labelSuffix, variantLabel } from "./catalog";

/** One `register_variant` sub-resource row (reg_meta's `VariantSummary`). */
export type Variant = VariantsResponse["variants"][number];
/** One `register_version` row nested under a variant (#799). */
export type Version = Variant["versions"][number];

/** A 1900–2099 year not embedded in a longer digit run — reg_meta's
 * `extract_year` regex (`queries.py`), mirrored so the SPA folds on the same
 * notion of a year the DB build derives its delivery windows from. (Distinct
 * from `period.ts`'s year fragment, which anchors a whole wire token; this one
 * reads a year OUT of delivered prose.) */
const YEAR = /(?<!\d)(?:19|20)\d{2}(?!\d)/g;

/** What a year is replaced by before two versions are compared. A NUL can never
 * appear in `JSON.stringify` output (it escapes to a six-character sequence),
 * so it can't collide with delivered text; and masking rather than deleting
 * keeps "LISA 2019" distinct from "LISA". */
const YEAR_MASK = "\0";

/** One concrete variant inside a browse entry. For a curated succession family
 * (#376) the entry has several, oldest delivery first; every other entry has
 * exactly one. */
export interface VariantSegment {
  variant: Variant;
  /** What the entry's label does NOT already say ("16 år och äldre"), so a
   * family reads as one variant with two frames; the full name otherwise. */
  name: string;
  /** The years this segment was delivered ("1990–2009"), or "" when its
   * versions name none. */
  span: string;
}

/** A register's variants folded into browse entries: a curated succession family
 * is ONE entry holding its concrete variants as segments; every other variant is
 * its own single-segment entry. */
export interface VariantGroup {
  /** Stable `#each` key — the family key, else the lone variant's slug. */
  key: string;
  /** The family's label, else the variant's own display label. */
  label: string;
  /** The entry's segments, oldest delivery first. */
  segments: VariantSegment[];
  /** The entry's delivery span across every segment ("1990–2023"). */
  span: string;
  isFamily: boolean;
}

/** A run of consecutive `register_version` rows that say the same thing. */
export interface VersionBlock {
  /** The run's year range ("1990–2023"), its single year, or — when SCB named
   * no year — the first version's name. */
  label: string;
  /** How many `register_version` rows the block folds. */
  count: number;
  /** The run's first version; every member repeats its text. */
  version: Version;
}

/** `display_group` duplicates `name` for most variants (SCB delivers them
 * identical), so a surface shows it only when it ADDS information. Compare
 * against what the surface ALREADY rendered, not against `name`: a variant with
 * no name is headed by its `display_group` (`variantLabel`), which a `name`
 * comparison would then print a second time. Compare trimmed: some source rows
 * carry trailing-whitespace noise on one side ("…AGI/KU " vs "…AGI/KU") that a
 * strict !== would treat as a difference. */
export function showsDistinctGroup(
  shown: string | null | undefined,
  group: string | null | undefined,
): boolean {
  return !!group && group.trim() !== (shown ?? "").trim();
}

/** Group a register's variants into browse entries, family members together.
 * Each variant's delivery years are read ONCE here and carried on the segment,
 * so neither the sort nor the two spans re-scan the version names. */
export function groupVariants(variants: readonly Variant[]): VariantGroup[] {
  interface Member {
    variant: Variant;
    years: number[];
  }
  interface Entry {
    key: string;
    label: string;
    isFamily: boolean;
    members: Member[];
  }
  const grouped = new Map<string, Entry>();
  for (const variant of variants) {
    // Every member of a curated succession component carries the same
    // `variant_family` (the component's head slug), so the first member seen
    // settles both the key and whether the entry is a family.
    const key = variant.variant_family ?? variant.slug;
    const member = { variant, years: variantYears(variant) };
    const existing = grouped.get(key);
    if (existing) {
      existing.members.push(member);
      continue;
    }
    grouped.set(key, {
      key,
      label: variant.variant_family_label ?? variantLabel(variant),
      isFamily: variant.variant_family != null,
      members: [member],
    });
  }
  return [...grouped.values()].map((entry) => {
    // The catalog orders variants by SLUG, which reads a family back-to-front
    // (the 2010–2023 frame above the 1990–2009 one it replaced). Order the
    // segments by the year they were first delivered instead; a variant whose
    // versions name no year keeps its slug order, last.
    entry.members.sort((a, b) => firstYear(a.years) - firstYear(b.years));
    return {
      key: entry.key,
      label: entry.label,
      isFamily: entry.isFamily,
      span: yearRange(entry.members.flatMap((member) => member.years)),
      segments: entry.members.map((member) => ({
        variant: member.variant,
        name: segmentName(entry.label, member.variant),
        span: yearRange(member.years),
      })),
    };
  });
}

/** Fold a variant's versions into blocks: consecutive versions whose text is
 * identical after year normalization become one block spanning their years, and
 * a version with no text to compare stands alone. */
export function foldVersions(versions: readonly Version[]): VersionBlock[] {
  /** A run under construction: the fold key it matches — `null` for a delivery
   * that can fold with nothing — plus the years and count its members
   * contribute to the block. */
  interface VersionRun {
    key: string | null;
    version: Version;
    years: number[];
    count: number;
  }
  const runs: VersionRun[] = [];
  for (const version of versions) {
    // A delivery carrying no text says nothing two neighbours could agree on,
    // so it folds with nothing AND terminates the run it interrupts: folding
    // 2018 and 2020 across a text-free 2019 would assert that 2019 repeated a
    // wording it never carried.
    const key = hasText(version) ? foldKey(version) : null;
    const year = versionYear(version);
    const open = runs.at(-1);
    if (key !== null && open?.key === key) {
      open.count += 1;
      if (year !== null) {
        open.years.push(year);
      }
      continue;
    }
    runs.push({ key, version, years: year === null ? [] : [year], count: 1 });
  }
  return runs.map((run, index) => ({
    label: yearRange(run.years) || (run.version.name ?? `#${index + 1}`),
    count: run.count,
    version: run.version,
  }));
}

/** Whether a version says anything a reader could compare. A delivery carrying
 * only a name is still a delivery — someone looking for "2019" must find it —
 * so it gets its own (bodyless) block; what it can't do is fold, in either
 * direction. reg_meta drops text-less population/object_type rows outright
 * (`_has_text`, `catalog.py`), but there the row IS the text; here the row is
 * the delivery. */
function hasText(version: Version): boolean {
  return (
    !!version.description?.trim() ||
    !!version.measurement_information?.trim() ||
    (version.populations ?? []).length > 0 ||
    (version.object_types ?? []).length > 0
  );
}

/** The delivery year in a version name ("2019", "LISA 2019", "HT 2019"), or
 * null when the name carries none. */
function versionYear(version: Version): number | null {
  const match = (version.name ?? "").match(YEAR);
  return match ? Number(match[0]) : null;
}

function variantYears(variant: Variant): number[] {
  return (variant.versions ?? [])
    .map(versionYear)
    .filter((year): year is number => year !== null);
}

/** The year a variant was first delivered; `Infinity` when it names none, which
 * sorts it after every dated sibling. */
function firstYear(years: readonly number[]): number {
  return years.length > 0 ? Math.min(...years) : Number.POSITIVE_INFINITY;
}

function yearRange(years: readonly number[]): string {
  if (years.length === 0) {
    return "";
  }
  let from = years[0];
  let to = years[0];
  for (const year of years) {
    if (year < from) from = year;
    if (year > to) to = year;
  }
  return from === to ? `${from}` : `${from}–${to}`;
}

/** Every text field of a version — its NAME included — with the years masked,
 * so two deliveries that differ ONLY in the year they name compare equal. A
 * year is the ONLY difference that folds: names that differ by anything else
 * ("Preliminär" / "Slutlig", "20190101" / "20200101" — 9 `register_version`
 * names in the corpus carry no year at all) are separate deliveries, and
 * collapsing them would print one name for both. Populations and object types
 * ride the same key: a changed frame — the one thing worth reading in a wall of
 * repeats — opens a new block instead of folding away. */
function foldKey(version: Version): string {
  return JSON.stringify([
    version.name,
    version.description,
    version.measurement_information,
    (version.populations ?? []).map((population) => [
      population.name,
      population.definition,
      population.comment,
      population.date_range,
    ]),
    (version.object_types ?? []).map((objectType) => [
      objectType.name,
      objectType.definition,
    ]),
  ]).replace(YEAR, YEAR_MASK);
}

/** The part of a concrete variant's name the entry label doesn't already carry:
 * "Individer" + "Individer, 16 år och äldre" reads as "16 år och äldre". Shares
 * the picker's stem projection (`labelSuffix`), so the two surfaces trim the
 * same separators; the `|| name` fallback covers the label-IS-the-name case,
 * where a suffix would be empty. */
function segmentName(label: string, variant: Variant): string {
  const name = variant.name ?? variant.slug;
  return labelSuffix(name, label) || name;
}
