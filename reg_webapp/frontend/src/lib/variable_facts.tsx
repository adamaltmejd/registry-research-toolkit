import type { ReactNode } from "react";
import type { CatalogSegment } from "./api";
import pageStyles from "./CatalogNodeView.module.css";
import { catalogHref, humanizeClassificationSlug } from "./catalog";
import { yearWindowLabel } from "./period";
import { columnYearRuns } from "./study_window";
import type { Definition } from "./ui/Definitions";
import { type ColumnType, TypePill } from "./ui/TypePill";
import styles from "./variable_facts.module.css";

/** One fact of a segment, keyed so equal values group; null when the catalog
 * does not state it, or states "no" for a fact shown only when yes, so the
 * row is left out rather than printed as "Not stated" or "No". */
type SegmentFact = { key: string; value: ReactNode } | null;

const FACTS: readonly {
  term: string;
  of: (row: CatalogSegment) => SegmentFact;
}[] = [
  // An identifier shows as the Identifier type pill, so it has no row here.
  {
    term: "Sensitive",
    of: (row) => (row.sensitive ? { key: "yes", value: "Yes" } : null),
  },
  // The codes themselves have their own section; only the classification
  // they code is a fact here.
  {
    term: "Classification",
    of: (row) =>
      row.classification
        ? {
            key: row.classification,
            value: (
              <a className="link" href={catalogHref(row.classification)}>
                {classificationName(row, row.classification)}
              </a>
            ),
          }
        : null,
  },
];

/** A classification's name: a vintage slug reads as its family and year
 * ("lkf1991" is "LKF 1991"); any other slug ("niva-oldv1") by the name its
 * value set gives it, as the value-set panel shows it. A value set's names
 * are shared across versions, so they never stand in for a vintage. */
function classificationName(row: CatalogSegment, ref: string): string {
  const slug = ref.split("/").at(-1) ?? "";
  const vintage = humanizeClassificationSlug(slug);
  if (vintage !== slug) return vintage;
  return (
    (row.value_set?.classification === ref
      ? row.value_set.names[0]
      : undefined) ?? slug
  );
}

/** The segments grouped by one fact's value, in first-seen order, and
 * whether any segment leaves it unstated. */
export function groupFact(
  segments: readonly CatalogSegment[],
  of: (row: CatalogSegment) => SegmentFact,
) {
  const groups = new Map<
    string,
    { value: ReactNode; segments: CatalogSegment[] }
  >();
  let unstated = false;
  for (const segment of segments) {
    const fact = of(segment);
    if (!fact) {
      unstated = true;
      continue;
    }
    const group = groups.get(fact.key) ?? { value: fact.value, segments: [] };
    group.segments.push(segment);
    groups.set(fact.key, group);
  }
  return { groups: [...groups], unstated };
}

/** A fact's years, muted, as a fact that differs by period lists them. */
export function factYears(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
) {
  return (
    <span className={styles.years}>
      {columnYearRuns(segments, bounds).map(yearWindowLabel).join(", ")}
    </span>
  );
}

const COLUMN_TYPES = {
  integer: true,
  decimal: true,
  text: true,
  date: true,
  datetime: true,
  boolean: true,
  geometry: true,
  uuid: true,
  binary: true,
  identifier: true,
} satisfies Record<ColumnType, true>;

/** A segment's type as the catalog states it, or null where it states none (no
 * pill). An identifier column shows as Identifier whatever it is stored as
 * (maintainer 2026-11-04: an identifier is its Type pill). */
function segmentType(segment: CatalogSegment): ColumnType | null {
  if (segment.identifier === true) return "identifier";
  const type = segment.type;
  return type !== undefined && Object.hasOwn(COLUMN_TYPES, type)
    ? (type as ColumnType)
    : null;
}

/** The Type row: one pill per catalog type of the columns, with its years
 * inside only where the type changes over time (maintainer 2026-11-04); a
 * column whose type the catalog does not state adds none. */
export function typeFact(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
): Definition[] {
  const { groups } = groupFact(segments, (row) => {
    const type = segmentType(row);
    return type && { key: type, value: null };
  });
  if (groups.length === 0) return [];
  return [
    {
      term: "Type",
      value: (
        <span className={styles.types}>
          {groups.map(([type, group]) => (
            <TypePill
              key={type}
              type={type as ColumnType}
              years={
                groups.length > 1
                  ? columnYearRuns(group.segments, bounds)
                      .map(yearWindowLabel)
                      .join(", ")
                  : undefined
              }
            />
          ))}
        </span>
      ),
    },
  ];
}

/** The facts of a variable's segments (DESIGN.md → Detail pane): a fact every
 * segment states alike is one value; one that differs lists each value with
 * its years; a fact no segment states is left out. */
export function segmentFacts(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
): Definition[] {
  return FACTS.flatMap(({ term, of }) => {
    const { groups, unstated } = groupFact(segments, of);
    if (groups.length === 0) return [];
    if (groups.length === 1 && !unstated)
      return [{ term, value: groups[0][1].value }];
    return [
      {
        term,
        value: (
          <ul className={pageStyles.plainList}>
            {groups.map(([key, group]) => (
              <li key={key}>
                {group.value} {factYears(group.segments, bounds)}
              </li>
            ))}
          </ul>
        ),
      },
    ];
  });
}
