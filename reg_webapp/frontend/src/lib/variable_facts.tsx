import type { ReactNode } from "react";
import { segmentType } from "./add_columns";
import type { CatalogSegment } from "./api";
import pageStyles from "./CatalogNodeView.module.css";
import { catalogHref, humanizeClassificationSlug } from "./catalog";
import { yearWindowLabel } from "./period";
import { columnYearRuns } from "./study_window";
import type { Definition } from "./ui/Definitions";
import { TypePill } from "./ui/TypePill";
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
                {humanizeClassificationSlug(
                  row.classification.split("/").at(-1) ?? "",
                )}
              </a>
            ),
          }
        : null,
  },
];

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

/** The Type row: one pill per type the columns are written as, with its years
 * inside only where the type changes over time (maintainer 2026-11-04). */
export function typeFact(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
): Definition[] {
  const { groups } = groupFact(segments, (row) => ({
    key: segmentType(row),
    value: null,
  }));
  if (groups.length === 0) return [];
  return [
    {
      term: "Type",
      value: (
        <span className={styles.types}>
          {groups.map(([type, group]) => (
            <TypePill
              key={type}
              type={segmentType(group.segments[0])}
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
