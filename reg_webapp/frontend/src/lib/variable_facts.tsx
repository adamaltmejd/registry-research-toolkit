import type { ReactNode } from "react";
import type { CatalogSegment } from "./api";
import pageStyles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { segmentYears } from "./catalog_period";
import type { Definition } from "./ui/Definitions";
import { type ColumnType, TypePill } from "./ui/TypePill";
import styles from "./variable_facts.module.css";

/** One fact of a segment, keyed so equal values group; null when the catalog
 * does not state it, or states "no" for a fact shown only when yes, so the
 * row is left out rather than printed as "Not stated" or "No". */
type SegmentFact = { key: string; value: ReactNode } | null;

/** A classification version's family, by the version's ref: its name is the
 * server's, from the classifications list. */
export type ClassFamilies = ReadonlyMap<string, { fqid: string; name: string }>;

const facts = (
  families: ClassFamilies,
): readonly {
  term: string;
  of: (row: CatalogSegment) => SegmentFact;
}[] => [
  // An identifier shows as the Identifier type pill, so it has no row here.
  {
    term: "Sensitive",
    of: (row) => (row.sensitive ? { key: "yes", value: "Yes" } : null),
  },
  // The codes themselves have their own section; only the classification
  // they code is a fact here: its family once, whose page lists the versions
  // (#2319), else the version, by the name the server serves (never derived
  // from a ref; a row without one states no fact).
  {
    term: "Classification",
    of: (row) => {
      if (!row.classification) return null;
      const family = families.get(row.classification);
      const [ref, name] = family
        ? [family.fqid, family.name]
        : [row.classification, row.classification_name];
      return name
        ? {
            key: ref,
            value: (
              <a className="link" href={catalogHref(ref)}>
                {name}
              </a>
            ),
          }
        : null;
    },
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
export function factYears(segments: readonly CatalogSegment[]) {
  return <span className={styles.years}>{segmentYears(segments)}</span>;
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

/** One pill per catalog type of the columns, with its years
 * inside only where the type changes over time (maintainer 2026-11-04); a
 * column whose type the catalog does not state adds no pill, but the stated
 * type then carries its years, so it never reads as the whole history. */
export function typePills(segments: readonly CatalogSegment[]): ReactNode[] {
  const { groups, unstated } = groupFact(segments, (row) => {
    const type = segmentType(row);
    return type && { key: type, value: null };
  });
  return groups.map(([type, group]) => (
    <TypePill
      key={type}
      type={type as ColumnType}
      years={
        groups.length > 1 || unstated ? segmentYears(group.segments) : undefined
      }
    />
  ));
}

/** The facts of a variable's or a group's segments (DESIGN.md → Detail pane):
 * a fact every segment states alike is one value; one that differs lists each
 * value with its years; a fact no segment states is left out. */
export function segmentFacts(
  segments: readonly CatalogSegment[],
  families: ClassFamilies,
): Definition[] {
  return facts(families).flatMap(({ term, of }) => {
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
                {group.value} {factYears(group.segments)}
              </li>
            ))}
          </ul>
        ),
      },
    ];
  });
}
