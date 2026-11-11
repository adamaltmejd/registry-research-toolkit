import { Fragment, useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { CatalogSegment, ValueSetRef } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, SMALL_VALUE_SET } from "./catalog";
import { catalogPeriodRuns } from "./catalog_period";
import { EntityPill } from "./ui/EntityPill";
import { Panel } from "./ui/Panel";
import { TreeChevron } from "./ui/TreeChevron";
import { ValueSetCodes } from "./ValueSetCodes";
import valueStyles from "./ValueSetView.module.css";

/** Where a value set is used: one column of one table, over its periods. */
interface Use {
  table: string;
  column: string;
  periods: CatalogSegment["period"][];
}

/** The value sets of a variable's or a group's columns (DESIGN.md → Detail
 * pane): one entry per distinct hash, in the API's order, each naming the
 * tables and columns that use it. A set of at most `SMALL_VALUE_SET` codes
 * opens at once; a larger one reads its codes when opened (#2320). */
export function ValueSetView({
  segments,
  names,
}: {
  segments: readonly CatalogSegment[];
  names: ReadonlyMap<string, string>;
}) {
  const sets = new Map<
    string,
    { valueSet: ValueSetRef; uses: Map<string, Use> }
  >();
  for (const segment of segments) {
    if (!segment.value_set) continue;
    const { hash } = segment.value_set;
    let entry = sets.get(hash);
    if (!entry) {
      entry = { valueSet: segment.value_set, uses: new Map() };
      sets.set(hash, entry);
    }
    // A group's members can share a table: the column tells them apart.
    const key = JSON.stringify([segment.table, segment.column]);
    const use = entry.uses.get(key) ?? {
      table: segment.table,
      column: segment.column,
      periods: [],
    };
    use.periods.push(segment.period);
    entry.uses.set(key, use);
  }
  if (!sets.size) return null;
  return (
    <Panel title={<span className={valueStyles.panelTitle}>Value sets</span>}>
      {[...sets].map(([hash, entry]) => (
        <ValueSetEntry key={hash} {...entry} names={names} />
      ))}
    </Panel>
  );
}

function ValueSetEntry({
  valueSet,
  uses,
  names,
}: {
  valueSet: ValueSetRef;
  uses: Map<string, Use>;
  names: ReadonlyMap<string, string>;
}) {
  const small = valueSet.n_items <= SMALL_VALUE_SET;
  const [requested, setRequested] = useState(small);
  const firstName = valueSet.names[0] ?? "Unnamed value set";
  const more = valueSet.names.length - 1;
  const label = `${firstName}${more > 0 ? ` and ${more} more` : ""}`;
  const tableName = (table: string) =>
    names.get(table) ?? "Table name unavailable";
  const coverage = [...uses.values()]
    .map(({ table, column, periods }) =>
      [tableName(table), column, catalogPeriodRuns(periods)]
        .filter(Boolean)
        .join(" "),
    )
    .join("; ");
  return (
    <Disclosure
      className={styles.disclosure}
      defaultExpanded={small}
      onExpandedChange={(expanded) => {
        if (expanded) setRequested(true);
      }}
    >
      {({ isExpanded }) => (
        <>
          <Heading
            level={3}
            className={`${styles.disclosureHeading} ${valueStyles.heading}`}
          >
            <Button
              slot="trigger"
              className={`${styles.disclosureButton} ${valueStyles.trigger}`}
              aria-label={`${label}; ${coverage}`}
            >
              <TreeChevron />
              <span className={valueStyles.firstName}>{firstName}</span>
              {more > 0 && (
                <span className={valueStyles.more}> and {more} more</span>
              )}
            </Button>
          </Heading>
          <ul className={`${styles.plainList} ${valueStyles.coverage}`}>
            {[...uses].map(([key, { table, column, periods }]) => (
              <li key={key}>
                <EntityPill
                  entity="table"
                  name={tableName(table)}
                  href={catalogHref(table)}
                />{" "}
                <EntityPill entity="column" name={column} />
                {/* Outside the chip, which never wraps: a long run of school
                    years wraps on a phone. */}
                <TableYears periods={periods} />
              </li>
            ))}
          </ul>
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div className={valueStyles.codes}>
              <ul className={`${styles.plainList} ${valueStyles.names}`}>
                {valueSet.names.map((name) => (
                  <li key={name}>{name}</li>
                ))}
              </ul>
              {requested && (
                <ValueSetCodes
                  subject={{ value_set: valueSet.hash }}
                  valueSet={valueSet}
                />
              )}
            </div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}

function TableYears({ periods }: { periods: CatalogSegment["period"][] }) {
  const years = catalogPeriodRuns(periods);
  // Wraps between runs, never inside one ("1991–1992").
  return (
    years && (
      <>
        {" "}
        <code>
          {years.split(", ").map((run, i) => (
            <Fragment key={run}>
              {i > 0 && ", "}
              <span className={valueStyles.run}>{run}</span>
            </Fragment>
          ))}
        </code>
      </>
    )
  );
}
