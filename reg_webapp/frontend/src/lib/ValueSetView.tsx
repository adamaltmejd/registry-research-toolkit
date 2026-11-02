import { useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { CatalogSegment, ValueSetRef } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogPeriodRuns } from "./catalog_period";
import { Panel } from "./ui/Panel";
import { TreeChevron } from "./ui/TreeChevron";
import { ValueSetCodes } from "./ValueSetCodes";
import valueStyles from "./ValueSetView.module.css";

/** The segments are already ordered by the API; share one code read per distinct hash. */
export function ValueSetView({
  segments,
  names,
}: {
  segments: CatalogSegment[];
  names: ReadonlyMap<string, string>;
}) {
  const sets = new Map<
    string,
    { valueSet: ValueSetRef; tables: Map<string, CatalogSegment["period"][]> }
  >();
  for (const segment of segments) {
    if (!segment.value_set) continue;
    const { hash } = segment.value_set;
    let entry = sets.get(hash);
    if (!entry) {
      entry = { valueSet: segment.value_set, tables: new Map() };
      sets.set(hash, entry);
    }
    const periods = entry.tables.get(segment.table) ?? [];
    periods.push(segment.period);
    entry.tables.set(segment.table, periods);
  }
  if (!sets.size) return null;
  return (
    <Panel title="Value sets">
      {[...sets].map(([hash, entry]) => (
        <ValueSetEntry key={hash} {...entry} names={names} />
      ))}
    </Panel>
  );
}

function ValueSetEntry({
  valueSet,
  tables,
  names,
}: {
  valueSet: ValueSetRef;
  tables: Map<string, CatalogSegment["period"][]>;
  names: ReadonlyMap<string, string>;
}) {
  const [requested, setRequested] = useState(false);
  const label = valueSet.names.join(", ") || "Unnamed value set";
  const coverage = [...tables]
    .map(
      ([table, periods]) =>
        `${names.get(table) ?? "Table name unavailable"} ${catalogPeriodRuns(periods)}`,
    )
    .join("; ");
  return (
    <Disclosure
      className={styles.disclosure}
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
              className={styles.disclosureButton}
              aria-label={`${label}; ${coverage}`}
            >
              <TreeChevron />
              <span>{label}</span>
            </Button>
          </Heading>
          <ul className={`${styles.plainList} ${valueStyles.coverage}`}>
            {[...tables].map(([table, periods]) => (
              <li key={table}>
                <a className="link" href={catalogHref(table)}>
                  {names.get(table) ?? "Table name unavailable"}
                </a>{" "}
                <code>{catalogPeriodRuns(periods)}</code>
              </li>
            ))}
          </ul>
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div className={valueStyles.codes}>
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
