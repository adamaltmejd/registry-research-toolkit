import { useCallback, useMemo, useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { ClassFamilyShow, Context } from "./api";
import styles from "./CatalogNodeView.module.css";
import classStyles from "./ClassificationView.module.css";
import { CodeTimeline } from "./CodeTimeline";
import { catalogHref, svCollator } from "./catalog";
import { catalogRootChildren } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { dataAxis } from "./period";
import { columnYearRuns, datedYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { Panel } from "./ui/Panel";
import { NameSkeleton } from "./ui/Skeleton";
import { TreeChevron } from "./ui/TreeChevron";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { useCatalogRead } from "./use_store_read";
import { windowBounds } from "./window_bounds";

/** A whole family's variable lists open at once when they come to at most this
 * many rows (about two screens); past that each version opens on demand (lkf:
 * 36 used versions, ~700 rows, a 35,000 px page). A row cap, not a version cap:
 * two versions can hold hundreds of rows. */
const OPEN_ALL_ROWS = 30;

/** A classification family's page (DESIGN.md → Detail pane): its versions in
 * start order, each over the steward's variables that use it behind a
 * disclosure, then the codes those variables' columns use. A version's address
 * renders this page with that version `focus`ed and open (Operator Q4, #2252). */
export function ClassificationView({
  node,
  focus,
  context,
}: {
  node: ClassFamilyShow;
  focus: string | null;
  context: Context | null;
}) {
  const { draft } = useProject();
  const bound = useMemo(
    () =>
      new Set(
        draft?.sources.flatMap((source) =>
          source.bindings.map((binding) => binding.variable),
        ) ?? [],
      ),
    [draft],
  );
  const study = studyWindow(draft);
  const catalogBounds = windowBounds(context);
  const bounds = [catalogBounds.min, catalogBounds.max] as const;
  const studyYears: [number, number] | null = study
    ? [study.from, study.to]
    : null;
  const variables = [...node.variables].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
  const axis = dataAxis(
    columnYearRuns(
      variables.flatMap((variable) => variable.segments),
      bounds,
    ).flatMap((run) => [run.from, run.to]),
    study,
    bounds,
  );
  // Like a grouped variable's lane: the focused version takes focus unless
  // the reader is already elsewhere, and scrolls only when out of view.
  const focusVersion = useCallback((heading: HTMLHeadingElement | null) => {
    if (!heading) return;
    const active = document.activeElement;
    if (active && active !== document.body) return;
    heading.focus({ preventScroll: true });
    const bar =
      Number.parseFloat(
        getComputedStyle(document.documentElement).getPropertyValue(
          "--top-bar-height",
        ),
      ) || 0;
    const { top, bottom } = heading.getBoundingClientRect();
    if (top < bar || bottom > window.innerHeight)
      heading.scrollIntoView({ block: "center" });
  }, []);
  // The register names come from the root read the browse column makes, so
  // naming each row's register costs no request per register.
  const root = useCatalogRead(catalogRootChildren, undefined);
  const registerNames = new Map(
    (root.value ?? []).flatMap((provider) =>
      provider.registers.map((register) => [register.fqid, register.name]),
    ),
  );
  // Only a list that spans registers names them: on one register's family the
  // name alone is enough.
  const crossRegister =
    new Set(variables.map((variable) => registerOf(variable.fqid))).size > 1;
  const registerLabel = (register: string) =>
    registerNames.get(register) ??
    (root.loading ? <NameSkeleton label="Loading register name" /> : null);
  const items = versionItems(node.versions, variables, focus);
  // Which versions show their variables: local state, not the URL. A family
  // with one used version, or few rows in all, opens whole; otherwise only the
  // version an address targets opens, and a later address opens its own.
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(() => {
    const used = items.flatMap((item) =>
      item.kind === "version" && item.users.length > 0 ? [item] : [],
    );
    const rows = used.reduce((sum, item) => sum + item.users.length, 0);
    return new Set(
      used.length <= 1 || rows <= OPEN_ALL_ROWS
        ? used.map((item) => item.version.fqid)
        : focus
          ? [focus]
          : [],
    );
  });
  const [openedFocus, setOpenedFocus] = useState(focus);
  if (focus !== openedFocus) {
    setOpenedFocus(focus);
    if (focus) setExpanded((open) => new Set(open).add(focus));
  }
  const setOpen = (fqid: string, open: boolean) =>
    setExpanded((current) => {
      const next = new Set(current);
      if (open) next.add(fqid);
      else next.delete(fqid);
      return next;
    });
  return (
    <article className={styles.page}>
      <Definitions
        kind="classification"
        title={node.name}
        definitions={
          node.description
            ? [{ term: "Description", value: node.description }]
            : []
        }
      />
      <Panel title="Versions" flush>
        <ul className={classStyles.versions}>
          {items.map((item) =>
            item.kind === "unused" ? (
              <li
                key={item.versions[0].fqid}
                className={`${classStyles.version} ${classStyles.unused}`}
              >
                {unusedLine(item.versions)}
              </li>
            ) : (
              <li
                key={item.version.fqid}
                className={classStyles.version}
                data-current={item.version.fqid === focus || undefined}
              >
                {item.users.length > 0 ? (
                  <Disclosure
                    className={classStyles.disclosure}
                    isExpanded={expanded.has(item.version.fqid)}
                    onExpandedChange={(open) =>
                      setOpen(item.version.fqid, open)
                    }
                  >
                    {({ isExpanded }) => (
                      <>
                        <Heading
                          level={3}
                          ref={
                            item.version.fqid === focus
                              ? focusVersion
                              : undefined
                          }
                          tabIndex={
                            item.version.fqid === focus ? -1 : undefined
                          }
                          className={classStyles.versionHeading}
                        >
                          <Button
                            slot="trigger"
                            className={classStyles.versionTrigger}
                          >
                            <TreeChevron />
                            <span className={classStyles.versionLine}>
                              <span className={classStyles.versionName}>
                                {item.version.name}
                              </span>
                              <span className={classStyles.years}>
                                {catalogPeriodLabel(item.version.period)}
                              </span>
                              <span className={classStyles.count}>
                                {item.users.length === 1
                                  ? "1 variable"
                                  : `${item.users.length} variables`}
                              </span>
                            </span>
                          </Button>
                        </Heading>
                        <DisclosurePanel
                          className={`motion-disclose ${isExpanded ? "open" : ""}`}
                        >
                          <div>
                            {/* Rows mount only while open: a long family's
                                closed versions cost no DOM. */}
                            {isExpanded && (
                              <VariableRowList
                                label={`Variables using ${item.version.name}`}
                                axis={axis}
                              >
                                {item.users.map(({ variable, rows }) => (
                                  <VariableRow
                                    key={variable.fqid}
                                    name={variable.name}
                                    href={catalogHref(variable.fqid)}
                                    inProject={bound.has(variable.fqid)}
                                    coverage={datedYearRuns(rows, bounds)}
                                    axis={axis}
                                    window={studyYears}
                                    detail={
                                      crossRegister
                                        ? registerLabel(
                                            registerOf(variable.fqid),
                                          )
                                        : undefined
                                    }
                                  />
                                ))}
                              </VariableRowList>
                            )}
                          </div>
                        </DisclosurePanel>
                      </>
                    )}
                  </Disclosure>
                ) : (
                  <>
                    <div className={classStyles.versionHead}>
                      <h3
                        ref={
                          item.version.fqid === focus ? focusVersion : undefined
                        }
                        tabIndex={item.version.fqid === focus ? -1 : undefined}
                        className={classStyles.versionName}
                      >
                        {item.version.name}
                      </h3>
                      <span className={classStyles.years}>
                        {catalogPeriodLabel(item.version.period)}
                      </span>
                    </div>
                    <p className={classStyles.unusedNote}>
                      No variable here uses this version.
                    </p>
                  </>
                )}
              </li>
            ),
          )}
        </ul>
      </Panel>
      <CodeTimeline
        members={variables}
        axis={axis}
        window={studyYears}
        bounds={bounds}
      />
    </article>
  );
}

type Version = ClassFamilyShow["versions"][number];
type User = ClassFamilyShow["variables"][number];

const registerOf = (fqid: string) => fqid.split("/").slice(0, 2).join("/");

/** The versions in start order, with each run of consecutive versions no
 * variable uses folded into one line, so a long family is not a page of empty
 * entries. A version address that lands inside a run splits it: the focused
 * version keeps its own entry (heading, years, focus), and the versions
 * before and after it fold on their own. */
function versionItems(
  versions: readonly Version[],
  variables: readonly User[],
  focus: string | null,
) {
  const items: (
    | {
        kind: "version";
        version: Version;
        users: { variable: User; rows: User["segments"] }[];
      }
    | { kind: "unused"; versions: Version[] }
  )[] = [];
  for (const version of versions) {
    const users = variables.flatMap((variable) => {
      const rows = variable.segments.filter(
        (segment) => segment.classification === version.fqid,
      );
      return rows.length > 0 ? [{ variable, rows }] : [];
    });
    const last = items.at(-1);
    if (users.length > 0 || version.fqid === focus)
      items.push({ kind: "version", version, users });
    else if (last?.kind === "unused") last.versions.push(version);
    else items.push({ kind: "unused", versions: [version] });
  }
  return items;
}

/** A folded run, by its versions' names: all of them up to three, else the
 * first two and the last. */
function unusedLine(versions: readonly Version[]): string {
  const names = versions.map((version) => version.name);
  if (names.length === 1)
    return `${names[0]}: no variable here uses this version.`;
  const listed =
    names.length <= 3
      ? names.join(", ")
      : `${names[0]}, ${names[1]} … ${names.at(-1)}`;
  return `${listed}: no variable here uses these versions.`;
}
