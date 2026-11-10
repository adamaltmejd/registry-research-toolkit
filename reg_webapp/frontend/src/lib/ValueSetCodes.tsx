import { type ReactNode, useEffect, useRef, useState } from "react";
import {
  Disclosure,
  Button as DisclosureButton,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import {
  getValueChanges,
  getValues,
  type ValueChange,
  type ValueChangesPage,
  type ValueItem,
  type ValueItemsPage,
  type ValueSetRef,
  type ValuesSubject,
} from "./api";
import pageStyles from "./CatalogNodeView.module.css";
import { SMALL_VALUE_SET } from "./catalog";
import { FilterInput } from "./FilterInput";
import { Button } from "./ui/Button";
import { EmptyState } from "./ui/EmptyState";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { useAsyncResource } from "./use_async_resource";
import styles from "./ValueSetCodes.module.css";

/** Codes per page past `SMALL_VALUE_SET` (maintainer, 2026-11-10, #2320). */
const PAGE_SIZE = 50;

interface Props {
  subject: ValuesSubject;
  valueSet: ValueSetRef;
}

/** Shared by a variable's or group's value set and a classification version. */
export function ValueSetCodes({ subject, valueSet }: Props) {
  const [filter, setFilter] = useState("");
  const [query, setQuery] = useState("");
  const [matches, setMatches] = useState<{
    query: string;
    total: number;
  } | null>(null);
  const showTotal = (total: number) => setMatches({ query, total });
  useEffect(() => {
    const timer = setTimeout(() => setQuery(filter.trim()), 200);
    return () => clearTimeout(timer);
  }, [filter]);
  return (
    <div className={styles.panel}>
      {valueSet.n_items > SMALL_VALUE_SET && (
        <FilterInput
          value={filter}
          onChange={setFilter}
          total={valueSet.n_items}
          shown={
            filter.trim() === query && matches?.query === query
              ? matches.total
              : null
          }
          label="Search codes"
          placeholder="Search codes…"
        />
      )}
      {valueSet.hierarchical ? (
        <>
          {/* Keep the reader's expanded branches while search shows flat matches. */}
          <div hidden={query !== ""}>
            <CodePage subject={subject} hierarchical query="" />
          </div>
          {query !== "" && (
            <CodePage
              key={query}
              subject={subject}
              hierarchical
              query={query}
              onTotal={showTotal}
            />
          )}
        </>
      ) : (
        <CodePage
          key={query}
          subject={subject}
          hierarchical={false}
          query={query}
          onTotal={showTotal}
        />
      )}
    </div>
  );
}

function CodeText({
  item,
  showLevel = false,
}: {
  item: ValueItem;
  showLevel?: boolean;
}) {
  return (
    <>
      <code className={styles.code}>{item.code === "" ? '""' : item.code}</code>
      <span className={styles.label}>
        <span>{item.label ?? "Not stated"}</span>
        {showLevel && <span className={styles.note}>Level {item.level}</span>}
        {item.sentinel && <span className={styles.note}>Special code</span>}
      </span>
    </>
  );
}

function Branch({
  item,
  subject,
}: {
  item: ValueItem;
  subject: ValuesSubject;
}) {
  const [requested, setRequested] = useState(false);
  return (
    <Disclosure
      className={styles.branch}
      onExpandedChange={(expanded) => {
        if (expanded) setRequested(true);
      }}
    >
      {({ isExpanded }) => (
        <>
          <DisclosureButton slot="trigger" className={styles.trigger}>
            <TreeChevron />
            <CodeText item={item} />
          </DisclosureButton>
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div className={styles.children}>
              {requested && (
                <CodePage
                  subject={subject}
                  hierarchical
                  query=""
                  parent={item.code}
                />
              )}
            </div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}

/** A list read page by page through the server's cursor: the pages read so
 * far, the latest page (for what every page repeats, a total or counts), and
 * the list and "Show more" refs. The owner remounts it to start over. */
function usePages<
  Item,
  Page extends { items: Item[]; next_cursor: string | null },
  List extends HTMLElement = HTMLUListElement,
>(
  key: string,
  load: (cursor: string | undefined, signal: AbortSignal) => Promise<Page>,
) {
  const list = useRef<List>(null);
  const moreButton = useRef<HTMLButtonElement>(null);
  const [carried, setCarried] = useState<{ items: Item[]; cursor?: string }>({
    items: [],
  });
  const [attempt, setAttempt] = useState(0);
  const resource = useAsyncResource(
    JSON.stringify([key, carried.cursor, attempt]),
    async (signal) => {
      const page = await load(carried.cursor, signal);
      // The last page removes its button. Move focus only if the reader is still on it.
      if (
        !signal.aborted &&
        carried.cursor &&
        page.next_cursor === null &&
        moreButton.current &&
        document.activeElement === moreButton.current
      )
        list.current?.focus({ preventScroll: true });
      return page;
    },
  );
  const items = [...carried.items, ...(resource.data?.items ?? [])];
  const cursor = resource.data?.next_cursor;
  return {
    items,
    page: resource.data,
    loading: resource.loading,
    error: resource.error,
    listRef: list,
    retry: () => setAttempt((n) => n + 1),
    more:
      cursor || (resource.loading && carried.cursor) ? (
        <Button
          ref={moreButton}
          isBusy={resource.loading}
          onPress={() => {
            if (!resource.loading && cursor) setCarried({ items, cursor });
          }}
        >
          Show more
        </Button>
      ) : null,
  };
}

/** Under a paged list: loading, a failed page with Retry, or the empty state;
 * then "Show more" while pages remain. */
function PagesFoot({
  pages,
  what,
  empty,
}: {
  pages: ReturnType<typeof usePages>;
  /** What did not load, capitalised: "Codes", "Code changes". */
  what: string;
  empty: ReactNode;
}) {
  return (
    <>
      {pages.loading ? (
        <div aria-busy="true">
          <Skeleton count={pages.items.length ? 1 : 3} />
        </div>
      ) : pages.error ? (
        <StatusRow tone="error" live="alert">
          {what} did not load ({pages.error}).{" "}
          <Button onPress={pages.retry}>Retry</Button>
        </StatusRow>
      ) : (
        !pages.items.length && empty
      )}
      {pages.more}
    </>
  );
}

/** Each sibling list owns its cursor. A new query remounts the list and drops old pages. */
function CodePage({
  subject,
  hierarchical,
  query,
  parent,
  onTotal,
}: {
  subject: ValuesSubject;
  hierarchical: boolean;
  query: string;
  parent?: string;
  onTotal?: (total: number) => void;
}) {
  const pages = usePages<ValueItem, ValueItemsPage>(
    JSON.stringify([subject, query, parent]),
    async (cursor, signal) => {
      const page = await getValues(
        subject,
        {
          limit: PAGE_SIZE,
          q: query || undefined,
          parent,
          level: hierarchical && !query && parent === undefined ? 1 : undefined,
          cursor,
        },
        { signal },
      );
      if (!signal.aborted) onTotal?.(page.total);
      return page;
    },
  );
  const { items, listRef } = pages;
  return (
    <div className={styles.panel}>
      {items.length > 0 && (
        <ul
          ref={listRef}
          tabIndex={-1}
          aria-label="Codes"
          className={`${pageStyles.plainList} ${styles.list}`}
        >
          {items.map((item) => (
            <li key={item.code}>
              {hierarchical && !query && (item.child_count ?? 0) > 0 ? (
                <Branch item={item} subject={subject} />
              ) : (
                <div className={styles.row}>
                  <CodeText item={item} showLevel={hierarchical && !!query} />
                </div>
              )}
            </li>
          ))}
        </ul>
      )}
      <PagesFoot
        pages={pages}
        what="Codes"
        empty={
          <EmptyState
            title={query ? "No matching codes." : "No codes recorded."}
            description={query ? "Try a different code or label." : undefined}
          />
        }
      />
    </div>
  );
}

const CHANGE_HEADINGS = {
  added: "Added",
  removed: "Removed",
  relabelled: "Relabelled",
} as const;

/** The codes added, removed and relabelled from `compare` to `classification`
 * (two versions of one classification): the counts first, the codes on
 * demand, a page at a time. Remount it for another pair. */
export function VersionChanges({
  classification,
  compare,
  compareName,
}: {
  classification: string;
  compare: string;
  compareName: string;
}) {
  const pages = usePages<ValueChange, ValueChangesPage, HTMLDivElement>(
    JSON.stringify([classification, compare]),
    (cursor, signal) =>
      getValueChanges(
        classification,
        compare,
        { limit: 200, cursor },
        { signal },
      ),
  );
  const counts = pages.page?.counts;
  const parts = counts
    ? (["added", "removed", "relabelled"] as const).flatMap((change) =>
        counts[change] > 0
          ? [`${counts[change]} ${CHANGE_HEADINGS[change].toLowerCase()}`]
          : [],
      )
    : [];
  // The server pages added, then removed, then relabelled: each run of one
  // kind goes under its heading, which a later page continues.
  const runs: { change: ValueChange["change"]; items: ValueChange[] }[] = [];
  for (const item of pages.items) {
    const last = runs.at(-1);
    if (last?.change === item.change) last.items.push(item);
    else runs.push({ change: item.change, items: [item] });
  }
  return (
    <section className={styles.changes}>
      <h3 className={styles.changesHeading}>Changes since {compareName}</h3>
      {/* The first page carries the counts: until it arrives, or when it
          fails, the foot says so. */}
      {counts && (
        <p className={styles.counts}>
          {parts.length > 0 ? `${parts.join(", ")}.` : "No codes changed."}
        </p>
      )}
      {pages.items.length > 0 && (
        <Disclosure className={pageStyles.disclosure}>
          {({ isExpanded }) => (
            <>
              <Heading level={4} className={pageStyles.disclosureHeading}>
                <DisclosureButton
                  slot="trigger"
                  className={pageStyles.disclosureButton}
                >
                  <TreeChevron />
                  Show the changed codes
                </DisclosureButton>
              </Heading>
              <DisclosurePanel
                className={`motion-disclose ${isExpanded ? "open" : ""}`}
              >
                <div className={styles.panel}>
                  <div
                    ref={pages.listRef}
                    tabIndex={-1}
                    className={styles.list}
                  >
                    {runs.map((run) => (
                      <section key={`${run.change}:${run.items[0].code}`}>
                        <h5 className={`micro-label ${styles.runHeading}`}>
                          {CHANGE_HEADINGS[run.change]}
                        </h5>
                        <ul className={pageStyles.plainList}>
                          {run.items.map((item) => (
                            <li key={item.code} className={styles.row}>
                              <code className={styles.code}>
                                {item.code === "" ? '""' : item.code}
                              </code>
                              <span className={styles.label}>
                                <span>
                                  {(item.change === "removed"
                                    ? item.previous_label
                                    : item.label) ?? "Not stated"}
                                </span>
                                {item.change === "relabelled" && (
                                  <span className={styles.note}>
                                    Was: {item.previous_label ?? "Not stated"}
                                  </span>
                                )}
                              </span>
                            </li>
                          ))}
                        </ul>
                      </section>
                    ))}
                  </div>
                  <PagesFoot pages={pages} what="Code changes" empty={null} />
                </div>
              </DisclosurePanel>
            </>
          )}
        </Disclosure>
      )}
      {pages.items.length === 0 && (
        <PagesFoot pages={pages} what="Code changes" empty={null} />
      )}
    </section>
  );
}
