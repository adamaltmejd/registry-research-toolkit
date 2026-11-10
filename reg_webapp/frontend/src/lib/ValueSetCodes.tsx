import { type ReactNode, useEffect, useRef, useState } from "react";
import {
  Disclosure,
  Button as DisclosureButton,
  DisclosurePanel,
} from "react-aria-components";
import {
  getValues,
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
 * far and the list and "Show more" refs. The owner remounts it to start over. */
function usePages<
  Item,
  Page extends { items: Item[]; next_cursor: string | null },
>(
  key: string,
  load: (cursor: string | undefined, signal: AbortSignal) => Promise<Page>,
) {
  const list = useRef<HTMLUListElement>(null);
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
