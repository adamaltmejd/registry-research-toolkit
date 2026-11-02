import { useEffect, useRef, useState } from "react";
import {
  Disclosure,
  Button as DisclosureButton,
  DisclosurePanel,
} from "react-aria-components";
import {
  getValues,
  type ValueItem,
  type ValueSetRef,
  type ValuesSubject,
} from "./api";
import pageStyles from "./CatalogNodeView.module.css";
import { FilterInput } from "./FilterInput";
import { Button } from "./ui/Button";
import { EmptyState } from "./ui/EmptyState";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { useAsyncResource } from "./use_async_resource";
import styles from "./ValueSetCodes.module.css";

interface Props {
  subject: ValuesSubject;
  valueSet: ValueSetRef;
}

/** Shared by an inline value set and a classification version. */
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
  const list = useRef<HTMLUListElement>(null);
  const moreButton = useRef<HTMLButtonElement>(null);
  const [carried, setCarried] = useState<{
    items: ValueItem[];
    cursor?: string;
  }>({ items: [] });
  const [attempt, setAttempt] = useState(0);
  const resource = useAsyncResource(
    JSON.stringify([subject, query, parent, carried.cursor, attempt]),
    async (signal) => {
      const page = await getValues(
        subject,
        {
          limit: 200,
          q: query || undefined,
          parent,
          level: hierarchical && !query && parent === undefined ? 1 : undefined,
          cursor: carried.cursor,
        },
        { signal },
      );
      if (!signal.aborted) {
        onTotal?.(page.total);
        // The last page removes its button. Move focus only if the reader is still on it.
        if (
          carried.cursor &&
          page.next_cursor === null &&
          moreButton.current &&
          document.activeElement === moreButton.current
        )
          list.current?.focus({ preventScroll: true });
      }
      return page;
    },
  );
  const items = [...carried.items, ...(resource.data?.items ?? [])];
  const cursor = resource.data?.next_cursor;
  return (
    <div className={styles.panel}>
      {items.length > 0 && (
        <ul
          ref={list}
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
      {resource.loading ? (
        <div aria-busy="true">
          <Skeleton count={items.length ? 1 : 3} />
        </div>
      ) : resource.error ? (
        <StatusRow tone="error" live="alert">
          Codes did not load ({resource.error}).{" "}
          <Button onPress={() => setAttempt((n) => n + 1)}>Retry</Button>
        </StatusRow>
      ) : (
        !items.length && (
          <EmptyState
            title={query ? "No matching codes." : "No codes recorded."}
            description={query ? "Try a different code or label." : undefined}
          />
        )
      )}
      {(cursor || (resource.loading && carried.cursor)) && (
        <Button
          ref={moreButton}
          isBusy={resource.loading}
          onPress={() => {
            if (!resource.loading && cursor) setCarried({ items, cursor });
          }}
        >
          Show more
        </Button>
      )}
    </div>
  );
}
