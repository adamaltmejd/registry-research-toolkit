import { useEffect, useState } from "react";
import { getValues, type ValueRow } from "./api";
import { CodeList } from "./CodeList";
import styles from "./CodeList.module.css";
import { CodeTree } from "./CodeTree";
import { useCodeTree } from "./code_tree";
import { FilterInput } from "./FilterInput";
import { Button } from "./ui/Button";
import { Skeleton } from "./ui/Skeleton";
import { useAsyncResource } from "./use_async_resource";

interface Props {
  /** Whose codes to read: a variable FQID (with `stateId`) or a classification
   * FQID (`class/<slug>`, no state). */
  fqid: string;
  /** The variable's state whose value set is read; with `classification`, its
   * stored part against that declared classification instead. */
  stateId?: string | null;
  partition?: "canonical" | "source_extensions" | "nonstandard" | "sentinels";
  /** The declared classification's ref (`class/<slug>`). */
  classification?: string;
  column?: string | null;
  aliasWindowFrom?: string | null;
  /** The whole (unfiltered) set's size from the leaf's summary, so the filter
   * and its count render before the first page lands. Null when the caller
   * does not know: the first unfiltered page's `total` decides. */
  codeCount: number | null;
  filterLabel?: string;
  filterPlaceholder?: string;
  /** The line for a set with no codes at all. */
  emptyText?: string;
}

// One request covers the ordinary coding; it is also the server's ceiling.
const PAGE_SIZE = 200;
// Below this many codes the filter is more chrome than help.
const CODE_FILTER_THRESHOLD = 5;
// The filter runs on the server over the whole set, so a burst of keystrokes
// becomes one read, not one per character.
const FILTER_DEBOUNCE_MS = 200;

type Carried = {
  codes: ValueRow[];
  total: number | null;
  cursor: string | null;
};
const NOTHING_CARRIED: Carried = { codes: [], total: null, cursor: null };

function treePage(items: ValueRow[] | undefined): boolean {
  const first = items?.[0];
  return (
    first !== undefined && "child_count" in first && first.child_count != null
  );
}

/**
 * The (code, label) viewer for one set of codes, read a page at a time (Y-46)
 * through `values`: one state's value set, that state's part against a declared
 * classification, or a classification's own codes. Mounted only where a code
 * table is shown, so a closed disclosure issues no request.
 *
 * Filtering is server-side over the complete set (the server folds diacritics
 * and case as `matchesFilter` does), then paged: that order keeps "12 of 740"
 * true. A classification whose codes name their parents (#1297) renders as a
 * code tree, unfiltered; a filter switches back to the flat matches.
 */
export function ValueSetCodes({
  fqid,
  stateId = null,
  partition = "source_extensions",
  classification,
  column = null,
  aliasWindowFrom = null,
  codeCount,
  filterLabel = "Filter codes",
  filterPlaceholder = "Filter codes…",
  emptyText = "This value set has no codes.",
}: Props) {
  const [filter, setFilter] = useState("");
  // What the server was last asked for: `filter` once typing settles.
  const [query, setQuery] = useState("");
  useEffect(() => {
    if (filter.trim() === query.trim()) return;
    const timer = setTimeout(() => setQuery(filter), FILTER_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [filter, query]);
  // Bumped by Retry: part of the request key, so a failed page re-requests.
  const [attempt, setAttempt] = useState(0);

  // The SET being read, and the filtered read of it. Accumulated pages are
  // keyed on the latter, so a new set or query drops them.
  const setIdentity = `${fqid}:${stateId ?? ""}:${partition}:${classification ?? ""}:${column ?? ""}:${aliasWindowFrom ?? ""}`;
  const filtering = query.trim() !== "";
  const setKey = `${setIdentity}:${query.trim()}`;

  // The pages before the one in flight, their matching total and the cursor
  // that continues after them.
  const [accumulated, setAccumulated] = useState<Carried & { key: string }>({
    key: "",
    ...NOTHING_CARRIED,
  });
  const carried = accumulated.key === setKey ? accumulated : NOTHING_CARRIED;

  // The whole set's size and whether it is a code tree, remembered per set from
  // the first unfiltered page, so typing a filter can't make the box vanish or
  // the tree forget itself.
  const [answered, setAnswered] = useState<{
    identity: string;
    size: number;
    tree: boolean;
  } | null>(null);
  const known = answered?.identity === setIdentity ? answered : null;
  const setSize = codeCount ?? known?.size ?? null;
  const tree = known?.tree ?? false;

  // A known-empty set has nothing to read, and the tree reads its own levels
  // once the first page has said it is one.
  const fetching = codeCount !== 0 && !(tree && !filtering);
  const resource = useAsyncResource(
    fetching ? `${setKey}\u0000${carried.cursor ?? ""}\u0000${attempt}` : null,
    (signal) =>
      getValues(
        fqid,
        {
          state: stateId,
          // A partition is a part of a declared classification's book.
          partition: classification ? partition : undefined,
          classification,
          column: column ?? undefined,
          alias_window_from: aliasWindowFrom ?? undefined,
          q: query.trim(),
          cursor: carried.cursor,
          limit: PAGE_SIZE,
        },
        { signal },
      ),
  );
  const data = fetching
    ? resource.data
    : {
        items: [],
        next_cursor: null,
        total: codeCount === 0 ? 0 : (setSize ?? 0),
      };

  if (resource.data && !filtering && answered?.identity !== setIdentity) {
    setAnswered({
      identity: setIdentity,
      size: resource.data.total,
      tree: treePage(resource.data.items),
    });
  }

  const codes = [...carried.codes, ...(data?.items ?? [])];
  // Where the next page starts; while a later page is in flight it is the
  // cursor that page was asked from, so "Load more" stays put (busy).
  const nextCursor = data ? (data.next_cursor ?? null) : carried.cursor;
  const total = data?.total ?? carried.total;
  // The count belongs to the query the server answered: withheld while the box
  // is ahead of it.
  const shownTotal = filter.trim() === query.trim() ? total : null;
  const showTree = !filtering && (tree || treePage(resource.data?.items));
  const showFilter = setSize !== null && setSize >= CODE_FILTER_THRESHOLD;

  // Each tree level's read and the open nodes, kept for the panel's life: a
  // filter round-trip reopens the tree as it was. Only a classification's codes
  // form a tree, and its panel is keyed on the FQID, so a tree never outlives
  // its classification.
  const codeTree = useCodeTree(fqid);

  const loading = fetching && resource.loading;
  function loadMore(): void {
    // Busy, not disabled: a second press mid-flight is a no-op.
    if (loading || nextCursor === null) return;
    setAccumulated({ key: setKey, codes, total, cursor: nextCursor });
  }

  return (
    <div className={styles.panel}>
      {showFilter && (
        <FilterInput
          value={filter}
          onChange={setFilter}
          total={setSize ?? 0}
          shown={shownTotal}
          label={filterLabel}
          placeholder={filterPlaceholder}
        />
      )}
      {showTree ? (
        <div className={styles.scroll}>
          {/* The tree owns its loading, error and paging states. */}
          <CodeTree tree={codeTree} label="Codes" />
        </div>
      ) : (
        <>
          <CodeList codes={codes} />
          {loading ? (
            <div aria-busy="true">
              <span className="visually-hidden">Loading codes…</span>
              <Skeleton count={codes.length > 0 ? 1 : 3} />
            </div>
          ) : resource.error !== null ? (
            <div className={styles.status} role="alert">
              <p className="error">
                <span aria-hidden="true">✕</span> Could not load codes:{" "}
                {resource.error}
              </p>
              <Button onPress={() => setAttempt((n) => n + 1)}>Retry</Button>
            </div>
          ) : (
            codes.length === 0 && (
              <p className={styles.empty}>
                {filtering ? `No codes match “${query}”.` : emptyText}
              </p>
            )
          )}
          {nextCursor !== null && resource.error === null && (
            // Outside the status chain, so the pressed button is not swapped
            // for a skeleton mid-page, which would drop focus to <body>.
            <div className={styles.status}>
              <Button onPress={loadMore} isBusy={loading}>
                Load more codes
              </Button>
              {total !== null && (
                <span className={styles.empty}>
                  Showing {codes.length} of {total} codes.
                </span>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
