import { type KeyboardEvent, useEffect, useRef, useState } from "react";
import {
  Button as AriaButton,
  Collection,
  type Key,
  type Selection,
  Tree,
  TreeItem,
  TreeItemContent,
} from "react-aria-components";
import {
  type ClassFamilyRef,
  type RootChild,
  SEARCH_MIN_QUERY_LENGTH,
  searchFacets,
} from "./api";
import styles from "./BrowseTree.module.css";
import {
  allRegisterFacets,
  browseMatches,
  browseOrder,
  useBrowseFilter,
  useSettledFilter,
} from "./browse_filter";
import {
  catalogHref,
  leafSlug,
  rankFilter,
  registerOfPath,
  sortedRegisters,
} from "./catalog";
import {
  CLASSIFICATIONS,
  CLASSIFICATIONS_LABEL,
  catalogRootChildren,
  classFamiliesRead,
} from "./catalog_names";
import { Highlight } from "./Highlight";
import { type Route, router } from "./router";
import { keepFocusVisible, type TreeState, visibleKey } from "./tree_focus";
import { Button } from "./ui/Button";
import { InProjectMark } from "./ui/ColumnList";
import columnStyles from "./ui/ColumnList.module.css";
import { focusBrowseColumn } from "./ui/column_keys";
import { StatusRow } from "./ui/StatusRow";
import { useSelectionBar } from "./ui/selection_bar";
import { useAsyncResource } from "./use_async_resource";
import { useRoute } from "./use_router";
import { useCatalogRead } from "./use_store_read";

/** One row: a provider at level 1, a register under it; or, below the
 * providers, the one "Classifications" row, which never expands: the
 * classifications are listed in the next column, as a register's variables
 * are (maintainer review of the classification pages, 2026-11-05, point 2). */
interface BrowseNode {
  id: string;
  name: string;
  href: string;
  children: BrowseNode[];
  /** Set on a row the filter keeps only through what it lists: how many
   * ("4 variables", "3 classifications"). */
  matchedOn?: string;
  /** Drawn under a rule that sets it apart from the rows above. */
  separated?: boolean;
  /** The items a leaf row lists in the next column, each by its names: the
   * filter keeps the row while one of them matches. */
  listed?: { noun: [string, string]; names: string[][] };
}

const COLLAPSED_KEY = "reg_webapp:browse_collapsed";

/** The groups this user collapsed, remembered across visits. Collapsed rather
 * than expanded, so a provider new to the catalog arrives open. Best-effort: an
 * unreadable store (private mode) starts with every group open. */
function readCollapsed(): Set<string> {
  try {
    const stored: unknown = JSON.parse(
      localStorage.getItem(COLLAPSED_KEY) ?? "[]",
    );
    return new Set(
      Array.isArray(stored)
        ? stored.filter((v): v is string => typeof v === "string")
        : [],
    );
  } catch {
    return new Set();
  }
}

function writeCollapsed(collapsed: Set<string>): void {
  try {
    localStorage.setItem(COLLAPSED_KEY, JSON.stringify([...collapsed]));
  } catch {
    // Not fatal: the collapse state lasts this session only.
  }
}

function inProjectCount(
  node: BrowseNode,
  counts: Record<string, number>,
): number {
  if (node.id in counts) return counts[node.id];
  const prefix = `${node.id}/`;
  return Object.entries(counts).reduce(
    (n, [register, count]) => (register.startsWith(prefix) ? n + count : n),
    0,
  );
}

interface CurrentRow {
  key: string;
  /** The page open is the row's own page, not one under it. */
  exact: boolean;
}

/** The row the current page belongs to, by key (the row's FQID): a variable,
 * group or table page lives under its register, every classification page
 * under the one "Classifications" row. */
function currentRow(route: Route): CurrentRow | null {
  switch (route.name) {
    case "catalog-node": {
      const segments = route.fqidPath.split("/");
      const key =
        segments[0] === CLASSIFICATIONS
          ? CLASSIFICATIONS
          : (registerOfPath(route.fqidPath) ?? segments[0]);
      return { key, exact: key === route.fqidPath };
    }
    default:
      return null;
  }
}

/** The browse filter applied to the tree. A group keeps the registers that
 * match by name or slug, then those that match only through their variables
 * (`facets`, per register FQID), each with its count; or all of them when its
 * own name matches. Those groups are `matched`. A group with no match is left
 * out until the filter clears (#1830), and so is a leaf row whose own name does
 * not match, unless a name it lists in the next column does. */
function filterNodes(
  nodes: BrowseNode[],
  needle: string,
  facets: ReadonlyMap<string, number>,
): { nodes: BrowseNode[]; matched: Set<Key> } {
  const matched = new Set<Key>();
  if (needle.trim() === "") return { nodes, matched };
  const named = (node: BrowseNode) => [node.name, leafSlug(node.id)];
  const filtered = nodes.flatMap((node) => {
    if (rankFilter([node], needle, named).length > 0) {
      if (node.children.length > 0) matched.add(node.id);
      return [node];
    }
    const listedHits = node.listed
      ? rankFilter(node.listed.names, needle, (names) => names).length
      : 0;
    if (node.listed && listedHits > 0)
      return [{ ...node, matchedOn: counted(listedHits, node.listed.noun) }];
    const byName = rankFilter(node.children, needle, named);
    const byVariables = node.children
      .filter((child) => !byName.includes(child) && facets.has(child.id))
      .map((child) => ({
        ...child,
        matchedOn: counted(facets.get(child.id) ?? 0, [
          "variable",
          "variables",
        ]),
      }));
    const children = [...byName, ...byVariables];
    if (children.length === 0) return [];
    matched.add(node.id);
    return [{ ...node, children }];
  });
  return { nodes: filtered, matched };
}

function browseNodes(
  root: RootChild[],
  families: ClassFamilyRef[],
): BrowseNode[] {
  const providers = root.map((child) => ({
    id: child.fqid,
    name: child.name ?? child.fqid,
    href: catalogHref(child.fqid),
    children: sortedRegisters(child.registers).map((register) => ({
      id: register.fqid,
      name: register.name ?? register.fqid,
      href: catalogHref(register.fqid),
      children: [],
    })),
  }));
  if (families.length === 0) return providers;
  // One row after the providers, under the list's section rule (maintainer
  // review of the classification pages, 2026-11-05, point 1); the
  // classifications themselves are the next column's items. The filter matches
  // them by name and slug, as that column does.
  const classifications: BrowseNode = {
    id: CLASSIFICATIONS,
    name: CLASSIFICATIONS_LABEL,
    href: catalogHref(CLASSIFICATIONS),
    separated: true,
    children: [],
    listed: {
      noun: ["classification", "classifications"],
      names: families.map((family) => [family.name, leafSlug(family.fqid)]),
    },
  };
  return [...providers, classifications];
}

/**
 * The first browse column (DESIGN.md → Layout): every register, grouped under a
 * sticky provider row. Selection follows the arrow keys and a click, and
 * selecting a row opens its page; the current page's row carries the selection
 * bar. Rows are not links (RAC's selectable rows are not `<a>`). Every group
 * starts open; a group's chevron, ← / →, and the
 * header's one "Collapse all" / "Expand all" toggle fold them, remembered per user.
 * Pressing a collapsed group's row also opens it, and never closes one.
 * Collapsing a group never moves the page: the detail pane keeps showing the
 * register it showed, and keyboard focus moves to the group row.
 */
export function BrowseTree({ className }: { className?: string }) {
  const boxRef = useRef<HTMLDivElement>(null);
  const barRef = useRef<HTMLSpanElement>(null);
  useSelectionBar(boxRef, barRef);
  const route = useRoute();
  const root = useCatalogRead(catalogRootChildren, undefined);
  const counts: Record<string, number> = {};
  const needle = useBrowseFilter();
  const filtering = needle.trim() !== "";
  // The variable matches per register, asked once the text rests.
  const facetKey = useSettledFilter();
  const facetResource = useAsyncResource(facetKey, (signal) =>
    searchFacets(facetKey ?? "", { signal }),
  );
  const facets = new Map(
    (facetKey === null ? [] : allRegisterFacets(facetResource.data)).flatMap(
      (f) =>
        f.fqid && f.variable_matches > 0
          ? [[f.fqid, f.variable_matches] as const]
          : [],
    ),
  );
  const current = currentRow(route);
  const currentKey = current?.key ?? null;
  const families = useCatalogRead(classFamiliesRead, undefined);
  const allNodes = browseNodes(root.value ?? [], families.value ?? []);
  // Without facets, name-only filtering would hide possible variable matches.
  // Keep the loaded registers visible until the researcher retries the search.
  const facetError = facetResource.error;
  const { nodes, matched } = filterNodes(
    allNodes,
    facetError ? "" : needle,
    facets,
  );
  // The tree waits for both reads, so the Classifications row never arrives
  // under a list already shown; no group toggles until then.
  const treeLoading = root.loading || families.loading;
  const expandable = (treeLoading ? [] : nodes)
    .filter((node) => node.children.length > 0)
    .map((node) => node.id);
  // The search field's "N matches": the registers the filter keeps. No count
  // until the variable matches for this needle have arrived: the debounce has
  // not elapsed (a long enough needle has no facet key yet) or the request is
  // in flight, and a count from names alone would read low and announce "0".
  const facetsPending =
    facetResource.loading ||
    (facetKey === null && needle.trim().length >= SEARCH_MIN_QUERY_LENGTH);
  const matchCount =
    filtering && root.value !== null && !facetsPending && !facetError
      ? nodes
          .filter((node) => node.id !== CLASSIFICATIONS && matched.has(node.id))
          .reduce((sum, node) => sum + node.children.length, 0)
      : null;
  useEffect(() => {
    browseMatches.set(matchCount);
    return () => browseMatches.set(null);
  }, [matchCount]);
  // The kept registers in the tree's order, for the variables column (#1828).
  // FQIDs hold no space, so the joined list keys the effect.
  const order =
    filtering && !facetError
      ? nodes
          .filter((node) => node.id !== CLASSIFICATIONS)
          .flatMap((node) => node.children.map((child) => child.id))
          .join(" ")
      : "";
  useEffect(() => {
    browseOrder.set(order === "" ? [] : order.split(" "));
    return () => browseOrder.set([]);
  }, [order]);

  const hrefs = new Map(
    allNodes.flatMap((node) => [
      [node.id, node.href] as const,
      ...node.children.map((c) => [c.id, c.href] as const),
    ]),
  );

  const [collapsed, setCollapsed] = useState(readCollapsed);
  // While the filter holds text the tree has its own expansion: the groups with
  // a match open, then whatever the user toggles under that text. Clearing it
  // (Escape) gives back the remembered collapse state untouched.
  const [filterExpanded, setFilterExpanded] = useState<{
    needle: string;
    keys: Set<Key>;
  } | null>(null);
  // Cleared with the filter, so typing the same text again starts from its
  // matches rather than from the toggles made under it last time.
  if (!filtering && filterExpanded !== null) {
    setFilterExpanded(null);
  }
  const expandedKeys: Set<Key> =
    !filtering || facetError
      ? new Set(expandable.filter((id) => !collapsed.has(id)))
      : filterExpanded?.needle === needle
        ? filterExpanded.keys
        : matched;

  // RAC has no controlled focused key, and a focused key on a hidden row takes
  // the whole tree out of the Tab order; so every focus move goes through
  // `visibleKey`, on the tree's state. RAC creates that state inside `Tree` and
  // exposes it only to render props: the className render prop captures it (the
  // spike's capture). TreeStateContext does not reach item components, which
  // RAC renders in a hidden collection pass first, so it cannot be used.
  const treeState = useRef<TreeState | null>(null);
  const treeShown = root.value !== null;

  // A page opened from elsewhere (a link, the search bar, history) moves the
  // tree's focus to its row, or to the collapsed group hiding it, so Tab into
  // the list lands there and the arrows continue from it.
  useEffect(() => {
    const state = treeState.current;
    if (currentKey !== null && treeShown && state) {
      state.selectionManager.setFocusedKey(
        visibleKey(state, currentKey, state.expandedKeys),
      );
    }
  }, [currentKey, treeShown]);

  // The filter can take the focused row out of the tree (#1830), and a focused
  // key with no row takes the whole tree out of the Tab order. Move focus to
  // the current page's row when it shows, else clear it so Tab lands on the
  // tree. Every render: the check is a map lookup.
  useEffect(() => {
    const state = treeState.current;
    const focused = state?.selectionManager.focusedKey;
    if (!state || focused == null || state.collection.getItem(focused)) return;
    const target =
      currentKey === null
        ? null
        : visibleKey(state, currentKey, state.expandedKeys);
    state.selectionManager.setFocusedKey(
      target !== null && state.collection.getItem(target) ? target : null,
    );
  });

  // Selection follows the arrow keys (DESIGN.md → BrowseTree) and a click, and
  // selecting a row opens its page. Type-ahead moves focus only: React Aria's
  // type-select never selects.
  function onSelectionChange(keys: Selection): void {
    if (keys === "all") return;
    const [key] = keys;
    const href = key === undefined ? undefined : hrefs.get(String(key));
    if (href !== undefined && key !== currentKey) router.navigate(href);
  }

  // The register row stays selected on a page under it (a variable, a group),
  // so Tab into the tree lands there instead of selecting, and so opening, the
  // first row. RAC fires no selection change for a row already selected, so a
  // press on that row opens the register's own page here.
  function openCurrentRow(): void {
    const href = currentKey === null ? undefined : hrefs.get(currentKey);
    if (href !== undefined) router.navigate(href);
  }

  function onExpandedChange(keys: Set<Key>): void {
    keepFocusVisible(treeState.current, keys);
    if (filtering && !facetError) {
      setFilterExpanded({ needle, keys });
      return;
    }
    const next = new Set(expandable.filter((id) => !keys.has(id)));
    setCollapsed(next);
    writeCollapsed(next);
  }

  // A press on a collapsed group's row opens it as well as its page; it never
  // closes one: the chevron, ← and "Collapse all" own that (maintainer
  // 2026-11-05). Reads the tree's live expansion, not this render's, so a
  // chevron press that just collapsed the group is not undone.
  function expandGroup(id: string): void {
    const keys = treeState.current?.expandedKeys;
    if (!keys || keys.has(id)) return;
    onExpandedChange(new Set([...keys, id]));
  }

  // → on a register row (a leaf) moves to the variables column (DESIGN.md →
  // Layout), and so does → on the open provider row of the provider's own
  // page, whose variables the column lists; RAC's Tree uses → only to expand
  // a group row. Captured, so the Tree never sees a key it would ignore.
  function onKeyDownCapture(event: KeyboardEvent): void {
    const row = (event.target as Element).closest('[role="row"]');
    if (
      event.key === "ArrowRight" &&
      !event.altKey &&
      !event.metaKey &&
      !event.ctrlKey &&
      row !== null &&
      (!row.hasAttribute("aria-expanded") ||
        (row.getAttribute("aria-expanded") === "true" &&
          row.getAttribute("aria-current") === "page")) &&
      focusBrowseColumn("variables")
    ) {
      event.preventDefault();
      event.stopPropagation();
    }
  }

  return (
    <nav
      className={`${styles.column} ${className ?? ""}`}
      aria-label="Catalog"
      data-browse-column="registers"
      onKeyDownCapture={onKeyDownCapture}
    >
      <div className={styles.header}>
        <span className="micro-label">
          {/* While filtering, the label is how many registers the filter
            leaves: a separate count wrapped the header to two lines, out of
            line with the variables column's (#2107). */}
          {matchCount === null
            ? "Registers"
            : matchCount === 1
              ? "1 register"
              : `${matchCount} registers`}
        </span>
        {/* One toggle (maintainer-decided 2026-10-22): it collapses while any
          group is open, and expands once all are closed. With no group listed
          (a search that matches nothing) there is nothing to toggle. */}
        {expandable.length > 0 && (
          <Button
            variant="ghost"
            onPress={() =>
              onExpandedChange(
                expandedKeys.size > 0 ? new Set() : new Set(expandable),
              )
            }
          >
            {expandedKeys.size > 0 ? "Collapse all" : "Expand all"}
          </Button>
        )}
      </div>
      {/* The scroll box: the tree and the column's one selection bar. */}
      <div ref={boxRef} className={styles.scroller}>
        {treeLoading ? (
          <p className={styles.note} aria-busy="true">
            Loading the catalog…
          </p>
        ) : root.value === null ? (
          <StatusRow tone="error" live="alert" className={styles.error}>
            The register list did not load. Reload the page to try again.
          </StatusRow>
        ) : (
          <>
            {facetError && (
              <StatusRow tone="error" live="alert" className={styles.error}>
                Variable search failed ({facetError}). Showing all loaded
                registers. Clear the search and enter it again to retry.
              </StatusRow>
            )}
            <Tree
              // A fixed name: the visible label turns into the count while
              // filtering.
              aria-label={
                families.value?.length
                  ? "Registers and classifications"
                  : "Registers"
              }
              className={({ state }) => {
                treeState.current = state;
                return styles.tree;
              }}
              items={nodes}
              selectionMode="single"
              selectionBehavior="replace"
              selectedKeys={currentKey === null ? [] : [currentKey]}
              onSelectionChange={onSelectionChange}
              expandedKeys={expandedKeys}
              onExpandedChange={onExpandedChange}
              // Escape belongs to the search (DESIGN.md → Search filters in
              // place): the tree never clears its selection on it, so the key
              // reaches the page-wide handler in TopBar.
              escapeKeyBehavior="none"
              // Device-neutral copy: below the breakpoint this column is
              // the whole screen, often on a phone with no Escape key.
              renderEmptyState={() =>
                filtering &&
                !facetsPending &&
                !facetError && (
                  <p className={styles.note}>
                    No registers match “{needle.trim()}”. Clear the search to
                    see them all.
                  </p>
                )
              }
              dependencies={[
                counts,
                currentKey,
                current?.exact,
                needle,
                facetResource.data,
                facetError,
                facetsPending,
              ]}
            >
              {(node) => (
                <BrowseItem
                  node={node}
                  counts={counts}
                  current={current}
                  needle={needle}
                  onOpenCurrent={openCurrentRow}
                  onExpand={expandGroup}
                />
              )}
            </Tree>
            {/* Where the Classifications row would be: without it a failed
              read would look like a catalog with none. */}
            {families.value === null && (
              <StatusRow tone="error" live="alert" className={styles.error}>
                The classification list did not load. Reload the page to try
                again.
              </StatusRow>
            )}
          </>
        )}
        <span
          ref={barRef}
          className={columnStyles.selectionBar}
          aria-hidden="true"
          hidden
        />
      </div>
    </nav>
  );
}

const counted = (n: number, [one, many]: [string, string]) =>
  `${n} ${n === 1 ? one : many}`;

function BrowseItem({
  node,
  counts,
  current,
  needle,
  onOpenCurrent,
  onExpand,
}: {
  node: BrowseNode;
  counts: Record<string, number>;
  current: CurrentRow | null;
  needle: string;
  onOpenCurrent: () => void;
  onExpand: (id: string) => void;
}) {
  const count = inProjectCount(node, counts);
  // "page" only on the row whose own page is open; "true" on the register row
  // while a page under it (a variable, a group, its variants) is open.
  const ariaCurrent =
    current?.key !== node.id ? undefined : current.exact ? "page" : "true";
  // A group whose register is the open page: ← from the variables column
  // lands here when a collapse hides that register's row.
  const holdsCurrent = current?.key.startsWith(`${node.id}/`) ?? false;
  return (
    <TreeItem
      id={node.id}
      textValue={
        node.matchedOn === undefined
          ? node.name
          : `${node.name}, matched on ${node.matchedOn}`
      }
      className={`${styles.row} ${node.separated ? styles.separated : ""}`}
      onPress={
        ariaCurrent === "true"
          ? onOpenCurrent
          : node.children.length > 0
            ? () => onExpand(node.id)
            : undefined
      }
      render={(props) => (
        <div
          {...props}
          aria-current={ariaCurrent}
          data-holds-current={holdsCurrent || undefined}
        />
      )}
    >
      <TreeItemContent>
        {({ hasChildItems }) => (
          <>
            {hasChildItems ? (
              <AriaButton slot="chevron" className={styles.chevron}>
                <span className={styles.triangle} aria-hidden="true" />
              </AriaButton>
            ) : (
              <span className={styles.chevronSpacer} />
            )}
            <span className={styles.label}>
              <span className={styles.name}>
                <Highlight text={node.name} needle={needle} />
              </span>
              {/* A register kept only through its variables says so on its
                  own line (DESIGN.md → ColumnList → Filter state). */}
              {node.matchedOn !== undefined && (
                <span className={styles.matchedOn}>
                  matched on <b>{node.matchedOn}</b>
                </span>
              )}
            </span>
            <InProjectMark count={count} />
          </>
        )}
      </TreeItemContent>
      <Collection items={node.children}>
        {(child) => (
          <BrowseItem
            node={child}
            counts={counts}
            current={current}
            needle={needle}
            onOpenCurrent={onOpenCurrent}
            onExpand={onExpand}
          />
        )}
      </Collection>
    </TreeItem>
  );
}
