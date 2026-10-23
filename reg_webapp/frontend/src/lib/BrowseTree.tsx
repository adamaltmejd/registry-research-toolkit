import { type KeyboardEvent, useEffect, useId, useRef, useState } from "react";
import {
  Button as AriaButton,
  Collection,
  type Key,
  type Selection,
  Tree,
  TreeItem,
  TreeItemContent,
  TreeLoadMoreItem,
} from "react-aria-components";
import {
  getShow,
  type RegisterChild,
  type RootChild,
  SEARCH_MIN_QUERY_LENGTH,
  searchFacets,
} from "./api";
import styles from "./BrowseTree.module.css";
import {
  browseMatches,
  browseOrder,
  useBrowseFilter,
  useSettledFilter,
} from "./browse_filter";
import { catalogHref, leafSlug, rankFilter, registerPrefixOf } from "./catalog";
import { catalogRootChildren } from "./catalog_names";
import { Highlight } from "./Highlight";
import { safeSourceBindings, safeSourceSlots } from "./project_data";
import { projectStore } from "./project_store";
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
import { useCatalogRead, useProjectRead } from "./use_store_read";

/** One row: a provider (or the classification root) at level 1, a register under
 * it. `children` is null while a provider's registers are still loading. */
interface BrowseNode {
  id: string;
  name: string;
  href: string;
  children: BrowseNode[] | null;
  /** Set on a register the filter keeps only through its variables: how many. */
  variableMatches?: number;
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

/** Columns in the open project, per register FQID. */
function readInProject(): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const source of safeSourceSlots(projectStore.draft?.sources)) {
    const variant = source?.register_variant;
    const register =
      typeof variant === "string" ? registerPrefixOf(variant) : "";
    if (register) {
      counts[register] =
        (counts[register] ?? 0) + safeSourceBindings(source).length;
    }
  }
  return counts;
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
 * group or variants page lives under its register, a classification page under
 * the classification root. */
function currentRow(route: Route): CurrentRow | null {
  switch (route.name) {
    case "catalog-node": {
      const [first, second] = route.fqidPath.split("/");
      const key =
        first === "class" || second === undefined
          ? first
          : `${first}/${second}`;
      return { key, exact: key === route.fqidPath };
    }
    case "group":
    case "variants":
      return { key: `${route.provider}/${route.register}`, exact: false };
    case "class-group":
      return { key: "class", exact: false };
    default:
      return null;
  }
}

/** The browse filter applied to the tree. A group keeps the registers that
 * match by name or slug, then those that match only through their variables
 * (`facets`, per register FQID), each with its count; or all of them when its
 * own name matches. Those groups are `matched`. A group with no match is left
 * out until the filter clears (#1830), and so is a leaf row (the
 * classification root) whose own name does not match. A provider whose
 * registers are still loading stays, as its loader row. */
function filterNodes(
  nodes: BrowseNode[],
  needle: string,
  facets: ReadonlyMap<string, number>,
): { nodes: BrowseNode[]; matched: Set<Key> } {
  const matched = new Set<Key>();
  if (needle.trim() === "") return { nodes, matched };
  const named = (node: BrowseNode) => [node.name, leafSlug(node.id)];
  const filtered = nodes.flatMap((node) => {
    if (node.children === null) return [node];
    if (rankFilter([node], needle, named).length > 0) {
      if (node.children.length > 0) matched.add(node.id);
      return [node];
    }
    const byName = rankFilter(node.children, needle, named);
    const byVariables = node.children
      .filter((child) => !byName.includes(child) && facets.has(child.id))
      .map((child) => ({ ...child, variableMatches: facets.get(child.id) }));
    const children = [...byName, ...byVariables];
    if (children.length === 0) return [];
    matched.add(node.id);
    return [{ ...node, children }];
  });
  return { nodes: filtered, matched };
}

function browseNodes(
  root: RootChild[],
  registers: Record<string, RegisterChild[]> | null,
): BrowseNode[] {
  return root.map((child) => ({
    id: child.fqid,
    name: child.name ?? child.fqid,
    href: catalogHref(child.fqid),
    children:
      child.kind !== "provider"
        ? []
        : registers === null
          ? null
          : (registers[child.fqid] ?? []).map((register) => ({
              id: register.fqid,
              name: register.name ?? register.fqid,
              href: catalogHref(register.fqid),
              children: [],
            })),
  }));
}

/**
 * The first browse column (DESIGN.md → Layout): every register, grouped under a
 * sticky provider row. Selection follows the arrow keys and a click, and
 * selecting a row opens its page; the current page's row carries the selection
 * bar. Rows are not links (RAC's selectable rows are not `<a>`). Every group
 * starts open; a group's chevron, ← / →, and the
 * header's one "Collapse all" / "Expand all" toggle fold them, remembered per user.
 * Collapsing a group never moves the page: the detail pane keeps showing the
 * register it showed, and keyboard focus moves to the group row.
 */
export function BrowseTree({ className }: { className?: string }) {
  const labelId = useId();
  const boxRef = useRef<HTMLDivElement>(null);
  const barRef = useRef<HTMLSpanElement>(null);
  useSelectionBar(boxRef, barRef);
  const route = useRoute();
  const root = useCatalogRead(catalogRootChildren, undefined);
  const counts = useProjectRead(readInProject, undefined);
  const providers = (root.value ?? []).filter((c) => c.kind === "provider");
  // One request per provider, all at once: every group starts open.
  const registers = useAsyncResource(
    root.value === null ? null : providers.map((p) => p.fqid).join(" "),
    async (signal) => {
      const shown = await Promise.all(
        providers.map((p) => getShow(p.fqid, { signal })),
      );
      return Object.fromEntries(
        shown.map((node, i) => [
          providers[i].fqid,
          node.kind === "provider" ? node.children : [],
        ]),
      );
    },
  );
  const needle = useBrowseFilter();
  const filtering = needle.trim() !== "";
  // The variable matches per register, asked once the text rests.
  const facetKey = useSettledFilter();
  const facetResource = useAsyncResource(facetKey, (signal) =>
    searchFacets(facetKey ?? "", { signal }),
  );
  const facets = new Map(
    (facetKey === null ? [] : (facetResource.data ?? [])).flatMap((f) =>
      f.fqid && f.variable_matches > 0
        ? [[f.fqid, f.variable_matches] as const]
        : [],
    ),
  );
  const allNodes = browseNodes(
    root.value ?? [],
    registers.loading ? null : (registers.data ?? {}),
  );
  const { nodes, matched } = filterNodes(allNodes, needle, facets);
  const expandable = nodes
    .filter((node) => node.children === null || node.children.length > 0)
    .map((node) => node.id);
  // The search field's "N matches": the registers the filter keeps. No count
  // until the variable matches for this needle have arrived: the debounce has
  // not elapsed (a long enough needle has no facet key yet) or the request is
  // in flight, and a count from names alone would read low and announce "0".
  const facetsPending =
    facetResource.loading ||
    (facetKey === null && needle.trim().length >= SEARCH_MIN_QUERY_LENGTH);
  const matchCount =
    filtering && !registers.loading && root.value !== null && !facetsPending
      ? nodes
          .filter((node) => matched.has(node.id))
          .reduce((sum, node) => sum + (node.children?.length ?? 0), 0)
      : null;
  useEffect(() => {
    browseMatches.set(matchCount);
    return () => browseMatches.set(null);
  }, [matchCount]);
  // The kept registers in the tree's order, for the variables column (#1828).
  // FQIDs hold no space, so the joined list keys the effect.
  const order = filtering
    ? nodes
        .flatMap((node) => (node.children ?? []).map((child) => child.id))
        .join(" ")
    : "";
  useEffect(() => {
    browseOrder.set(order === "" ? [] : order.split(" "));
    return () => browseOrder.set([]);
  }, [order]);

  const current = currentRow(route);
  const currentKey = current?.key ?? null;
  const hrefs = new Map(
    allNodes.flatMap((node) => [
      [node.id, node.href] as const,
      ...(node.children ?? []).map((c) => [c.id, c.href] as const),
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
  const expandedKeys: Set<Key> = !filtering
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
  const treeShown = root.value !== null && !registers.loading;

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
    if (filtering) {
      setFilterExpanded({ needle, keys });
      return;
    }
    const next = new Set(expandable.filter((id) => !keys.has(id)));
    setCollapsed(next);
    writeCollapsed(next);
  }

  // → on a register row (a leaf) moves to the variables column (DESIGN.md →
  // Layout); RAC's Tree uses → only on a group row. Captured, so the Tree
  // never sees a key it would ignore.
  function onKeyDownCapture(event: KeyboardEvent): void {
    const row = (event.target as Element).closest('[role="row"]');
    if (
      event.key === "ArrowRight" &&
      !event.altKey &&
      !event.metaKey &&
      !event.ctrlKey &&
      row !== null &&
      !row.hasAttribute("aria-expanded") &&
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
        <span id={labelId} className="micro-label">
          Registers
        </span>
        {/* How many registers the filter leaves: an actionable count. */}
        {matchCount !== null && (
          <span className={styles.matchCount}>
            {matchCount === 1 ? "1 match" : `${matchCount} matches`}
          </span>
        )}
        {/* One toggle (maintainer-decided 2026-10-22): it collapses while any
            group is open, and expands once all are closed. */}
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
      </div>
      {/* The scroll box: the tree and the column's one selection bar. */}
      <div ref={boxRef} className={styles.scroller}>
        {root.loading ? (
          <p className={styles.note} aria-busy="true">
            Loading the catalog…
          </p>
        ) : root.value === null ? (
          <StatusRow tone="error" live="alert" className={styles.error}>
            The register list did not load. Reload the page to try again.
          </StatusRow>
        ) : (
          <>
            {registers.error && (
              <StatusRow tone="error" live="alert" className={styles.error}>
                Some registers did not load ({registers.error}). Reload the page
                to try again.
              </StatusRow>
            )}
            <Tree
              aria-labelledby={labelId}
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
                filtering && (
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
              ]}
            >
              {(node) => (
                <BrowseItem
                  node={node}
                  counts={counts}
                  current={current}
                  needle={needle}
                  onOpenCurrent={openCurrentRow}
                />
              )}
            </Tree>
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

const matchedCount = (n: number) => (n === 1 ? "1 variable" : `${n} variables`);

function BrowseItem({
  node,
  counts,
  current,
  needle,
  onOpenCurrent,
}: {
  node: BrowseNode;
  counts: Record<string, number>;
  current: CurrentRow | null;
  needle: string;
  onOpenCurrent: () => void;
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
        node.variableMatches === undefined
          ? node.name
          : `${node.name}, matched on ${matchedCount(node.variableMatches)}`
      }
      className={styles.row}
      onPress={ariaCurrent === "true" ? onOpenCurrent : undefined}
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
              {node.variableMatches !== undefined && (
                <span className={styles.variableMatches}>
                  matched on <b>{matchedCount(node.variableMatches)}</b>
                </span>
              )}
            </span>
            <InProjectMark count={count} />
          </>
        )}
      </TreeItemContent>
      {/* RAC 1.22.1 ignores `hasChildItems` on a parent with no child rows (no
          aria-expanded, → does nothing), so a provider whose registers are
          still loading keeps a loader row as its child. */}
      {node.children === null ? (
        <TreeLoadMoreItem isLoading className={styles.loading}>
          <span aria-busy="true">Loading registers…</span>
        </TreeLoadMoreItem>
      ) : (
        <Collection items={node.children}>
          {(child) => (
            <BrowseItem
              node={child}
              counts={counts}
              current={current}
              needle={needle}
              onOpenCurrent={onOpenCurrent}
            />
          )}
        </Collection>
      )}
    </TreeItem>
  );
}
