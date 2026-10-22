import { useEffect, useId, useRef, useState } from "react";
import {
  Button,
  Collection,
  type Key,
  type Selection,
  Tree,
  TreeItem,
  TreeItemContent,
  TreeLoadMoreItem,
  type TreeRenderProps,
} from "react-aria-components";
import { getShow, type RegisterChild, type RootChild } from "./api";
import styles from "./BrowseTree.module.css";
import { useBrowseFilter } from "./browse_filter";
import { catalogHref, leafSlug, rankFilter, registerPrefixOf } from "./catalog";
import { catalogRootChildren } from "./catalog_names.svelte";
import { Highlight } from "./Highlight";
import { safeSourceBindings, safeSourceSlots } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { type Route, router } from "./router";
import { useRune } from "./svelte_bridge.svelte";
import { InProjectMark } from "./ui/ColumnList";
import { useAsyncResource } from "./use_async_resource";
import { useRoute } from "./use_router";

/** One row: a provider (or the classification root) at level 1, a register under
 * it. `children` is null while a provider's registers are still loading. */
interface BrowseNode {
  id: string;
  name: string;
  href: string;
  children: BrowseNode[] | null;
}

type TreeState = TreeRenderProps["state"];

/** The row that shows `key` under `expanded`: the key itself, or its outermost
 * collapsed ancestor. Focusing a hidden row takes every row out of the Tab order
 * (RAC keeps the focused key's tabIndex only), so focus goes here instead. */
function visibleKey(state: TreeState, key: Key, expanded: Set<Key>): Key {
  let target = key;
  let node = state.collection.getItem(key);
  while (node?.parentKey != null) {
    if (!expanded.has(node.parentKey)) target = node.parentKey;
    node = state.collection.getItem(node.parentKey);
  }
  return target;
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
 * match by name or slug, or all of them when its own name matches; those groups
 * are `matched`. A group with no match keeps its whole list, so it still shows
 * and still opens, and starts closed.
 * simplify: a register that matches only through its variables is not shown.
 * DESIGN.md wants it shown "with the count", which needs a per-register count
 * of variable matches for a query that the API does not serve (GET /api/search
 * pages ranked hits, no facet). Add it when the server answers that count. */
function filterNodes(
  nodes: BrowseNode[],
  needle: string,
): { nodes: BrowseNode[]; matched: Set<Key> } {
  const matched = new Set<Key>();
  if (needle.trim() === "") return { nodes, matched };
  const named = (node: BrowseNode) => [node.name, leafSlug(node.id)];
  const filtered = nodes.map((node) => {
    if (node.children === null || node.children.length === 0) return node;
    if (rankFilter([node], needle, named).length > 0) {
      matched.add(node.id);
      return node;
    }
    const children = rankFilter(node.children, needle, named);
    if (children.length === 0) return node;
    matched.add(node.id);
    return { ...node, children };
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
  const route = useRoute();
  const root = useRune(catalogRootChildren, undefined);
  const counts = useRune(readInProject, undefined);
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
  const allNodes = browseNodes(
    root.value ?? [],
    registers.loading ? null : (registers.data ?? {}),
  );
  const { nodes, matched } = filterNodes(allNodes, needle);
  const expandable = nodes
    .filter((node) => node.children === null || node.children.length > 0)
    .map((node) => node.id);

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

  // Selection follows the arrow keys (DESIGN.md → BrowseTree) and a click, and
  // selecting a row opens its page. Type-ahead moves focus only: React Aria's
  // type-select never selects.
  function onSelectionChange(keys: Selection): void {
    if (keys === "all") return;
    const [key] = keys;
    const href = key === undefined ? undefined : hrefs.get(String(key));
    if (href !== undefined && key !== currentKey) router.navigate(href);
  }

  function onExpandedChange(keys: Set<Key>): void {
    const state = treeState.current;
    const focused = state?.selectionManager.focusedKey;
    if (state && focused != null) {
      const target = visibleKey(state, focused, keys);
      if (target !== focused) state.selectionManager.setFocusedKey(target);
    }
    if (filtering) {
      setFilterExpanded({ needle, keys });
      return;
    }
    const next = new Set(expandable.filter((id) => !keys.has(id)));
    setCollapsed(next);
    writeCollapsed(next);
  }

  return (
    <nav className={`${styles.column} ${className ?? ""}`} aria-label="Catalog">
      <div className={styles.header}>
        <span id={labelId} className="micro-label">
          Registers
        </span>
        {/* One toggle (maintainer-decided 2026-10-22): it collapses while any
            group is open, and expands once all are closed. */}
        <Button
          className="ghost-button"
          onPress={() =>
            onExpandedChange(
              expandedKeys.size > 0 ? new Set() : new Set(expandable),
            )
          }
        >
          {expandedKeys.size > 0 ? "Collapse all" : "Expand all"}
        </Button>
      </div>
      {root.loading ? (
        <p className={styles.note} aria-busy="true">
          Loading the catalog…
        </p>
      ) : root.value === null ? (
        <p className={`banner-error ${styles.error}`} role="alert">
          <span aria-hidden="true">✕</span> The register list did not load.
          Reload the page to try again.
        </p>
      ) : (
        <>
          {registers.error && (
            <p className={`banner-error ${styles.error}`} role="alert">
              <span aria-hidden="true">✕</span> Some registers did not load (
              {registers.error}). Reload the page to try again.
            </p>
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
            dependencies={[counts, currentKey, current?.exact, needle]}
          >
            {(node) => (
              <BrowseItem
                node={node}
                counts={counts}
                current={current}
                needle={needle}
              />
            )}
          </Tree>
        </>
      )}
    </nav>
  );
}

function BrowseItem({
  node,
  counts,
  current,
  needle,
}: {
  node: BrowseNode;
  counts: Record<string, number>;
  current: CurrentRow | null;
  needle: string;
}) {
  const count = inProjectCount(node, counts);
  // "page" only on the row whose own page is open; "true" on the register row
  // while a page under it (a variable, a group, its variants) is open.
  const ariaCurrent =
    current?.key !== node.id ? undefined : current.exact ? "page" : "true";
  return (
    <TreeItem
      id={node.id}
      textValue={node.name}
      className={styles.row}
      render={(props) => <div {...props} aria-current={ariaCurrent} />}
    >
      <TreeItemContent>
        {({ hasChildItems }) => (
          <>
            {hasChildItems ? (
              <Button slot="chevron" className={styles.chevron}>
                <Chevron />
              </Button>
            ) : (
              <span className={styles.chevronSpacer} />
            )}
            <span className={styles.name}>
              <Highlight text={node.name} needle={needle} />
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
            />
          )}
        </Collection>
      )}
    </TreeItem>
  );
}

function Chevron() {
  return (
    <svg viewBox="0 0 16 16" width="12" height="12" aria-hidden="true">
      <path
        d="M6 3l5 5-5 5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
      />
    </svg>
  );
}
