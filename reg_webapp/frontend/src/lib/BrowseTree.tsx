import { useId, useRef, useState } from "react";
import {
  Button,
  Collection,
  type Key,
  Link,
  NavigationTree,
  NavigationTreeItem,
  NavigationTreeItemContent,
  type NavigationTreeRenderProps,
  TreeLoadMoreItem,
} from "react-aria-components";
import { getShow, type RegisterChild, type RootChild } from "./api";
import styles from "./BrowseTree.module.css";
import { catalogHref, registerPrefixOf } from "./catalog";
import { catalogRootChildren } from "./catalog_names.svelte";
import { safeSourceBindings, safeSourceSlots } from "./project_data";
import { projectStore } from "./project_store.svelte";
import type { Route } from "./router";
import { useRune } from "./svelte_bridge.svelte";
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

type TreeState = NavigationTreeRenderProps["state"];

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

/** The row the current page belongs to, as that row's href: a variable, group or
 * variants page lives under its register, a classification page under the
 * classification root. */
function browseHref(route: Route): string | null {
  switch (route.name) {
    case "catalog-node": {
      const [first, second] = route.fqidPath.split("/");
      return catalogHref(
        first === "class" || second === undefined
          ? first
          : `${first}/${second}`,
      );
    }
    case "group":
    case "variants":
      return catalogHref(`${route.provider}/${route.register}`);
    case "class-group":
      return catalogHref("class");
    default:
      return null;
  }
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
 * sticky provider row, as a navigation tree. Rows are links; arrow keys move
 * focus, Enter or a click opens the page, and the current page's row carries the
 * selection bar. Every group starts open; a group's chevron, ← / →, and the
 * header's "Collapse all" / "Expand all" fold them, remembered per user.
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
  const nodes = browseNodes(
    root.value ?? [],
    registers.loading ? null : (registers.data ?? {}),
  );
  const expandable = nodes
    .filter((node) => node.children === null || node.children.length > 0)
    .map((node) => node.id);

  const [collapsed, setCollapsed] = useState(readCollapsed);
  const expandedKeys = new Set(expandable.filter((id) => !collapsed.has(id)));

  // RAC has no controlled focused key. If a collapse hides the focused row, the
  // tree keeps pointing at it, renders every row out of the Tab order and drops
  // out of the page's Tab sequence; so move focus to the row's visible group
  // first. The state comes from the className render prop.
  const treeState = useRef<TreeState | null>(null);

  function onExpandedChange(keys: Set<Key>): void {
    const manager = treeState.current?.selectionManager;
    const focused =
      manager?.focusedKey != null
        ? treeState.current?.collection.getItem(manager.focusedKey)
        : null;
    if (focused?.parentKey != null && !keys.has(focused.parentKey)) {
      manager?.setFocusedKey(focused.parentKey);
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
        <Button
          className={styles.ghost}
          onPress={() => onExpandedChange(new Set())}
        >
          Collapse all
        </Button>
        <Button
          className={styles.ghost}
          onPress={() => onExpandedChange(new Set(expandable))}
        >
          Expand all
        </Button>
      </div>
      {root.loading ? (
        <p className={styles.note} aria-busy="true">
          Loading the catalog…
        </p>
      ) : root.value === null ? (
        <p className={styles.error} role="alert">
          The register list did not load. Reload the page to try again.
        </p>
      ) : (
        <>
          {registers.error && (
            <p className={styles.error} role="alert">
              Some registers did not load ({registers.error}). Reload the page
              to try again.
            </p>
          )}
          <NavigationTree
            aria-labelledby={labelId}
            className={({ state }) => {
              treeState.current = state;
              return styles.tree;
            }}
            items={nodes}
            selectedRoute={browseHref(route)}
            expandedKeys={expandedKeys}
            onExpandedChange={onExpandedChange}
            dependencies={[counts]}
          >
            {(node) => <BrowseItem node={node} counts={counts} />}
          </NavigationTree>
        </>
      )}
    </nav>
  );
}

function BrowseItem({
  node,
  counts,
}: {
  node: BrowseNode;
  counts: Record<string, number>;
}) {
  const count = inProjectCount(node, counts);
  return (
    <NavigationTreeItem
      id={node.id}
      href={node.href}
      textValue={node.name}
      className={styles.row}
    >
      <NavigationTreeItemContent>
        {({ hasChildItems }) => (
          <>
            {hasChildItems ? (
              <Button slot="chevron" className={styles.chevron}>
                <Chevron />
              </Button>
            ) : (
              <span className={styles.chevronSpacer} />
            )}
            <Link className={styles.link}>{node.name}</Link>
            {count > 0 && (
              <span className={styles.inProject}>
                <span className={styles.dot} aria-hidden="true" />
                {count}
                <span className="visually-hidden"> in the project</span>
              </span>
            )}
          </>
        )}
      </NavigationTreeItemContent>
      {/* RAC 1.22.1 ignores `hasChildItems` on a parent with no child rows (no
          aria-expanded, → does nothing), so a provider whose registers are
          still loading keeps a loader row as its child. */}
      {node.children === null ? (
        <TreeLoadMoreItem isLoading className={styles.loading}>
          <span aria-busy="true">Loading registers…</span>
        </TreeLoadMoreItem>
      ) : (
        <Collection items={node.children}>
          {(child) => <BrowseItem node={child} counts={counts} />}
        </Collection>
      )}
    </NavigationTreeItem>
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
