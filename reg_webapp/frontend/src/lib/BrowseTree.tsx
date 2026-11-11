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
import type { ClassFamilyRef, RootChild } from "./api";
import styles from "./BrowseTree.module.css";
import { catalogHref, registerOfPath, sortedRegisters } from "./catalog";
import {
  CLASSIFICATIONS,
  CLASSIFICATIONS_LABEL,
  catalogRootChildren,
  classFamiliesRead,
} from "./catalog_names";
import { type Route, router } from "./router";
import { keepFocusVisible, type TreeState, visibleKey } from "./tree_focus";
import { Button } from "./ui/Button";
import { InProjectMark } from "./ui/ColumnList";
import columnStyles from "./ui/ColumnList.module.css";
import { focusBrowseColumn } from "./ui/column_keys";
import { StatusRow } from "./ui/StatusRow";
import { useSelectionBar } from "./ui/selection_bar";
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
  /** Drawn under a rule that sets it apart from the rows above. */
  separated?: boolean;
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
  // classifications themselves are the next column's items.
  const classifications: BrowseNode = {
    id: CLASSIFICATIONS,
    name: CLASSIFICATIONS_LABEL,
    href: catalogHref(CLASSIFICATIONS),
    separated: true,
    children: [],
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
  const current = currentRow(route);
  const currentKey = current?.key ?? null;
  const families = useCatalogRead(classFamiliesRead, undefined);
  const nodes = browseNodes(root.value ?? [], families.value ?? []);
  // The tree waits for both reads, so the Classifications row never arrives
  // under a list already shown; no group toggles until then.
  const treeLoading = root.loading || families.loading;
  const expandable = (treeLoading ? [] : nodes)
    .filter((node) => node.children.length > 0)
    .map((node) => node.id);

  const hrefs = new Map(
    nodes.flatMap((node) => [
      [node.id, node.href] as const,
      ...node.children.map((c) => [c.id, c.href] as const),
    ]),
  );

  const [collapsed, setCollapsed] = useState(readCollapsed);
  const expandedKeys: Set<Key> = new Set(
    expandable.filter((id) => !collapsed.has(id)),
  );

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
        <span className="micro-label">Registers</span>
        {/* One toggle (maintainer-decided 2026-10-22): it collapses while any
          group is open, and expands once all are closed. */}
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
            <Tree
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
              // The selection is the open page: Escape never clears it.
              escapeKeyBehavior="none"
              dependencies={[counts, currentKey, current?.exact]}
            >
              {(node) => (
                <BrowseItem
                  node={node}
                  counts={counts}
                  current={current}
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

function BrowseItem({
  node,
  counts,
  current,
  onOpenCurrent,
  onExpand,
}: {
  node: BrowseNode;
  counts: Record<string, number>;
  current: CurrentRow | null;
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
      textValue={node.name}
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
            <span className={styles.name}>{node.name}</span>
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
            onOpenCurrent={onOpenCurrent}
            onExpand={onExpand}
          />
        )}
      </Collection>
    </TreeItem>
  );
}
