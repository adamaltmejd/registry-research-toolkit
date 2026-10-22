import { useEffect, useEffectEvent, useRef } from "react";
import {
  Button as AriaButton,
  Collection,
  type Key,
  Tree,
  TreeItem,
  TreeItemContent,
  TreeLoadMoreItem,
} from "react-aria-components";
import type { ClassificationCodeModel } from "./api";
import styles from "./CodeList.module.css";
import type { CodeTreeModel, Level } from "./code_tree";
import { keepFocusVisible, type TreeState } from "./tree_focus";
import { Button } from "./ui/Button";
import { Skeleton } from "./ui/Skeleton";
import { TreeChevron } from "./ui/TreeChevron";

// Past this many skeleton rows the 18rem scroll is full; more reserve nothing.
const MAX_RESERVED = 20;

/** A row of the tree: a code, with its children once its level is read. */
interface CodeNode {
  id: string;
  code: ClassificationCodeModel;
}

function nodesOf(level: Level | undefined): CodeNode[] {
  return level?.status === "ok"
    ? level.rows.map((code) => ({ id: code.code, code }))
    : [];
}

/**
 * A classification's codes as a tree (#1297): the roots, each node opening onto
 * its children, as a RAC `Tree` (arrow keys move, ← / → close and open, one Tab
 * stop). Codes are not selectable; the tree is for reading. A node with
 * children issues no request until it opens. While a level loads its node
 * reserves one skeleton row per child it announced, so the list below does not
 * jump (#1139).
 */
export function CodeTree({
  tree,
  label,
}: {
  tree: CodeTreeModel;
  /** The tree's accessible name. */
  label: string;
}) {
  const { levels, expanded, setExpanded, load } = tree;
  const roots = levels.get("");
  const treeState = useRef<TreeState | null>(null);

  // The roots are read when the tree first shows; `load` skips a level already
  // read or in flight, so StrictMode's second run asks for nothing.
  const loadRoots = useEffectEvent(() => load(""));
  useEffect(() => loadRoots(), []);

  function onExpandedChange(keys: Set<Key>): void {
    keepFocusVisible(treeState.current, keys);
    for (const key of keys) {
      if (!expanded.has(key)) load(String(key));
    }
    setExpanded(keys);
  }

  if (roots === undefined || roots.status === "loading") {
    return (
      <div className={styles.treeLoading} aria-busy="true">
        <span className="visually-hidden">Loading codes…</span>
        <Skeleton count={3} />
      </div>
    );
  }
  if (roots.status === "error") {
    return (
      <div className={styles.status} role="alert">
        <p className="error">
          <span aria-hidden="true">✕</span> Could not load codes: {roots.error}
        </p>
        <Button size="sm" onPress={() => load("")}>
          Retry
        </Button>
      </div>
    );
  }
  if (roots.rows.length === 0) {
    return <p className={styles.empty}>This classification has no codes.</p>;
  }
  return (
    <Tree
      aria-label={label}
      className={({ state }) => {
        treeState.current = state;
        return styles.tree;
      }}
      items={nodesOf(roots)}
      selectionMode="none"
      expandedKeys={expanded}
      onExpandedChange={onExpandedChange}
      dependencies={[levels]}
    >
      {(node) => <CodeItem node={node} levels={levels} load={load} />}
    </Tree>
  );
}

function CodeItem({
  node,
  levels,
  load,
}: {
  node: CodeNode;
  levels: ReadonlyMap<string, Level>;
  load: (parent: string) => void;
}) {
  const { code } = node;
  const childCount = code.child_count ?? 0;
  const level = childCount > 0 ? levels.get(code.code) : undefined;
  return (
    <TreeItem
      id={node.id}
      textValue={`${code.code} ${code.label}`}
      className={styles.treeRow}
    >
      <TreeItemContent>
        {({ hasChildItems }) => (
          <>
            {hasChildItems ? (
              <AriaButton slot="chevron" className={styles.chevron}>
                <TreeChevron />
              </AriaButton>
            ) : (
              <span className={styles.chevronSpacer} />
            )}
            <code className={styles.key}>{code.code}</code>
            <span className={styles.label}>
              {code.label}
              {childCount > 0 && (
                <span className={styles.childCount}>
                  {" "}
                  ({childCount}
                  <span className="visually-hidden">
                    {childCount === 1 ? " code" : " codes"}
                  </span>
                  )
                </span>
              )}
            </span>
          </>
        )}
      </TreeItemContent>
      {childCount > 0 && (
        <Children
          parent={code.code}
          expected={childCount}
          level={level}
          levels={levels}
          load={load}
        />
      )}
    </TreeItem>
  );
}

/** A node's child rows by the state of its level. Until the level is read, a
 * loader stands in: RAC 1.22.1 ignores `hasChildItems` on a parent with no child
 * rows (no aria-expanded, → does nothing), and a loader counts as one while
 * rendering nothing but a sentinel until it is loading. */
function Children({
  parent,
  expected,
  level,
  levels,
  load,
}: {
  parent: string;
  expected: number;
  level: Level | undefined;
  levels: ReadonlyMap<string, Level>;
  load: (parent: string) => void;
}) {
  if (level === undefined || level.status === "loading") {
    const reserved = Array.from(
      { length: Math.min(expected, MAX_RESERVED) },
      (_, i) => `reserve-${i}`,
    );
    return (
      <TreeLoadMoreItem
        isLoading={level?.status === "loading"}
        className={styles.treeLoader}
      >
        <div className={styles.treeLoading} aria-busy="true">
          <span className="visually-hidden">Loading codes…</span>
          {reserved.map((id) => (
            <span key={id} className={styles.reserve}>
              <Skeleton width="60%" />
            </span>
          ))}
        </div>
      </TreeLoadMoreItem>
    );
  }
  if (level.status === "error") {
    // A row, not a button inside the loader: the loader is not focusable, so
    // a Retry inside it would be out of keyboard reach. Enter on the row retries.
    return (
      <TreeItem
        id={`${parent}\u0000error`}
        textValue={`Could not load the codes under ${parent}: ${level.error}. Retry`}
        className={`${styles.treeRow} ${styles.treeError}`}
        onAction={() => load(parent)}
      >
        <TreeItemContent>
          <span className={styles.chevronSpacer} />
          <span className={styles.label}>
            <span aria-hidden="true">✕</span> Could not load the codes under{" "}
            {parent}: {level.error}. <span className={styles.retry}>Retry</span>
          </span>
        </TreeItemContent>
      </TreeItem>
    );
  }
  if (level.rows.length === 0) {
    return (
      <TreeItem
        id={`${parent}\u0000empty`}
        textValue={`No codes under ${parent}`}
        className={`${styles.treeRow} ${styles.treeNote}`}
      >
        <TreeItemContent>
          <span className={styles.chevronSpacer} />
          <span className={styles.label}>No codes under {parent}.</span>
        </TreeItemContent>
      </TreeItem>
    );
  }
  return (
    <Collection items={nodesOf(level)}>
      {(child) => <CodeItem node={child} levels={levels} load={load} />}
    </Collection>
  );
}
