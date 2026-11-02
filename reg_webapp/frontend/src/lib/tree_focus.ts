import type { Key, TreeRenderProps } from "react-aria-components";

/** A RAC tree's state, captured from its `className` render prop: RAC creates it
 * inside `Tree` and exposes it only there. */
export type TreeState = TreeRenderProps["state"];

/** The row that shows `key` under `expanded`: the key itself, or its outermost
 * collapsed ancestor. RAC 1.22 has no controlled focused key, and focusing a
 * hidden row takes every row out of the Tab order (RAC keeps the focused key's
 * tabIndex only), so a collapse moves focus here first. */
export function visibleKey(
  state: TreeState,
  key: Key,
  expanded: ReadonlySet<Key>,
): Key {
  let target = key;
  let node = state.collection.getItem(key);
  while (node?.parentKey != null) {
    if (!expanded.has(node.parentKey)) target = node.parentKey;
    node = state.collection.getItem(node.parentKey);
  }
  return target;
}

/** Before an expansion change lands, move the tree's focus off a row it is about
 * to hide (see `visibleKey`). Call it first in the one `onExpandedChange`. */
export function keepFocusVisible(
  state: TreeState | null,
  expanded: ReadonlySet<Key>,
): void {
  const focused = state?.selectionManager.focusedKey;
  if (state && focused != null) {
    const target = visibleKey(state, focused, expanded);
    if (target !== focused) state.selectionManager.setFocusedKey(target);
  }
}
