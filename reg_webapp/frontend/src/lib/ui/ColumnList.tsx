import { type ComponentProps, type ReactNode, useId } from "react";
import { ListBox, ListBoxItem, type Selection } from "react-aria-components";
import { router } from "../router";
import styles from "./ColumnList.module.css";
import { StatusRow } from "./StatusRow";

export interface ColumnItem {
  /** Unique across the list: a FQID, or `group:<key>`. */
  id: string;
  name: string;
  href: string;
  /** Columns of this item in the open project; 0 shows no mark. */
  inProject: number;
}

interface Props {
  label: string;
  /** Null while the items load. */
  items: readonly ColumnItem[] | null;
  /** The item whose page is open, or null. */
  selectedId: string | null;
  /** True when that page is the item's own, not one under it. */
  selectedExact: boolean;
  /** An item's visible name: plain, or with the filter's match marked. */
  renderName: (item: ColumnItem) => ReactNode;
  /** Shown in place of the list when there are no items. */
  empty: string;
  /** Shown in place of the list when the items did not load. */
  error: string | null;
  className?: string;
}

/**
 * A browse column (DESIGN.md → ColumnList): a `label` header, then the items,
 * the current page's selected. Selection follows the arrow keys and a click,
 * and selecting an item opens its page; type-ahead moves focus only. Items are
 * not links (RAC's selectable options are not `<a>`).
 */
export function ColumnList({
  label,
  items,
  selectedId,
  selectedExact,
  renderName,
  empty,
  error,
  className,
}: Props) {
  const labelId = useId();

  function onSelectionChange(keys: Selection): void {
    if (keys === "all") return;
    const [key] = keys;
    const item = items?.find((i) => i.id === key);
    if (item !== undefined && item.id !== selectedId) {
      router.navigate(item.href);
    }
  }

  return (
    <nav
      className={`${styles.column} ${className ?? ""}`}
      aria-labelledby={labelId}
    >
      <div className={styles.header}>
        <span id={labelId} className="micro-label">
          {label}
        </span>
      </div>
      {error !== null ? (
        <StatusRow tone="error" live="alert" className={styles.error}>
          {error}
        </StatusRow>
      ) : items === null ? (
        <p className={styles.note} aria-busy="true">
          Loading {label.toLowerCase()}…
        </p>
      ) : (
        <ListBox
          aria-labelledby={labelId}
          className={styles.list}
          items={items}
          selectionMode="single"
          selectionBehavior="replace"
          disallowEmptySelection
          selectedKeys={selectedId === null ? [] : [selectedId]}
          onSelectionChange={onSelectionChange}
          dependencies={[renderName, selectedId, selectedExact]}
          renderEmptyState={() => <p className={styles.note}>{empty}</p>}
        >
          {(item) => (
            <ListBoxItem
              id={item.id}
              textValue={item.name}
              className={styles.row}
              render={(props) => (
                <div
                  // Items carry no href, so RAC renders a div, never a link.
                  {...(props as ComponentProps<"div">)}
                  aria-current={
                    item.id !== selectedId
                      ? undefined
                      : selectedExact
                        ? "page"
                        : "true"
                  }
                />
              )}
            >
              <span className={styles.name}>{renderName(item)}</span>
              <InProjectMark count={item.inProject} />
            </ListBoxItem>
          )}
        </ListBox>
      )}
    </nav>
  );
}

/** The in-project mark on a browse row: a 6px accent dot and the count. */
export function InProjectMark({ count }: { count: number }) {
  if (count === 0) return null;
  return (
    <span className={styles.inProject}>
      <span className={styles.dot} aria-hidden="true" />
      {count}
      <span className="visually-hidden"> in the project</span>
    </span>
  );
}
