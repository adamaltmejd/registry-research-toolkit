import {
  type ComponentProps,
  type KeyboardEvent,
  type ReactNode,
  useId,
  useRef,
} from "react";
import {
  Collection,
  Header,
  ListBox,
  ListBoxItem,
  ListBoxSection,
  type Selection,
} from "react-aria-components";
import { router } from "../router";
import styles from "./ColumnList.module.css";
import { focusBrowseColumn } from "./column_keys";
import { StatusRow } from "./StatusRow";
import { useSelectionBar } from "./selection_bar";

export interface ColumnItem {
  /** Unique across the list: a FQID, or `group:<key>`. */
  id: string;
  name: string;
  /** The option's accessible name where `name` repeats across sections (a
   * member under its facet header); the visible text stays `name`. */
  label?: string;
  href: string;
  /** Columns of this item in the open project; 0 shows no mark. */
  inProject: number;
}

/** A run of items under a facet header (a group's members, DESIGN.md →
 * ColumnList). A flat list is one section with no label. */
export interface ColumnSection {
  id: string;
  label: string | null;
  items: readonly ColumnItem[];
}

interface Props {
  label: string;
  /** Null while the items load. */
  sections: readonly ColumnSection[] | null;
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
 * The second browse column (DESIGN.md → ColumnList): a `label` header, then the
 * items, the current page's selected, under `mono` facet headers when the list
 * is a group's members. Selection follows the arrow keys and a click, and
 * selecting an item opens its page; type-ahead moves focus only. Items are not
 * links (RAC's selectable options are not `<a>`). ← moves back to the register
 * column.
 */
export function ColumnList({
  label,
  sections,
  selectedId,
  selectedExact,
  renderName,
  empty,
  error,
  className,
}: Props) {
  const labelId = useId();
  const boxRef = useRef<HTMLDivElement>(null);
  const barRef = useRef<HTMLSpanElement>(null);
  useSelectionBar(boxRef, barRef);
  const items = sections?.flatMap((s) => s.items) ?? null;

  function onSelectionChange(keys: Selection): void {
    if (keys === "all") return;
    const [key] = keys;
    const item = items?.find((i) => i.id === key);
    const current = items?.find((i) => i.id === selectedId);
    // Two rows can open one page (a variable that is several group members).
    if (item !== undefined && item.href !== current?.href) {
      router.navigate(item.href);
    }
  }

  // ← in the list returns to the register column; captured, so the ListBox
  // (which ignores ← in a vertical list) never sees it.
  function onKeyDownCapture(event: KeyboardEvent): void {
    if (
      event.key === "ArrowLeft" &&
      !event.altKey &&
      !event.metaKey &&
      !event.ctrlKey &&
      (event.target as Element).closest('[role="option"]') &&
      focusBrowseColumn("registers")
    ) {
      event.preventDefault();
      event.stopPropagation();
    }
  }

  const option = (item: ColumnItem) => (
    <ListBoxItem
      key={item.id}
      id={item.id}
      textValue={item.label ?? item.name}
      aria-label={item.label}
      className={styles.row}
      render={(props) => (
        <div
          // Items carry no href, so RAC renders a div, never a link.
          {...(props as ComponentProps<"div">)}
          aria-current={
            item.id !== selectedId ? undefined : selectedExact ? "page" : "true"
          }
        />
      )}
    >
      <span className={styles.name}>{renderName(item)}</span>
      <InProjectMark count={item.inProject} />
    </ListBoxItem>
  );

  return (
    <nav
      className={`${styles.column} ${className ?? ""}`}
      aria-labelledby={labelId}
      data-browse-column="variables"
      onKeyDownCapture={onKeyDownCapture}
    >
      <div className={styles.header}>
        <span id={labelId} className="micro-label">
          {label}
        </span>
      </div>
      {/* The scroll box: the list and the column's one selection bar. */}
      <div ref={boxRef} className={styles.scroller}>
        {error !== null ? (
          <StatusRow tone="error" live="alert" className={styles.error}>
            {error}
          </StatusRow>
        ) : sections === null ? (
          <p className={styles.note} aria-busy="true">
            Loading {label.toLowerCase()}…
          </p>
        ) : (
          <ListBox
            aria-labelledby={labelId}
            className={styles.list}
            selectionMode="single"
            selectionBehavior="replace"
            disallowEmptySelection
            selectedKeys={selectedId === null ? [] : [selectedId]}
            onSelectionChange={onSelectionChange}
            renderEmptyState={() => <p className={styles.note}>{empty}</p>}
          >
            {/* A flat list is plain options; only labelled runs get a section
              (an unnamed group would be noise to a screen reader). */}
            {sections.map((section) =>
              section.label === null ? (
                <Collection
                  key={section.id}
                  items={section.items}
                  dependencies={[renderName, selectedId, selectedExact]}
                >
                  {option}
                </Collection>
              ) : (
                section.items.length > 0 && (
                  <ListBoxSection key={section.id} id={section.id}>
                    <Header className={styles.facet}>{section.label}</Header>
                    <Collection
                      items={section.items}
                      dependencies={[renderName, selectedId, selectedExact]}
                    >
                      {option}
                    </Collection>
                  </ListBoxSection>
                )
              ),
            )}
          </ListBox>
        )}
        <span
          ref={barRef}
          className={styles.selectionBar}
          aria-hidden="true"
          hidden
        />
      </div>
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
