import {
  type ComponentProps,
  type KeyboardEvent,
  type ReactNode,
  useEffect,
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
  /** Opens a page outside the list's register (another register's match,
   * #1828): on click or Enter only, never as the arrow keys pass, so walking
   * the list does not re-root it under the reader. */
  leaves?: boolean;
}

/** A run of items under a facet header (a group's members, DESIGN.md →
 * ColumnList), or under another register's header while the search bar holds
 * text (`register`, #1828). A flat list is one section with no label. */
export interface ColumnSection {
  id: string;
  label: string | null;
  register?: boolean;
  items: readonly ColumnItem[];
}

interface Props {
  label: string;
  /** Whose items these are (a register's or a group's name): the header reads
   * "<label> in <subject>", truncated, with the whole of it as hover text. */
  subject?: string;
  /** Whose list it is (the open register's FQID): a change re-roots it. */
  root?: string;
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
  /** Shown above the list when the first run is empty but later ones are not
   * (other registers' matches, #1828). */
  notice?: string | null;
  /** Shown in place of the list when the items did not load. */
  error: string | null;
  className?: string;
}

/** The keys that open the focused row; any other key (the arrows, a
 * type-ahead letter) moves focus, and selection with it. */
const OPEN_KEYS = new Set(["Enter", " "]);

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
  subject,
  root,
  sections,
  selectedId,
  selectedExact,
  renderName,
  empty,
  notice = null,
  error,
  className,
}: Props) {
  const labelId = useId();
  const boxRef = useRef<HTMLDivElement>(null);
  const barRef = useRef<HTMLSpanElement>(null);
  useSelectionBar(boxRef, barRef);
  const items = sections?.flatMap((s) => s.items) ?? null;
  // Set while a key that walks the list is down: a leaving row opens on a
  // click, Enter or Space, not as the arrows or type-ahead pass it.
  const walking = useRef(false);
  // Set to the list's root when a leaving row opens its page: the list
  // re-roots under another register and its rows render in steps (a row
  // focused early can unmount, the register's own rows can come last), so
  // while nothing, or another register's row, has focus, focus goes to the
  // selected row or the first own one, until the reader next presses a key
  // or the pointer.
  const refocus = useRef<string | null>(null);
  useEffect(() => {
    if (refocus.current === null || refocus.current === root) return;
    const target = selectedId ?? items?.find((i) => !i.leaves)?.id;
    const active = document.activeElement;
    const box = boxRef.current;
    if (target === undefined || !box) return;
    const leavingFocused =
      active instanceof HTMLElement &&
      box.contains(active) &&
      items?.some((i) => i.leaves && i.id === active.dataset.key);
    if (active !== document.body && !leavingFocused) return;
    box
      .querySelector<HTMLElement>(
        `[role="option"][data-key="${CSS.escape(target)}"]`,
      )
      ?.focus();
  });
  useEffect(() => {
    const done = () => {
      refocus.current = null;
    };
    document.addEventListener("keydown", done, true);
    document.addEventListener("pointerdown", done, true);
    return () => {
      document.removeEventListener("keydown", done, true);
      document.removeEventListener("pointerdown", done, true);
    };
  }, []);

  function onSelectionChange(keys: Selection): void {
    if (keys === "all") return;
    const [key] = keys;
    const item = items?.find((i) => i.id === key);
    const current = items?.find((i) => i.id === selectedId);
    if (item?.leaves && walking.current) return;
    // Two rows can open one page (a variable that is several group members).
    if (item !== undefined && item.href !== current?.href) {
      refocus.current = item.leaves ? (root ?? null) : null;
      router.navigate(item.href);
    }
  }

  // ← in the list returns to the register column; captured, so the ListBox
  // (which ignores ← in a vertical list) never sees it.
  function onKeyDownCapture(event: KeyboardEvent): void {
    walking.current = !OPEN_KEYS.has(event.key);
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
      // RAC selects inside the keydown, so the key's release ends the walk:
      // a later activation without keys (a screen reader's) opens the row.
      onKeyUpCapture={() => {
        walking.current = false;
      }}
      onPointerDownCapture={() => {
        walking.current = false;
      }}
    >
      <div className={styles.header}>
        <span
          id={labelId}
          className={`micro-label ${styles.label}`}
          title={subject === undefined ? undefined : `${label} in ${subject}`}
        >
          {subject === undefined ? label : `${label} in ${subject}`}
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
          <>
            {notice !== null && <p className={styles.note}>{notice}</p>}
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
                      <Header
                        className={
                          section.register ? styles.register : styles.facet
                        }
                        title={section.register ? section.label : undefined}
                      >
                        {section.label}
                      </Header>
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
          </>
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
