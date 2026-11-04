import { type RefObject, useEffect, useState } from "react";
import {
  Header,
  Menu,
  MenuItem,
  MenuSection,
  Popover,
} from "react-aria-components";
import {
  SEARCH_MIN_QUERY_LENGTH,
  type SearchHit,
  type SearchPage,
  type SearchType,
  search,
} from "./api";
import styles from "./CommandPalette.module.css";
import { catalogHref, rankFilter } from "./catalog";
import { router } from "./router";
import { type NodeType, TypeTag } from "./ui/TypeTag";
import { useAsyncResource } from "./use_async_resource";

/** One palette row: where it goes and how it reads. */
interface Row {
  id: string;
  label: string;
  /** An action's context, in the text face. */
  detail: string | null;
  href: string;
  type: NodeType | null;
}

/** What the palette offers besides the catalog: places in the app. Ranked with
 * the browse filter's own `rankFilter`, never RAC's. */
const ACTIONS: readonly Row[] = [
  {
    id: "action:project",
    label: "Go to the project",
    detail: "Study years, tables and columns",
    href: "/project",
    type: null,
  },
  {
    id: "action:browse",
    label: "Browse the data",
    detail: "Registers and their variables",
    href: "/catalog",
    type: null,
  },
  {
    id: "action:home",
    label: "Go to the start page",
    detail: null,
    href: "/",
    type: null,
  },
];

/** The arms the palette asks, in the order it lists them. Classifications
 * are reached through their variables (Operator 2026-11-04). */
const ARMS = ["register", "variable"] as const satisfies readonly SearchType[];
const ARM_HEADERS: Record<(typeof ARMS)[number], string> = {
  register: "Registers",
  variable: "Variables",
};
const ARM_LIMIT = 4;
const DEBOUNCE_MS = 150;

/** A search hit as a palette row: a name, and for a variable its register. */
function hitRow(hit: SearchHit): Omit<Row, "id"> | null {
  switch (hit.type) {
    case "register":
      return {
        label: hit.name,
        detail: null,
        href: catalogHref(hit.fqid),
        type: "register",
      };
    case "variable":
      return {
        label: hit.name,
        detail: hit.register_name,
        href: catalogHref(hit.fqid),
        type: "variable",
      };
    default:
      return null;
  }
}

/** One arm's rows: the catalog's hits, then the steward's own, each in the
 * server's order (never re-ranked together), once per place. */
function hitRows(page: SearchPage): Row[] {
  const seen = new Set<string>();
  const rows: Row[] = [];
  for (const hit of [...page.items, ...(page.steward?.items ?? [])]) {
    const row = hitRow(hit);
    if (!row || seen.has(row.href)) continue;
    seen.add(row.href);
    rows.push({ ...row, id: `${hit.type}:${row.href}` });
  }
  return rows;
}

/** The query the server sees, `DEBOUNCE_MS` after the last keystroke. */
function useDebounced(value: string): string {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setSettled(value), DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [value]);
  return settled;
}

interface Props {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  /** The top-bar search field it opens under, and whose text it searches. */
  triggerRef: RefObject<HTMLElement | null>;
  /** The popover, so the field can tell a blur into it from one away. */
  popoverRef: RefObject<HTMLElement | null>;
  /** The top-bar field's text: the palette has no field of its own. */
  text: string;
  /** A row was chosen: the palette closes, then the row navigates. */
  onChoose: () => void;
}

/** The command palette (DESIGN.md → Layout → Top bar): a `Menu` in a popover
 * under the top-bar search field, for registers, variables and the app's
 * places. It renders inside the top bar's RAC `Autocomplete`, so that field is
 * its one input: focus stays there and the arrow keys move a virtual focus
 * through the rows. The popover is non-modal for the same reason; a press
 * outside it and the field closes it, and so does focus leaving the field for
 * anything outside it (TopBar). Its results are a mixed list, so each row
 * carries its type. The list is controlled: the actions are ranked with
 * `rankFilter`, the catalog by the server, and RAC filters nothing. */
export function CommandPalette({
  isOpen,
  onOpenChange,
  triggerRef,
  popoverRef,
  text,
  onChoose,
}: Props) {
  // A non-modal popover does not dismiss itself on an outside press.
  useEffect(() => {
    if (!isOpen) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (
        target &&
        !popoverRef.current?.contains(target) &&
        !triggerRef.current?.contains(target)
      ) {
        onOpenChange(false);
      }
    };
    document.addEventListener("pointerdown", onPointerDown, true);
    return () =>
      document.removeEventListener("pointerdown", onPointerDown, true);
  }, [isOpen, onOpenChange, popoverRef, triggerRef]);

  return (
    <Popover
      ref={popoverRef}
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      triggerRef={triggerRef}
      isNonModal
      placement="bottom start"
      offset={4}
      // RAC writes its own z-index inline; the palette sits at the popover
      // layer (DESIGN.md → Elevation), under a dialog's scrim.
      style={{ zIndex: "var(--z-popover)" }}
      className={({ isEntering, isExiting }) =>
        `${styles.popover} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
      }
    >
      <PaletteBody text={text} choose={onChoose} />
    </Popover>
  );
}

function PaletteBody({ text, choose }: { text: string; choose: () => void }) {
  const query = useDebounced(text.trim());
  const asked = query.length >= SEARCH_MIN_QUERY_LENGTH ? query : null;
  const results = useAsyncResource(asked, (signal) =>
    Promise.all(
      ARMS.map((type) => search(query, { signal, type, limit: ARM_LIMIT })),
    ),
  );

  const actions = rankFilter([...ACTIONS], text.trim(), (a) => [
    a.label,
    a.detail,
  ]);
  const arms = ARMS.map((type, i) => ({
    type,
    rows: results.data?.[i] ? hitRows(results.data[i]) : [],
  }));
  const hits = arms.flatMap((arm) => arm.rows);
  const byId = new Map([...actions, ...hits].map((row) => [row.id, row]));
  const typing = text.trim() !== query;
  const pending =
    text.trim().length >= SEARCH_MIN_QUERY_LENGTH &&
    (typing || results.loading);

  let status: string | null = null;
  if (results.error) {
    status = `Search failed: ${results.error}`;
  } else if (pending) {
    status = "Searching…";
  } else if (
    text.trim().length > 0 &&
    text.trim().length < SEARCH_MIN_QUERY_LENGTH
  ) {
    status = `Type ${SEARCH_MIN_QUERY_LENGTH} characters to search.`;
  } else if (asked && hits.length === 0) {
    status = `Nothing matches “${asked}”.`;
  }

  return (
    <div className={styles.body}>
      <output
        className={`${styles.status} ${results.error ? "error" : ""}`}
        aria-busy={pending || undefined}
      >
        {results.error && <span aria-hidden="true">✕ </span>}
        {status}
      </output>
      <Menu
        aria-label="Registers, variables and places"
        className={styles.menu}
        onAction={(id) => {
          const row = byId.get(String(id));
          if (!row) return;
          choose();
          router.navigate(row.href);
        }}
      >
        {actions.length > 0 && (
          <MenuSection className={styles.section}>
            <Header className={`micro-label ${styles.header}`}>Go to</Header>
            {actions.map((row) => (
              <PaletteItem key={row.id} row={row} />
            ))}
          </MenuSection>
        )}
        {arms.map(
          (arm) =>
            arm.rows.length > 0 && (
              <MenuSection key={arm.type} className={styles.section}>
                <Header className={`micro-label ${styles.header}`}>
                  {ARM_HEADERS[arm.type]}
                </Header>
                {arm.rows.map((row) => (
                  <PaletteItem key={row.id} row={row} />
                ))}
              </MenuSection>
            ),
        )}
      </Menu>
    </div>
  );
}

function PaletteItem({ row }: { row: Row }) {
  return (
    <MenuItem id={row.id} textValue={row.label} className={styles.item}>
      <span className={styles.label}>{row.label}</span>
      {row.detail && <span className={styles.detail}>{row.detail}</span>}
      {row.type && (
        <span className={styles.type}>
          <TypeTag type={row.type} />
        </span>
      )}
    </MenuItem>
  );
}
