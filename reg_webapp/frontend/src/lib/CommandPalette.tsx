import { type RefObject, useEffect, useRef, useState } from "react";
import {
  Autocomplete,
  Dialog,
  Header,
  Input,
  Menu,
  MenuItem,
  MenuSection,
  Popover,
  SearchField,
} from "react-aria-components";
import {
  type CodeHit,
  type ConceptGroupHit,
  SEARCH_MIN_QUERY_LENGTH,
  type SearchHit,
  type SearchType,
  search,
} from "./api";
import styles from "./CommandPalette.module.css";
import {
  catalogHref,
  classGroupHref,
  fqidSegments,
  groupHref,
  rankFilter,
} from "./catalog";
import { router } from "./router";
import { type NodeType, TypeTag } from "./ui/TypeTag";
import { useAsyncResource } from "./use_async_resource";

/** One palette row: where it goes and how it reads. */
interface Row {
  id: string;
  label: string;
  /** An action's context, in the text face. */
  detail: string | null;
  /** A catalog hit's identifier, in mono beside its name. */
  fqid: string | null;
  href: string;
  type: NodeType | null;
}

/** What the palette offers besides the catalog: places in the app. Ranked with
 * the browse filter's own `rankFilter`, never RAC's. */
const ACTIONS: readonly Row[] = [
  {
    id: "action:project",
    label: "Go to the project",
    detail: "Sources, columns and the order",
    fqid: null,
    href: "/project",
    type: null,
  },
  {
    id: "action:browse",
    label: "Browse the data",
    detail: "Registers and their variables",
    fqid: null,
    href: "/catalog",
    type: null,
  },
  {
    id: "action:home",
    label: "Go to the start page",
    detail: null,
    fqid: null,
    href: "/",
    type: null,
  },
];

/** The arms the palette asks, in the order it lists them: the in-place filter
 * already finds registers and variables (DESIGN.md → Layout). */
const ARMS: readonly SearchType[] = [
  "classification",
  "classification_code",
  "register_value",
];
const ARM_LIMIT = 5;
const DEBOUNCE_MS = 150;

/** A variable concept group's page lives under its register, so it has one only
 * when every member shares that register; a classification group always has one. */
function conceptGroupHref(hit: ConceptGroupHit): string | null {
  if (hit.kind === "classification") {
    const [head, key] = fqidSegments(hit.key);
    return classGroupHref(head === "class" && key ? key : hit.key);
  }
  const registers = new Set(
    hit.members.map((m) => fqidSegments(m.fqid).slice(0, 2).join("/")),
  );
  if (registers.size !== 1) return null;
  const [registerFqid] = registers;
  const [provider, register, key] = fqidSegments(hit.key);
  return groupHref(
    registerFqid,
    key && `${provider}/${register}` === registerFqid ? key : hit.key,
  );
}

/** A code has no page of its own: it goes to the classification that defines
 * it, else to the first variable that uses it; `fqid` names that target. */
function codeTarget(hit: CodeHit): string | null {
  const owner =
    hit.classifications.find(
      (c) => c.fqid != null && (c.short_name ?? c.name) === hit.code_system,
    ) ?? hit.classifications.find((c) => c.fqid != null);
  const fqid = owner?.fqid ?? hit.variables.find((v) => v.fqid)?.fqid;
  return fqid ?? null;
}

/** A search hit as a palette row, or null for a hit with nowhere to go. */
function hitRow(hit: SearchHit): Omit<Row, "id"> | null {
  switch (hit.type) {
    case "group": {
      const href = conceptGroupHref(hit);
      return href
        ? {
            label: hit.label,
            detail: null,
            fqid: hit.key,
            href,
            type: "group",
          }
        : null;
    }
    case "classification":
    case "classification_succession":
      return hit.fqid
        ? {
            label: hit.name ?? hit.short_name ?? hit.fqid,
            detail: null,
            fqid: hit.fqid,
            href: catalogHref(hit.fqid),
            type: "classification",
          }
        : null;
    case "code": {
      const fqid = codeTarget(hit);
      return fqid
        ? {
            label: `${hit.code} ${hit.label}`,
            detail: null,
            fqid,
            href: catalogHref(fqid),
            type: "codeList",
          }
        : null;
    }
    default:
      // Registers and variables are the in-place filter's, not the palette's.
      return null;
  }
}

/** The rows for a page of hits, keyed by content (with an occurrence suffix:
 * the same hit can come back twice under two arms). */
function hitRows(hits: readonly SearchHit[]): Row[] {
  const seen = new Map<string, number>();
  const rows: Row[] = [];
  for (const hit of hits) {
    const row = hitRow(hit);
    if (!row) continue;
    const base = `${hit.type}:${row.href}:${row.label}`;
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    rows.push({ ...row, id: n === 0 ? base : `${base}#${n}` });
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
  /** The top-bar field it opens under; focus returns there on close. */
  triggerRef: RefObject<HTMLElement | null>;
  /** The text the palette starts with: the top-bar field's. */
  seed: string;
}

/** The command palette (DESIGN.md → Layout → Top bar): a RAC `Autocomplete`
 * over a `Menu`, in a popover under the search bar, for codes, classifications
 * and the app's places. Its results are a mixed list, so each row carries its
 * type. The list is controlled: the actions are ranked with `rankFilter`, the
 * catalog by the server, and RAC filters nothing. */
export function CommandPalette({
  isOpen,
  onOpenChange,
  triggerRef,
  seed,
}: Props) {
  return (
    <Popover
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      triggerRef={triggerRef}
      placement="bottom start"
      offset={4}
      // RAC writes its own z-index inline; the palette sits at the popover
      // layer (DESIGN.md → Elevation), under a dialog's scrim.
      style={{ zIndex: "var(--z-popover)" }}
      className={({ isEntering, isExiting }) =>
        `${styles.popover} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
      }
    >
      <Dialog aria-label="Command palette" className={styles.dialog}>
        {/* Mounted per opening, so each one starts from the field's text. */}
        <PaletteBody seed={seed} close={() => onOpenChange(false)} />
      </Dialog>
    </Popover>
  );
}

function PaletteBody({ seed, close }: { seed: string; close: () => void }) {
  const [text, setText] = useState(seed);
  const input = useRef<HTMLInputElement>(null);
  // Opening the palette is the user's own act, so its field takes focus; the
  // Dialog would otherwise focus itself.
  useEffect(() => input.current?.focus(), []);
  const query = useDebounced(text.trim());
  const asked = query.length >= SEARCH_MIN_QUERY_LENGTH ? query : null;
  const results = useAsyncResource(asked, async (signal) =>
    (
      await Promise.all(
        ARMS.map((type) => search(query, { signal, type, limit: ARM_LIMIT })),
      )
    ).flatMap((page) => page.items),
  );

  const actions = rankFilter([...ACTIONS], text.trim(), (a) => [
    a.label,
    a.detail,
  ]);
  const hits = hitRows(results.data ?? []);
  const byId = new Map([...actions, ...hits].map((row) => [row.id, row]));
  const typing = text.trim() !== query;
  const pending =
    text.trim().length >= SEARCH_MIN_QUERY_LENGTH &&
    (typing || results.loading);

  let status: string | null = null;
  if (results.error) {
    status = `Search failed: ${results.error}`;
  } else if (pending) {
    status = "Searching classifications and codes…";
  } else if (
    text.trim().length > 0 &&
    text.trim().length < SEARCH_MIN_QUERY_LENGTH
  ) {
    status = `Type ${SEARCH_MIN_QUERY_LENGTH} characters to search classifications and codes.`;
  } else if (asked && hits.length === 0) {
    status = `No classification or code matches “${asked}”.`;
  }

  return (
    <Autocomplete inputValue={text} onInputChange={setText}>
      <SearchField
        aria-label="Search classifications and codes"
        className={styles.field}
        // One Escape closes the palette; SearchField would spend the first
        // one clearing its text.
        onKeyDown={(event) => {
          if (event.key === "Escape") close();
        }}
      >
        <Input
          ref={input}
          className={styles.input}
          placeholder="Classifications, codes, value sets…"
          autoComplete="off"
        />
      </SearchField>
      <output
        className={`${styles.status} ${results.error ? "error" : ""}`}
        aria-busy={pending || undefined}
      >
        {results.error && <span aria-hidden="true">✕ </span>}
        {status}
      </output>
      <Menu
        aria-label="Results"
        className={styles.menu}
        onAction={(id) => {
          const row = byId.get(String(id));
          if (!row) return;
          close();
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
        {hits.length > 0 && (
          <MenuSection className={styles.section}>
            <Header className={`micro-label ${styles.header}`}>
              Classifications and codes
            </Header>
            {hits.map((row) => (
              <PaletteItem key={row.id} row={row} />
            ))}
          </MenuSection>
        )}
      </Menu>
    </Autocomplete>
  );
}

function PaletteItem({ row }: { row: Row }) {
  return (
    <MenuItem id={row.id} textValue={row.label} className={styles.item}>
      <span className={styles.label}>{row.label}</span>
      {row.detail && <span className={styles.detail}>{row.detail}</span>}
      {row.fqid && <span className={styles.fqid}>{row.fqid}</span>}
      {row.type && (
        <span className={styles.type}>
          <TypeTag type={row.type} />
        </span>
      )}
    </MenuItem>
  );
}
