/**
 * Application state store, written as a class so Svelte 5 runes pick
 * up the field-level `$state` declarations. Components import `store`,
 * read `store.snapshot` etc. reactively, and call mutators that
 * encapsulate the 409/stale-state recovery loop.
 */

import {
  ApiError,
  ApiStaleState,
  getState as fetchState,
  initProject as apiInitProject,
  listRegisters,
  setColumnType as apiSetColumnType,
  setGroupRegister as apiSetGroupRegister,
  unsetColumnManual as apiUnsetColumnManual,
  type SetColumnTypeArgs,
  type SetGroupRegisterArgs,
  type UnsetColumnManualArgs,
} from "./api";
import type {
  ColumnInfo,
  ColumnType,
  RegisterEntry,
  StateSnapshot,
} from "./types";

/** Concern flags surfaced by the filter chips. Each picks a different
 * subset of cells the user is likely to want to review:
 *  - manual:    user-edited types (the audit trail)
 *  - mismatch:  auto-classified types that disagree with regmeta
 *  - unmatched: categoricals with no regmeta evidence (best-effort)
 *  - opaque:    free-text classifier fallback (commonly worth reviewing)
 */
export type ConcernFilter = "manual" | "mismatch" | "unmatched" | "opaque";

/** Predicate checks pulled out of GroupCard so the FilterBar can use the
 * same definitions for its counts. Keeping these in the store module
 * avoids a circular import between the filter UI and the row renderer. */
export function columnIsManual(c: ColumnInfo): boolean {
  return c.provenance === "manual";
}

export function columnIsMismatch(c: ColumnInfo): boolean {
  if (c.provenance === "manual") return false;
  if (c.regmeta_implied_type === null) return false;
  return c.regmeta_implied_type !== c.current_type;
}

export function columnIsUnmatchedCategorical(c: ColumnInfo): boolean {
  if (c.current_type !== "categorical") return false;
  const sig = c.regmeta_signal;
  if (!sig) return true;
  return !sig.classification_short_name && !sig.has_value_codes;
}

export function columnHasConcern(
  c: ColumnInfo,
  concern: ConcernFilter,
): boolean {
  switch (concern) {
    case "manual":
      return columnIsManual(c);
    case "mismatch":
      return columnIsMismatch(c);
    case "unmatched":
      return columnIsUnmatchedCategorical(c);
    case "opaque":
      return c.current_type === "opaque";
  }
}

export interface Toast {
  id: number;
  level: "info" | "warning" | "error";
  message: string;
}

/** Distinguish "config exists but couldn't load" (real error) from
 * "config not initialised yet" (which the empty-state UI handles). */
export type LoadState =
  | { kind: "loading" }
  | { kind: "ready" }
  | { kind: "uninitialised" }
  | { kind: "error"; message: string };

const TOAST_TIMEOUT_MS = 6000;
// Cap on visible toasts. Older ones are dropped when this is exceeded so
// a flurry of stale-state errors during a contentious editing session
// doesn't pile up over the page.
const MAX_TOASTS = 4;

// localStorage keys for per-browser view preferences. Per-browser, not
// per-project — these are UI affordances, not project state, so they
// don't belong in mock_data_config.json.
const VIEW_PREF_KEY = "mdw.web.groupColumnsByName";
const COLUMNS_PREF_KEY = "mdw.web.visibleColumns";

function loadGroupingPref(): boolean {
  if (typeof localStorage === "undefined") return true;
  // localStorage access can throw SecurityError in privacy-restricted
  // or opaque-origin contexts; falling back to the default keeps app
  // startup from blowing up before the UI renders. The write path
  // already swallows the same error.
  try {
    const raw = localStorage.getItem(VIEW_PREF_KEY);
    if (raw === null) return true; // default: grouped
    return raw === "true";
  } catch {
    return true;
  }
}

function saveGroupingPref(value: boolean): void {
  if (typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(VIEW_PREF_KEY, String(value));
  } catch {
    // Storage quota / private mode — failing silently here is fine; the
    // toggle still works in-session.
  }
}

/** Optional columns in the per-group column table. The "name" column is
 * always rendered. Defaults: sql hidden (raw discover types are noise
 * for most reviewers), type and coverage shown. */
export type OptionalColumnId = "sql" | "type" | "coverage";

export interface VisibleColumns {
  sql: boolean;
  type: boolean;
  coverage: boolean;
}

const DEFAULT_VISIBLE_COLUMNS: VisibleColumns = {
  sql: false,
  type: true,
  coverage: true,
};

function loadVisibleColumns(): VisibleColumns {
  if (typeof localStorage === "undefined") return { ...DEFAULT_VISIBLE_COLUMNS };
  try {
    const raw = localStorage.getItem(COLUMNS_PREF_KEY);
    if (raw === null) return { ...DEFAULT_VISIBLE_COLUMNS };
    const parsed = JSON.parse(raw) as Partial<VisibleColumns>;
    // Spread defaults first so a stored payload missing a key (e.g.
    // upgrade adds a new column id) keeps the new column at its default
    // rather than silently disappearing.
    return { ...DEFAULT_VISIBLE_COLUMNS, ...parsed };
  } catch {
    return { ...DEFAULT_VISIBLE_COLUMNS };
  }
}

function saveVisibleColumns(value: VisibleColumns): void {
  if (typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(COLUMNS_PREF_KEY, JSON.stringify(value));
  } catch {
    // Storage quota / private mode — non-fatal.
  }
}

class Store {
  snapshot: StateSnapshot | null = $state(null);
  registers: RegisterEntry[] | null = $state(null);
  /** True when ``listRegisters`` failed (regmeta unavailable etc.).
   * RegisterEditor uses this to soften client-side validation: when we
   * can't enumerate valid names, manual entry must still be permitted
   * and the server stays the source of truth. */
  registersUnavailable = $state(false);
  toasts: Toast[] = $state([]);
  loadState: LoadState = $state({ kind: "loading" });
  busy = $state(false);
  /** Bumped whenever stale-state recovery refreshes the snapshot.
   * Open modals subscribe to this and self-close when it changes — their
   * pre-mutation snapshot is no longer trustworthy. */
  staleRecoveryTick = $state(0);
  /** UI view: collapse same-named agreeing columns within a register
   * into one row, badged "× N". Persisted to localStorage so it
   * survives reloads. Default true. */
  groupColumnsByName = $state(loadGroupingPref());

  /** Optional column visibility in the per-group table. The "name"
   * column is always shown; the rest are toggleable via ColumnsPicker.
   * Persisted to localStorage. */
  visibleColumns: VisibleColumns = $state(loadVisibleColumns());

  /** Free-text filter on column name. Substring, case-insensitive.
   * Session-scoped — losing the filter on reload is the right default
   * (otherwise a stale filter from yesterday silently hides everything
   * on tomorrow's project). */
  filterQuery = $state("");
  /** Single column-type filter, or null for "all". */
  filterType: ColumnType | null = $state(null);
  /** Single concern filter, or null for "all". Mutually exclusive with
   * itself: clicking the active chip clears it. Combining chips would
   * give an empty intersection on most projects. */
  filterConcern: ConcernFilter | null = $state(null);

  private nextToastId = 1;
  private toastTimers = new Map<number, ReturnType<typeof setTimeout>>();

  async load(): Promise<void> {
    try {
      this.snapshot = await fetchState();
      this.loadState = { kind: "ready" };
    } catch (exc) {
      if (exc instanceof ApiError && exc.code === "not_initialized") {
        this.snapshot = null;
        this.loadState = { kind: "uninitialised" };
        return;
      }
      const message = exc instanceof Error ? exc.message : String(exc);
      this.loadState = { kind: "error", message };
    }
  }

  async init(): Promise<void> {
    // Reusing the mutation `busy` flag: from the empty-state UI no
    // mutation path is reachable (groups don't render until loaded),
    // so init can't race with setColumnType / setGroupRegister. The
    // shared flag also disables the Initialise button on double-click.
    if (this.busy) return;
    this.busy = true;
    try {
      this.snapshot = await apiInitProject();
      this.loadState = { kind: "ready" };
      this.pushToast("info", "Project initialised. Review the auto-classified columns below.");
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : String(exc);
      this.pushToast("error", `Could not initialise: ${message}`);
    } finally {
      this.busy = false;
    }
  }

  /** Lazy-load on first popover open; idempotent. */
  async ensureRegisters(): Promise<void> {
    if (this.registers !== null) return;
    try {
      const response = await listRegisters();
      this.registers = response.registers;
      this.registersUnavailable = false;
    } catch (exc) {
      // Regmeta unavailable; surface an empty list rather than
      // blocking the modal, but warn once so a broken regmeta install
      // is visible (the autocomplete would otherwise look intentional).
      // The flag is what RegisterEditor reads to skip client-side
      // name-resolution; without it, the empty list would block every
      // manual name and contradict the toast we're about to push.
      this.registers = [];
      this.registersUnavailable = true;
      const message = exc instanceof Error ? exc.message : String(exc);
      this.pushToast(
        "warning",
        `Could not load register list: ${message}. Type the register name manually.`,
      );
    }
  }

  async setColumnType(args: SetColumnTypeArgs): Promise<boolean> {
    return this.runMutation(() => apiSetColumnType(args));
  }

  async unsetColumnManual(args: UnsetColumnManualArgs): Promise<boolean> {
    return this.runMutation(() => apiUnsetColumnManual(args));
  }

  async setGroupRegister(args: SetGroupRegisterArgs): Promise<boolean> {
    return this.runMutation(() => apiSetGroupRegister(args));
  }

  pushToast(level: Toast["level"], message: string): void {
    const id = this.nextToastId++;
    let next = [...this.toasts, { id, level, message }];
    while (next.length > MAX_TOASTS) {
      const dropped = next.shift();
      if (dropped) {
        const timer = this.toastTimers.get(dropped.id);
        if (timer !== undefined) {
          clearTimeout(timer);
          this.toastTimers.delete(dropped.id);
        }
      }
    }
    this.toasts = next;
    const timer = setTimeout(() => {
      this.toastTimers.delete(id);
      this.toasts = this.toasts.filter((t) => t.id !== id);
    }, TOAST_TIMEOUT_MS);
    this.toastTimers.set(id, timer);
  }

  setGroupColumnsByName(value: boolean): void {
    this.groupColumnsByName = value;
    saveGroupingPref(value);
  }

  toggleColumnVisibility(id: OptionalColumnId): void {
    this.visibleColumns[id] = !this.visibleColumns[id];
    saveVisibleColumns(this.visibleColumns);
  }

  setFilterQuery(value: string): void {
    this.filterQuery = value;
  }

  /** Toggle a type chip: clicking the active type clears the filter. */
  toggleFilterType(t: ColumnType): void {
    this.filterType = this.filterType === t ? null : t;
  }

  /** Toggle a concern chip: clicking the active concern clears it. */
  toggleFilterConcern(c: ConcernFilter): void {
    this.filterConcern = this.filterConcern === c ? null : c;
  }

  clearFilters(): void {
    this.filterQuery = "";
    this.filterType = null;
    this.filterConcern = null;
  }

  hasActiveFilters(): boolean {
    return (
      this.filterQuery !== "" ||
      this.filterType !== null ||
      this.filterConcern !== null
    );
  }

  /** True when this column matches all active filters. Empty filters
   * pass everything; the FilterBar's counts use the same predicate. */
  columnMatchesFilters(c: ColumnInfo): boolean {
    const q = this.filterQuery.trim().toLowerCase();
    if (q !== "" && !c.name.toLowerCase().includes(q)) return false;
    if (this.filterType !== null && c.current_type !== this.filterType) {
      return false;
    }
    if (this.filterConcern !== null && !columnHasConcern(c, this.filterConcern)) {
      return false;
    }
    return true;
  }

  /** Filter check for a grouped-by-name partition (multiple cells that
   * share name + type + hints). Name and type are uniform across the
   * partition, but provenance and regmeta context can differ — so the
   * concern check must scan every cell. Filtering on `sample` alone
   * would hide a partition whose only manually-edited source happens
   * not to be the sample, defeating the "find what I edited" workflow. */
  columnsMatchFilters(cells: readonly ColumnInfo[]): boolean {
    if (cells.length === 0) return false;
    const sample = cells[0];
    const q = this.filterQuery.trim().toLowerCase();
    if (q !== "" && !sample.name.toLowerCase().includes(q)) return false;
    if (this.filterType !== null && sample.current_type !== this.filterType) {
      return false;
    }
    if (this.filterConcern !== null) {
      const concern = this.filterConcern;
      if (!cells.some((c) => columnHasConcern(c, concern))) return false;
    }
    return true;
  }

  dismissToast(id: number): void {
    const timer = this.toastTimers.get(id);
    if (timer !== undefined) {
      clearTimeout(timer);
      this.toastTimers.delete(id);
    }
    this.toasts = this.toasts.filter((t) => t.id !== id);
  }

  private async runMutation(
    fn: () => Promise<StateSnapshot>,
  ): Promise<boolean> {
    if (this.busy) return false;
    this.busy = true;
    try {
      this.snapshot = await fn();
      return true;
    } catch (exc) {
      if (exc instanceof ApiStaleState) {
        if (exc.freshState) {
          this.snapshot = exc.freshState;
        }
        // Bump the tick so any open modal can self-close: its mounted
        // snapshot of the column/group state is no longer trustworthy.
        this.staleRecoveryTick++;
        this.pushToast(
          "warning",
          "Another writer updated the config; your change was not applied. The view has been refreshed.",
        );
        return false;
      }
      const message =
        exc instanceof ApiError
          ? exc.message
          : exc instanceof Error
            ? exc.message
            : String(exc);
      this.pushToast("error", `Update failed: ${message}`);
      return false;
    } finally {
      this.busy = false;
    }
  }
}

export const store = new Store();
