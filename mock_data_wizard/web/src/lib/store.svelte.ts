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
  putPanel as apiPutPanel,
  removePanel as apiRemovePanel,
  setColumnType as apiSetColumnType,
  setGroupRegister as apiSetGroupRegister,
  setSourceRegisters as apiSetSourceRegisters,
  unsetColumnManual as apiUnsetColumnManual,
  type PutPanelArgs,
  type RemovePanelArgs,
  type SetColumnTypeArgs,
  type SetGroupRegisterArgs,
  type SetSourceRegistersArgs,
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
const OPEN_GROUPS_KEY = "mdw.web.openGroups";
const OPEN_SOURCES_KEY = "mdw.web.openSources";

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
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object") return { ...DEFAULT_VISIBLE_COLUMNS };
    // Per-key boolean coercion: an upgrade may add a column id (parsed
    // is missing the key → falls through to the default) and a hand-
    // edited or stale payload may have non-boolean values for known
    // keys (ignored, default wins).
    const out = { ...DEFAULT_VISIBLE_COLUMNS };
    for (const key of Object.keys(out) as OptionalColumnId[]) {
      const v = (parsed as Record<string, unknown>)[key];
      if (typeof v === "boolean") out[key] = v;
    }
    return out;
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

/** Per-browser persistence for the user's expand/collapse state. Stored
 * as a flat string array (open group_ids) so a fresh page load — or any
 * snapshot mutation that re-renders the GroupCard list — preserves
 * exactly which cards the user had open. Closed-by-default for groups
 * that have never been touched is still the right initial behavior;
 * only opened groups are persisted. */
function loadStringSet(key: string): Set<string> {
  if (typeof localStorage === "undefined") return new Set();
  try {
    const raw = localStorage.getItem(key);
    if (raw === null) return new Set();
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return new Set();
    return new Set(parsed.filter((v): v is string => typeof v === "string"));
  } catch {
    return new Set();
  }
}

function saveStringSet(key: string, value: Set<string>): void {
  if (typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(key, JSON.stringify([...value]));
  } catch {
    // Storage quota / private mode — non-fatal.
  }
}

/** "<group_id>|<source_name>" composite keys. Flat string set keeps the
 * persistence shape identical to OPEN_GROUPS_KEY and avoids a nested
 * Map<string, Set<string>> serialiser; the pipe is safe because
 * group_ids ("reg-N" / "noreg-<source>") and source names don't carry
 * one in practice. */
function sourceKey(groupId: string, sourceName: string): string {
  return `${groupId}|${sourceName}`;
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

  /** Open group_ids and "<group_id>|<source>" keys. Persisted to
   * localStorage so reload + post-mutation re-renders don't collapse
   * cards the user is actively working on. Closed-by-default: only
   * touched entries are persisted. */
  openGroups: Set<string> = $state(loadStringSet(OPEN_GROUPS_KEY));
  openSources: Set<string> = $state(loadStringSet(OPEN_SOURCES_KEY));

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
      this.setSnapshot(await fetchState());
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
      this.setSnapshot(await apiInitProject());
      this.loadState = { kind: "ready" };
      this.pushToast("info", "Project initialised. Review the auto-classified columns below.");
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : String(exc);
      this.pushToast("error", `Could not initialise: ${message}`);
    } finally {
      this.busy = false;
    }
  }

  /** Sole snapshot setter. Also prunes ``openGroups``/``openSources`` of
   * any entry whose group_id no longer exists in the new snapshot — the
   * persisted set would otherwise grow forever as users move between
   * projects or as re-discover renames groups. */
  private setSnapshot(snap: StateSnapshot): void {
    this.snapshot = snap;
    this.pruneOpenStateAgainst(snap);
  }

  private pruneOpenStateAgainst(snap: StateSnapshot): void {
    const liveGroupIds = new Set(snap.groups.map((g) => g.group_id));
    const liveSourceKeys = new Set<string>();
    for (const g of snap.groups) {
      for (const sn of g.sources) liveSourceKeys.add(sourceKey(g.group_id, sn));
    }
    let groupsChanged = false;
    const nextGroups = new Set<string>();
    for (const id of this.openGroups) {
      if (liveGroupIds.has(id)) nextGroups.add(id);
      else groupsChanged = true;
    }
    if (groupsChanged) {
      this.openGroups = nextGroups;
      saveStringSet(OPEN_GROUPS_KEY, nextGroups);
    }
    let sourcesChanged = false;
    const nextSources = new Set<string>();
    for (const key of this.openSources) {
      if (liveSourceKeys.has(key)) nextSources.add(key);
      else sourcesChanged = true;
    }
    if (sourcesChanged) {
      this.openSources = nextSources;
      saveStringSet(OPEN_SOURCES_KEY, nextSources);
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

  async setSourceRegisters(args: SetSourceRegistersArgs): Promise<boolean> {
    return this.runMutation(() => apiSetSourceRegisters(args));
  }

  async putPanel(args: PutPanelArgs): Promise<boolean> {
    return this.runMutation(() => apiPutPanel(args));
  }

  async removePanel(args: RemovePanelArgs): Promise<boolean> {
    return this.runMutation(() => apiRemovePanel(args));
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

  setGroupOpen(groupId: string, open: boolean): void {
    if (this.openGroups.has(groupId) === open) return;
    // Reassign the Set so Svelte's reactivity picks up the change —
    // mutating in place won't trigger downstream $derived recomputes.
    const next = new Set(this.openGroups);
    if (open) next.add(groupId);
    else next.delete(groupId);
    this.openGroups = next;
    saveStringSet(OPEN_GROUPS_KEY, next);
  }

  isGroupOpen(groupId: string): boolean {
    return this.openGroups.has(groupId);
  }

  setSourceOpen(groupId: string, sourceName: string, open: boolean): void {
    const key = sourceKey(groupId, sourceName);
    if (this.openSources.has(key) === open) return;
    const next = new Set(this.openSources);
    if (open) next.add(key);
    else next.delete(key);
    this.openSources = next;
    saveStringSet(OPEN_SOURCES_KEY, next);
  }

  isSourceOpen(groupId: string, sourceName: string): boolean {
    return this.openSources.has(sourceKey(groupId, sourceName));
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
      this.setSnapshot(await fn());
      return true;
    } catch (exc) {
      if (exc instanceof ApiStaleState) {
        if (exc.freshState) {
          this.setSnapshot(exc.freshState);
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
