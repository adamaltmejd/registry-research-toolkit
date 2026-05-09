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
  type SetColumnTypeArgs,
  type SetGroupRegisterArgs,
} from "./api";
import type { RegisterEntry, StateSnapshot } from "./types";

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
