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

const TOAST_TIMEOUT_MS = 6000;

class Store {
  snapshot: StateSnapshot | null = $state(null);
  registers: RegisterEntry[] | null = $state(null);
  toasts: Toast[] = $state([]);
  loadError: string | null = $state(null);
  busy = $state(false);

  private nextToastId = 1;
  private toastTimers = new Map<number, ReturnType<typeof setTimeout>>();

  async load(): Promise<void> {
    try {
      this.snapshot = await fetchState();
      this.loadError = null;
    } catch (exc) {
      this.loadError = exc instanceof Error ? exc.message : String(exc);
    }
  }

  /** Lazy-load on first popover open; idempotent. */
  async ensureRegisters(): Promise<void> {
    if (this.registers !== null) return;
    try {
      const response = await listRegisters();
      this.registers = response.registers;
    } catch (exc) {
      // Regmeta unavailable; surface an empty list rather than
      // blocking the modal, but warn once so a broken regmeta install
      // is visible (the autocomplete would otherwise look intentional).
      this.registers = [];
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
    this.toasts = [...this.toasts, { id, level, message }];
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
