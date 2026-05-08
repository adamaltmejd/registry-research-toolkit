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
    } catch {
      // Regmeta unavailable; surface an empty list rather than blocking.
      this.registers = [];
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
    setTimeout(() => {
      this.toasts = this.toasts.filter((t) => t.id !== id);
    }, TOAST_TIMEOUT_MS);
  }

  dismissToast(id: number): void {
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
