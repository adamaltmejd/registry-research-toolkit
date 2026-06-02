/**
 * The A5.4 production persistence: a `ProjectPersistence` over the RAW IndexedDB
 * browser API (no `idb` dep — keeps the frontend dep surface lean). One draft per
 * key; the store stamps each record with `storeSchemaVersion` and `load` gates
 * restore on a match via the pure `restoredDraft` helper.
 *
 * GRACEFUL DEGRADATION is mandatory: in private mode / disabled storage / quota
 * failures `indexedDB` may be absent or `open()` may reject/block. In every such
 * case `save` resolves and `load` resolves `null` so the app keeps working
 * in-memory — autosave NEVER rejects or crashes the debounced `$effect`.
 */

import type { ProjectData } from "./project_data";
import type { ProjectPersistence } from "./project_store.svelte";

/** A persisted draft record. Keyed externally (no `keyPath` on the store), so the
 * stamped `schemaVersion` rides alongside the draft for the load-time gate. */
type StoredDraft = { draft: ProjectData; schemaVersion: number };

const DB_NAME = "reg_webapp_projects";
const STORE_NAME = "drafts";
const DB_VERSION = 1;

/**
 * Restore gate (PURE — unit-testable without IndexedDB): hand back the stored
 * draft only when the record exists AND its stamped `schemaVersion` matches the
 * current store schema, else `null` (a missing record or a stale-schema draft).
 */
export function restoredDraft(
  record: StoredDraft | undefined,
  currentSchemaVersion: number,
): ProjectData | null {
  return record && record.schemaVersion === currentSchemaVersion
    ? record.draft
    : null;
}

/** Promisified `indexedDB.open` at version 1. Creates the (keyless) object store
 * in `onupgradeneeded`. Rejects when IndexedDB is unavailable or the open
 * errors/blocks — callers swallow that into graceful degradation. */
function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    if (typeof indexedDB === "undefined") {
      reject(new Error("IndexedDB unavailable"));
      return;
    }
    const request: IDBOpenDBRequest = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (db && !db.objectStoreNames.contains(STORE_NAME)) {
        // No keyPath: keys are supplied explicitly on put(value, key).
        db.createObjectStore(STORE_NAME);
      }
    };
    request.onsuccess = () => {
      if (request.result) {
        resolve(request.result);
      } else {
        reject(new Error("IndexedDB open returned no database"));
      }
    };
    request.onerror = () => reject(request.error ?? new Error("open failed"));
    request.onblocked = () => reject(new Error("IndexedDB open blocked"));
  });
}

export class IndexedDBPersistence implements ProjectPersistence {
  constructor(
    private key: string,
    private schemaVersion: number,
  ) {}

  async save(
    key: string,
    draft: ProjectData,
    schemaVersion: number,
  ): Promise<void> {
    try {
      const db = await openDb();
      await new Promise<void>((resolve, reject) => {
        const txn = db.transaction(STORE_NAME, "readwrite");
        txn.objectStore(STORE_NAME).put({ draft, schemaVersion }, key);
        txn.oncomplete = () => resolve();
        txn.onerror = () => reject(txn.error ?? new Error("save txn failed"));
        txn.onabort = () => reject(txn.error ?? new Error("save txn aborted"));
      });
      db.close();
    } catch {
      // Swallow: a debounced autosave must never reject. Degrade to in-memory.
      console.warn("IndexedDB save failed; autosave degraded to in-memory.");
    }
  }

  async load(): Promise<ProjectData | null> {
    try {
      const db = await openDb();
      const record = await new Promise<StoredDraft | undefined>(
        (resolve, reject) => {
          const txn = db.transaction(STORE_NAME, "readonly");
          const req = txn.objectStore(STORE_NAME).get(this.key);
          req.onsuccess = () => resolve(req.result as StoredDraft | undefined);
          req.onerror = () => reject(req.error ?? new Error("load failed"));
        },
      );
      db.close();
      return restoredDraft(record, this.schemaVersion);
    } catch {
      return null;
    }
  }
}
