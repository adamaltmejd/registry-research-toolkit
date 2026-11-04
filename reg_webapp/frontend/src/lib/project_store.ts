/** One browser-local working document. Rejected input remains byte-for-byte
 * downloadable; only a document accepted by WASM enters the typed draft. */
import { triggerDownload } from "./api";
import type { components } from "./api-types-rust";
import {
  newProjectData,
  type ProjectData,
  type ProjectSeed,
} from "./project_data";
import { checkProject } from "./reg_core";

type Issue = components["schemas"]["Issue"];
const STORAGE_KEY = "reg_webapp:project_data";

export interface ProjectSnapshot {
  draft: ProjectData | null;
  raw: string | null;
  issues: Issue[];
  storageError: string | null;
  dirty: boolean;
}

const listeners = new Set<() => void>();
let snapshot: ProjectSnapshot = {
  draft: null,
  raw: null,
  issues: [],
  storageError: null,
  dirty: false,
};

let lastDownloaded: string | null = null;

function publish(next: ProjectSnapshot): void {
  snapshot = next;
  for (const listener of listeners) listener();
}

function replace(raw: string | null, persist: boolean): void {
  const check = raw === null ? null : checkProject(raw);
  let storageError: string | null = null;
  if (persist) {
    try {
      if (raw === null) localStorage.removeItem(STORAGE_KEY);
      else localStorage.setItem(STORAGE_KEY, raw);
    } catch {
      storageError =
        "The browser could not save this draft. Download project_data.json to keep it.";
    }
  }
  publish({
    raw,
    draft: raw !== null && check?.ok ? (JSON.parse(raw) as ProjectData) : null,
    issues: check?.issues ?? [],
    storageError,
    dirty: raw !== null && raw !== lastDownloaded,
  });
}

function updateField<K extends keyof ProjectData>(
  key: K,
  value: ProjectData[K],
): void;
function updateField(fields: Partial<ProjectData>): void;
function updateField(
  key: keyof ProjectData | Partial<ProjectData>,
  value?: ProjectData[keyof ProjectData],
): void {
  if (snapshot.draft === null) return;
  const fields = typeof key === "string" ? { [key]: value } : key;
  replace(
    `${JSON.stringify({ ...snapshot.draft, ...fields }, null, 2)}\n`,
    true,
  );
}

export const projectStore = {
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  getSnapshot(): ProjectSnapshot {
    return snapshot;
  },
  get draft(): ProjectData | null {
    return snapshot.draft;
  },
  get raw(): string | null {
    return snapshot.raw;
  },
  /** An opened file is saved as it is on disk, so it is not unsaved. */
  open(raw: string): void {
    lastDownloaded = raw;
    replace(raw, true);
  },
  newProject(seed: ProjectSeed): void {
    lastDownloaded = null;
    replace(`${JSON.stringify(newProjectData(seed), null, 2)}\n`, true);
  },
  discard(): void {
    lastDownloaded = null;
    replace(null, true);
  },
  updateField,
  download(): void {
    if (snapshot.raw === null) return;
    triggerDownload(
      new Blob([snapshot.raw], { type: "application/json" }),
      "project_data.json",
    );
    lastDownloaded = snapshot.raw;
    publish({ ...snapshot, dirty: false });
  },
};

/** Called after WASM initializes, before mounting React. Never migrate a stored
 * document: the same check and rejection path applies to storage and uploads. */
export function startDraftLifecycle(): void {
  try {
    replace(localStorage.getItem(STORAGE_KEY), false);
  } catch {
    publish({
      ...snapshot,
      storageError:
        "The browser could not read its saved draft. Open project_data.json to continue.",
    });
  }
}
