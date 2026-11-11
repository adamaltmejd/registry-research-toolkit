import { useMemo, useSyncExternalStore } from "react";
import type {
  CatalogPeriod,
  CatalogSegment,
  RegisterShow,
  TableShow,
} from "./api";
import { type Read, tableSegmentsReads, tableShowRead } from "./catalog_names";
import { useCatalogRead } from "./use_store_read";

/** A variable's years in one table: in all, and per database on a steward with
 * several (by database id). */
export interface TableVariableYears {
  periods: readonly CatalogPeriod[];
  databases: ReadonlyMap<string, readonly CatalogPeriod[]>;
}

/** One table's variables' years, by variable. */
export type TableYears = ReadonlyMap<string, TableVariableYears>;

/** A key of the register page's table filter (#2110): a table, or a table in one
 * of the steward's databases. */
export interface LensKey {
  table: string;
  database: string | null;
}

export function lensKeyId({ table, database }: LensKey): string {
  return database === null ? table : JSON.stringify([table, database]);
}

function lensKey(id: string): LensKey {
  if (!id.startsWith("[")) return { table: id, database: null };
  const [table, database] = JSON.parse(id) as [string, string];
  return { table, database };
}

// ── The selection, shared by the register page and the browse column ─────
// The page owns it (local state, keyed by address) and publishes it here, so the
// browse column's list narrows with the page's (#2110, maintainer 2026-11-11). It
// lives as long as the page: leaving the register page clears it.

const NONE: ReadonlySet<string> = new Set();
let lens: { register: string; keys: ReadonlySet<string> } | null = null;
const listeners = new Set<() => void>();

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Publish the register page's selection; null when the page leaves. */
export function publishRegisterLens(
  register: string | null,
  keys: ReadonlySet<string> = NONE,
): void {
  const next = register === null || keys.size === 0 ? null : { register, keys };
  if (next?.register === lens?.register && next?.keys === lens?.keys) return;
  lens = next;
  for (const listener of listeners) listener();
}

/** The register page's selection while it shows `register`; empty otherwise. */
export function useRegisterLens(register: string | null): ReadonlySet<string> {
  return useSyncExternalStore(subscribe, () =>
    lens !== null && lens.register === register ? lens.keys : NONE,
  );
}

// ── The selected tables' years ────────────────────────────────────────────

/** What the years read needs of each selected table. */
export interface LensTable {
  fqid: string;
  /** Its address is its register's, where `show` answers the register. */
  implicit: boolean;
  /** Its variables, from the register's `show`. */
  variables: readonly string[];
}

/** The tables `keys` name, with what their years read needs. */
export function lensTables(
  node: RegisterShow,
  keys: ReadonlySet<string>,
): LensTable[] {
  const named = new Set([...keys].map((id) => lensKey(id).table));
  return node.tables
    .filter((table) => named.has(table.fqid))
    .map((table) => ({
      fqid: table.fqid,
      implicit: table.implicit,
      variables: node.variables
        .filter((variable) => variable.tables.includes(table.fqid))
        .map((variable) => variable.fqid),
    }));
}

const fromShow = new WeakMap<Read<TableShow>, Read<TableYears>>();
const fromSegments = new Map<
  string,
  { inputs: readonly Read<CatalogSegment[]>[]; read: Read<TableYears> }
>();

function showYears(show: TableShow): TableYears {
  return new Map(
    show.variables.map((variable) => [
      variable.fqid,
      {
        periods: variable.periods,
        databases: new Map(
          (variable.databases ?? []).map((d) => [d.space, d.periods]),
        ),
      },
    ]),
  );
}

function segmentYears(
  rows: readonly (readonly CatalogSegment[])[],
): TableYears {
  const years = new Map<
    string,
    { periods: CatalogPeriod[]; databases: Map<string, CatalogPeriod[]> }
  >();
  for (const row of rows.flat()) {
    let entry = years.get(row.variable);
    if (!entry) {
      entry = { periods: [], databases: new Map() };
      years.set(row.variable, entry);
    }
    entry.periods.push(row.period);
    if (row.space) {
      const held = entry.databases.get(row.space) ?? [];
      held.push(row.period);
      entry.databases.set(row.space, held);
    }
  }
  return years;
}

/** Each table's variables' years, by table fqid: a table's `show`, or, for an
 * implicit table (whose address answers its register), its rows from `segments`
 * in the project page's batches. Each entry keeps its identity until a read
 * behind it settles. */
export function lensYearsReads(
  tables: readonly LensTable[],
): Record<string, Read<TableYears>> {
  return Object.fromEntries(
    tables.map((table) => {
      if (!table.implicit) {
        const show = tableShowRead(table.fqid);
        let years = fromShow.get(show);
        if (!years) {
          years = {
            loading: show.loading,
            value: show.value && showYears(show.value),
          };
          fromShow.set(show, years);
        }
        return [table.fqid, years];
      }
      const inputs = Object.values(
        tableSegmentsReads(table.fqid, table.variables),
      );
      const cached = fromSegments.get(table.fqid);
      if (
        cached &&
        cached.inputs.length === inputs.length &&
        cached.inputs.every((input, i) => input === inputs[i])
      )
        return [table.fqid, cached.read];
      const read: Read<TableYears> = {
        loading: inputs.some((input) => input.loading),
        value: inputs.every((input) => input.value)
          ? segmentYears(inputs.map((input) => input.value ?? []))
          : null,
      };
      fromSegments.set(table.fqid, { inputs, read });
      return [table.fqid, read];
    }),
  );
}

// ── The lens ──────────────────────────────────────────────────────────────

type Variable = RegisterShow["variables"][number];

/** What the selection leaves of a register's variables. */
export type Lens =
  | { state: "off" }
  | { state: "loading" }
  /** A selected table's years did not load: nothing is shown for it. */
  | { state: "failed"; tables: string[] }
  | {
      state: "on";
      variables: Variable[];
      /** Each shown variable's years in the selected tables and databases. */
      years: ReadonlyMap<string, readonly CatalogPeriod[]>;
    };

/** OR within the selection: a variable is shown when a selected table, or a
 * selected table in a selected database, has it, and its bar draws only those
 * years (#2110, maintainer 2026-11-11). */
export function registerLens(
  variables: readonly Variable[],
  keys: ReadonlySet<string>,
  reads: Record<string, Read<TableYears>>,
): Lens {
  if (keys.size === 0) return { state: "off" };
  const selected = [...keys].map(lensKey);
  if (selected.some((key) => reads[key.table]?.loading ?? true))
    return { state: "loading" };
  const failed = [
    ...new Set(
      selected
        .filter((key) => !reads[key.table]?.value)
        .map((key) => key.table),
    ),
  ];
  if (failed.length > 0) return { state: "failed", tables: failed };
  const years = new Map<string, CatalogPeriod[]>();
  for (const key of selected) {
    for (const [variable, held] of reads[key.table]?.value ?? []) {
      const periods =
        key.database === null ? held.periods : held.databases.get(key.database);
      if (!periods?.length) continue;
      years.set(variable, [...(years.get(variable) ?? []), ...periods]);
    }
  }
  return {
    state: "on",
    variables: variables.filter((variable) => years.has(variable.fqid)),
    years,
  };
}

/** The lens `keys` make of `node`'s variables, reading the selected tables'
 * years on first selection (each read once, then cached). */
export function useRegisterLensOf(
  node: RegisterShow | null,
  keys: ReadonlySet<string>,
): Lens {
  const tables = useMemo(
    () => (node ? lensTables(node, keys) : []),
    [node, keys],
  );
  const reads = useCatalogRead(lensYearsReads, tables);
  return node ? registerLens(node.variables, keys, reads) : { state: "off" };
}
