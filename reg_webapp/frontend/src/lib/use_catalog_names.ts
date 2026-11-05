import { catalogNameRead, catalogNames, type Read } from "./catalog_names";
import { useCatalogRead, useDistinct } from "./use_store_read";

const readNames = (refs: readonly string[]) =>
  Object.fromEntries(catalogNames(refs));

const readPending = (refs: readonly string[]) =>
  refs.filter((ref) => catalogNameRead(ref).loading);

export function useCatalogNames(
  refs: readonly string[],
): ReadonlyMap<string, string> {
  return new Map(Object.entries(useCatalogRead(readNames, useDistinct(refs))));
}

export function useCatalogName(ref: string | null): Read<string> {
  return useCatalogRead(catalogNameRead, ref);
}

/** The refs whose name read is still in flight: a placeholder there, while a
 * settled miss is a real "unavailable". */
export function useCatalogNamesPending(
  refs: readonly string[],
): ReadonlySet<string> {
  return new Set(useCatalogRead(readPending, useDistinct(refs)));
}
