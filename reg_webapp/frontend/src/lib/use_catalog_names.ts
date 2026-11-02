import { catalogNames } from "./catalog_names";
import { useCatalogRead, useDistinct } from "./use_store_read";

const readNames = (refs: readonly string[]) =>
  Object.fromEntries(catalogNames(refs));

export function useCatalogNames(
  refs: readonly string[],
): ReadonlyMap<string, string> {
  return new Map(Object.entries(useCatalogRead(readNames, useDistinct(refs))));
}
