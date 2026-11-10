import { type CatalogSegment, type Context, stewardConfig } from "./api";
import { segmentYears } from "./catalog_period";
import type { ProjectData } from "./project_data";

export type Space = NonNullable<
  ReturnType<typeof stewardConfig>
>["spaces"][number];

/** A steward's databases when it has several, each with its own person key;
 * `null` with one, where nothing about databases is shown. */
export function stewardDatabases(context: Context | null): Space[] | null {
  const spaces = stewardConfig(context)?.spaces ?? [];
  return spaces.length > 1 ? spaces : null;
}

/** The database the project names, when it is one of the steward's. */
export function chosenDatabase(
  context: Context | null,
  project: ProjectData | null,
): Space | null {
  const id = project?.linkage?.database;
  return stewardDatabases(context)?.find((space) => space.id === id) ?? null;
}

/** A segment's rows in one database. A row carries its database only on a
 * steward with several, so with one every row is in. */
export function inDatabase<T extends { space?: string }>(
  segments: readonly T[],
  database: Space | null,
): T[] {
  return database
    ? segments.filter(
        (segment) => !segment.space || segment.space === database.id,
      )
    : [...segments];
}

/** The line under a table's name that says which databases have its rows:
 * "All databases" when every one has the same years, the names when they
 * share years, and each name with its years when they differ (#2343, #2344). */
export function databasesLine(
  segments: readonly Pick<CatalogSegment, "space" | "period">[],
  databases: readonly Space[] | null,
): string | null {
  if (!databases) return null;
  const years = databases.flatMap((space) => {
    const rows = segments.filter((segment) => segment.space === space.id);
    return rows.length ? [{ name: space.name, years: segmentYears(rows) }] : [];
  });
  if (years.length === 0) return null;
  const same = years.every((entry) => entry.years === years[0].years);
  if (same && years.length === databases.length) return "All databases";
  return same
    ? years.map((entry) => entry.name).join(", ")
    : years.map((entry) => `${entry.name} ${entry.years}`).join(", ");
}
