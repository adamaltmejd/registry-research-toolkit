import type { components } from "./api-types-rust";

type SeriesBreak = components["schemas"]["ShowSeriesBreak"];

/** One break with where it applies: a table's name on a register page. */
export interface PlacedBreak {
  where: string | null;
  seriesBreak: SeriesBreak;
}

/** `breaks` at `where`, earliest first: by day, else by first release, undated
 * last. */
export function placeBreaks(
  where: string | null,
  breaks: readonly SeriesBreak[] | undefined,
): PlacedBreak[] {
  const start = (b: SeriesBreak) =>
    b.at ?? b.releases?.[0]?.period.start ?? "￿";
  return [...(breaks ?? [])]
    .sort((a, b) => (start(a) < start(b) ? -1 : start(a) > start(b) ? 1 : 0))
    .map((seriesBreak) => ({ where, seriesBreak }));
}

/** React keys for `values` that may repeat: each value, numbered from its
 * second occurrence. */
export function repeatKeys(values: readonly string[]): string[] {
  const seen = new Map<string, number>();
  return values.map((value) => {
    const count = seen.get(value) ?? 0;
    seen.set(value, count + 1);
    return count === 0 ? value : `${value}#${count}`;
  });
}
