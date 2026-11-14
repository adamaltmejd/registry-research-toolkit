import type { components } from "./api-types-rust";
import styles from "./CatalogNodeView.module.css";
import { segmentYears, startText } from "./catalog_period";
import breakStyles from "./SeriesBreaks.module.css";
import { Panel } from "./ui/Panel";
import variableStyles from "./VariableView.module.css";

type SeriesBreak = components["schemas"]["ShowSeriesBreak"];

/** One break with where it applies: a table's name on a register page. */
export interface PlacedBreak {
  where: string | null;
  seriesBreak: SeriesBreak;
}

/** The muted line over a break: where it is and when, or nothing when neither
 * is stated. */
function when({ where, seriesBreak }: PlacedBreak): string | null {
  const day = seriesBreak.at ? `from ${startText(seriesBreak.at)}` : null;
  const years = seriesBreak.releases?.length
    ? segmentYears(seriesBreak.releases)
    : null;
  const time = day ?? years;
  const line = [where && `In ${where}`, time].filter(Boolean).join(", ");
  return line ? line[0].toUpperCase() + line.slice(1) : null;
}

/** The provider's breaks in the series (DESIGN.md → Detail pane): each with its
 * place and day, then what changed. Renders nothing when there are none. */
export function SeriesBreaks({ breaks }: { breaks: readonly PlacedBreak[] }) {
  if (breaks.length === 0) return null;
  return (
    <Panel title="Series breaks">
      <ul className={styles.plainList}>
        {breaks.map((placed) => {
          const line = when(placed);
          const text = placed.seriesBreak.text;
          return (
            <li
              key={`${placed.where}:${placed.seriesBreak.at}:${text}:${segmentYears(placed.seriesBreak.releases ?? [])}`}
              className={variableStyles.wording}
            >
              {line && <p className={breakStyles.when}>{line}</p>}
              {text ? (
                text
                  .split("\n")
                  .filter((part) => part.trim() !== "")
                  .map((part) => (
                    <p key={part} className={styles.prose}>
                      {part}
                    </p>
                  ))
              ) : (
                <p className={styles.prose}>
                  The provider marks a break here without saying what changed.
                </p>
              )}
            </li>
          );
        })}
      </ul>
    </Panel>
  );
}
