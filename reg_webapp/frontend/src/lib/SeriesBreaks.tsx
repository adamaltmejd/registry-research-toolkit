import styles from "./CatalogNodeView.module.css";
import { segmentYears, startText } from "./catalog_period";
import { Paragraphs } from "./Paragraphs";
import breakStyles from "./SeriesBreaks.module.css";
import { type PlacedBreak, repeatKeys } from "./series_breaks";
import { Panel } from "./ui/Panel";

/** The muted line over a break: where it is and when, or nothing when neither
 * is stated. */
function when({ where, seriesBreak }: PlacedBreak): string | null {
  const time = seriesBreak.at
    ? `${where ? "from" : "From"} ${startText(seriesBreak.at)}`
    : seriesBreak.releases?.length
      ? segmentYears(seriesBreak.releases)
      : null;
  if (!where) return time;
  return time ? `In ${where}, ${time}` : `In ${where}`;
}

/** The provider's breaks in the series (DESIGN.md → Detail pane): each with its
 * place and day, then what changed. Renders nothing when there are none. */
export function SeriesBreaks({ breaks }: { breaks: readonly PlacedBreak[] }) {
  if (breaks.length === 0) return null;
  const keys = repeatKeys(
    breaks.map((placed) =>
      JSON.stringify([when(placed), placed.seriesBreak.text ?? null]),
    ),
  );
  return (
    <Panel title="Series breaks">
      <ul className={styles.plainList}>
        {breaks.map((placed, index) => {
          const line = when(placed);
          const text = placed.seriesBreak.text;
          return (
            <li key={keys[index]} className={breakStyles.item}>
              {line && <p className={breakStyles.when}>{line}</p>}
              {text ? (
                <Paragraphs text={text} />
              ) : (
                <p className={styles.prose}>
                  A break is noted without saying what changed.
                </p>
              )}
            </li>
          );
        })}
      </ul>
    </Panel>
  );
}
