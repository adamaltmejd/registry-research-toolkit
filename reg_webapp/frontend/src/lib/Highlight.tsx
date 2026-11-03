import styles from "./Highlight.module.css";
import { foldSearch } from "./reg_core";

/** Where `needle` matches in `text`, as UTF-16 offsets into `text`, under the
 * same fold `rankFilter` matches with, so "alder" marks "Ålder". The text is
 * folded one character at a time to keep each folded unit's source offset;
 * whitespace stands for itself.
 * simplify: a run of several spaces in `text` folds to that many, where the
 * whole-string fold collapses it to one, so a needle spanning such a run marks
 * nothing; fold the text whole with an offset map if catalog names carry them. */
function matchRange(text: string, needle: string): [number, number] | null {
  const query = foldSearch(needle);
  if (!query) return null;
  let folded = "";
  const source: number[] = [];
  let offset = 0;
  for (const char of text) {
    const unit = /\s/.test(char) ? " " : foldSearch(char);
    folded += unit;
    for (let i = 0; i < unit.length; i += 1) source.push(offset);
    offset += char.length;
  }
  const at = folded.indexOf(query);
  if (at < 0) return null;
  const last = source[at + query.length - 1];
  return [source[at], last + ((text.codePointAt(last) ?? 0) > 0xffff ? 2 : 1)];
}

/** `text` with the browse filter's match underlined in `accent` at weight 600
 * (DESIGN.md → Layout → Search filters in place). */
export function Highlight({ text, needle }: { text: string; needle: string }) {
  const range = needle.trim() === "" ? null : matchRange(text, needle);
  if (range === null) return text;
  const [start, end] = range;
  return (
    <>
      {text.slice(0, start)}
      <mark className={styles.match}>{text.slice(start, end)}</mark>
      {text.slice(end)}
    </>
  );
}
