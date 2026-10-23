import type { CSSProperties } from "react";
import { Slider, SliderThumb, SliderTrack } from "react-aria-components";
import styles from "./PeriodThumbs.module.css";
import type { ProjectStudyWindow } from "./project_data";

type Years = ProjectStudyWindow;

interface Props {
  /** The drawing's axis, inclusive years: the track the thumbs move on. */
  axis: [number, number];
  /** Where the thumbs sit. */
  selection: Years;
  /** The one legal span for a moved thumb: the subject's availability. */
  bounds: Years;
  /** Names the slider group, e.g. "Period window (years)". */
  label: string;
  isDisabled?: boolean;
  /** Each move, live: the band and the columns follow it. */
  onMove: (at: Years) => void;
  /** The thumbs came to rest (release, or each key press). */
  onRest: (at: Years) => void;
}

/**
 * The window band's thumbs (DESIGN.md → Time): a two-thumb RAC `Slider` laid
 * over a coverage row, on the drawing's axis. Shared by the variable page's
 * Timeline and the /project source timeline, so both period controls move the
 * same way.
 */
export function PeriodThumbs({
  axis,
  selection,
  bounds,
  label,
  isDisabled = false,
  onMove,
  onRest,
}: Props) {
  /** The thumbs' next position: only the thumb that moved is clamped into
   * the bounds, and never past the other end, so a selection seeded outside
   * them does not drag or swap its other end. */
  function moved(next: number[]): Years {
    const [a = selection.from, b = selection.to] = next;
    const clamp = (y: number) => Math.min(bounds.to, Math.max(bounds.from, y));
    return {
      from: a === selection.from ? a : Math.min(clamp(a), b),
      to: b === selection.to ? b : Math.max(clamp(b), a),
    };
  }
  // RAC places a thumb at (value − min) / (max − min) of its track; inset by
  // half a year cell each side, each thumb sits on the centre of its year.
  const trackInset = `${(0.5 / (axis[1] - axis[0] + 1)) * 100}%`;
  // The window's share of the track, which the thumbs' minimum offset reads.
  const span =
    axis[1] > axis[0]
      ? (selection.to - selection.from) / (axis[1] - axis[0])
      : 0;
  return (
    <Slider
      className={styles.slider}
      aria-label={label}
      minValue={axis[0]}
      maxValue={axis[1]}
      value={[selection.from, selection.to]}
      formatOptions={{ useGrouping: false }}
      isDisabled={isDisabled}
      style={
        {
          left: trackInset,
          right: trackInset,
          "--thumb-span": span,
        } as CSSProperties
      }
      onChange={(next) => onMove(moved(next))}
      onChangeEnd={(next) => onRest(moved(next))}
    >
      <SliderTrack className={styles.track}>
        <SliderThumb
          index={0}
          aria-label="From year"
          className={`${styles.thumb} ${styles.from}`}
        />
        <SliderThumb
          index={1}
          aria-label="To year"
          className={`${styles.thumb} ${styles.to}`}
        />
      </SliderTrack>
    </Slider>
  );
}
