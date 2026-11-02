// Every table role is restated on purpose: the stacked form's `display: block`
// strips the native roles in Firefox and Safari.
// oxlint-disable jsx-a11y/no-redundant-roles, jsx-a11y/no-interactive-element-to-noninteractive-role
import {
  type CSSProperties,
  type MouseEvent,
  type ReactNode,
  useRef,
} from "react";
import styles from "./DataTable.module.css";
import type { Column } from "./types";

interface Props<Row extends object> {
  columns: Column<Row>[];
  rows: Row[];
  /** Stable row identity, used as the React key. */
  getRowId: (row: Row) => string;
  /** The cell's content; `row[column.key]` when omitted. */
  renderCell?: (row: Row, column: Column<Row>) => ReactNode;
  /** Link-list tables: a plain click anywhere on a row follows the row's first
   * link. The link stays the row's only tab stop and accessible name. */
  rowNavigation?: boolean;
  /** The raised panel surface, with the column headers as its title row. */
  framed?: boolean;
  /** The page already names what the columns hold: the headers stay for
   * assistive technology only, and the stacked form drops its cell labels. */
  hideHeaders?: boolean;
}

// A click that lands on a control inside a cell is that control's own.
const INTERACTIVE =
  'a[href], button, input, select, textarea, label, [role="button"], [tabindex]';

function alignOf<Row>(column: Column<Row>): "start" | "end" {
  return column.align ?? (column.numeric ? "end" : "start");
}

/**
 * The plain `<table>` (reg_webapp/DESIGN.md, not re-litigated): `label`-level
 * headers, hairline rows, mono identifiers, right-aligned measures. Below the
 * narrow breakpoint it stacks into cards whose non-primary cells carry their
 * column label; a framed two-column table collapses to one column instead. Every
 * role is set explicitly, because the stacked form's `display: block` strips the
 * native table roles in Firefox and Safari.
 */
export function DataTable<Row extends object>({
  columns,
  rows,
  getRowId,
  renderCell,
  rowNavigation = false,
  framed = false,
  hideHeaders = false,
}: Props<Row>) {
  const pressStart = useRef<{ row: Element; x: number; y: number } | null>(
    null,
  );

  function onMouseDown(event: MouseEvent<HTMLTableRowElement>): void {
    pressStart.current =
      event.button === 0
        ? { row: event.currentTarget, x: event.clientX, y: event.clientY }
        : null;
  }

  function onClick(event: MouseEvent<HTMLTableRowElement>): void {
    const row = event.currentTarget;
    const start = pressStart.current;
    pressStart.current = null;
    // A drag (text selection) or a modified click is not a row press.
    const dragged =
      start?.row === row &&
      Math.hypot(event.clientX - start.x, event.clientY - start.y) > 4;
    const selection = window.getSelection();
    const selecting =
      selection !== null &&
      !selection.isCollapsed &&
      selection.toString().trim() !== "" &&
      (row.contains(selection.anchorNode) || row.contains(selection.focusNode));
    const target = event.target instanceof Element ? event.target : null;
    const control = target?.closest(INTERACTIVE);
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey ||
      (control != null && control !== row) ||
      dragged ||
      selecting
    ) {
      return;
    }
    row.querySelector<HTMLAnchorElement>("a[href]")?.click();
  }

  const tableClass = [
    styles.table,
    framed && styles.framed,
    framed && columns.length === 2 && styles.narrowStack,
    hideHeaders && styles.headless,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={styles.scroll}>
      <table
        className={tableClass}
        role="table"
        style={{ "--data-table-columns": columns.length } as CSSProperties}
      >
        <thead
          role="rowgroup"
          className={hideHeaders ? "visually-hidden" : undefined}
        >
          <tr role="row">
            {columns.map((column, i) => (
              <th
                key={column.key}
                scope="col"
                role="columnheader"
                className={`micro-label ${styles[alignOf(column)]} ${i === 0 ? styles.first : ""}`}
                style={column.width ? { width: column.width } : undefined}
              >
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody role="rowgroup">
          {rows.map((row) => (
            <tr
              key={getRowId(row)}
              role="row"
              className={rowNavigation ? styles.navigable : undefined}
              onMouseDown={rowNavigation ? onMouseDown : undefined}
              onClick={rowNavigation ? onClick : undefined}
            >
              {columns.map((column, i) => (
                <td
                  key={column.key}
                  role="cell"
                  className={[
                    styles[alignOf(column)],
                    i === 0 && styles.first,
                    (column.mono || column.numeric) && styles.mono,
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  data-label={column.label}
                >
                  {renderCell
                    ? renderCell(row, column)
                    : String(row[column.key] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
