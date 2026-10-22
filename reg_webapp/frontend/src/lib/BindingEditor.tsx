import styles from "./BindingEditor.module.css";
import { catalogHref, registerPrefixOf } from "./catalog";
import { columnNames, registerRead, UNASKED } from "./catalog_names.svelte";
import { periodLabel, periodWindowRelation } from "./period";
import type { DraftBinding, ProjectStudyWindow } from "./project_data";
import { projectStore } from "./project_store";
import { sourcePeriodFromWire, sourcePeriodToWire } from "./reg_core";
import ScopedDataWarnings from "./ScopedDataWarnings.svelte";
import { SvelteIsland } from "./SvelteIsland";
import { useRune } from "./svelte_bridge.svelte";
import { Button } from "./ui/Button";
import { ColumnChip } from "./ui/ColumnChip";
import { bindingAnchorId } from "./validation";

// READ-ONLY column row in the #991 data-order cart: it DISPLAYS the delivery
// COLUMN this binding orders and the variable it came from, and offers "Remove
// column" only. Editing a binding — variable, type, display_name, representation —
// happens by re-picking in the catalog browser. See reg_webapp/DESIGN.md and #991.

interface Props {
  sourceIndex: number;
  bindingIndex: number;
  binding: DraftBinding;
  /** The owning source's concrete variant slug and its stored period (wire form,
   * null when it has none) — where this column's default name is resolved. */
  variant: string;
  period: string | null;
  /** The project's study window, null when none is set: a column with no year
   * inside it is drawn out of the window. */
  studyWindow: ProjectStudyWindow | null;
}

/** `columnNames` behind one string argument, for `useRune`. */
function readColumnNames(key: string) {
  const [fqid, period, variant] = JSON.parse(key) as [
    string,
    string | null,
    string,
  ];
  return columnNames(fqid, period, variant);
}
const readUnasked = () => UNASKED;

export function BindingEditor({
  sourceIndex,
  bindingIndex,
  binding,
  variant,
  period: sourcePeriod,
  studyWindow,
}: Props) {
  // A binding field coerced to a display string (non-string → "").
  const strField = (field: keyof DraftBinding): string => {
    const v = binding[field];
    return typeof v === "string" ? v : "";
  };
  const variable = strField("variable");
  // A binding period narrows this column to its own years inside the source's
  // (#1462); the row says so, and resolves and warns at those years. Absent, the
  // column inherits the source period and the row says nothing.
  const ownPeriod =
    binding.period == null ? null : sourcePeriodToWire(binding.period);
  const period = ownPeriod ?? sourcePeriod;
  // The column's own years, else the source's: none inside the window draws the
  // chips out of it.
  const outOfWindow =
    period !== null &&
    periodWindowRelation(sourcePeriodFromWire(period), studyWindow) ===
      "disjoint";
  const chipState = outOfWindow ? "out" : "default";
  // The variable's descriptive name, from the register read every source card on
  // this register already shares: no request per column.
  const registerFqid = registerPrefixOf(variable);
  const register = useRune(
    registerFqid ? registerRead : readUnasked,
    registerFqid,
  );
  const variableName =
    register.value?.children
      .find((child) => child.fqid === variable)
      ?.name?.trim() || null;

  // The row LEADS with the delivery column. Where the FILE names it, the file
  // wins: an explicit `display_name` first, then the `representation` a pick pins.
  // Carrying NEITHER is the ORDINARY case (#992): that column's name is the
  // reg_meta default at the source's (variant, period), which lives only in the
  // catalog, so the row resolves it there (cached per (fqid, period, variant)).
  // Nothing resolvable keeps the FQID it was picked from. Nothing here is written
  // back to the draft.
  const fileColumn = strField("display_name") || strField("representation");
  const resolved = useRune(
    fileColumn ? readUnasked : readColumnNames,
    JSON.stringify([variable, period, variant]),
  );
  const resolvedNames = resolved.value ?? [];
  const columnName = fileColumn || resolvedNames[0] || "";
  // A column RENAMED within the source's period resolves to several names: the
  // current one leads, and the superseded ones trail it OLDEST-first.
  const supersededColumns = resolvedNames.slice(1).reverse();
  // What the delete button calls this row: the column it shows, else the variable.
  const columnLabel = columnName || variable;
  const representation = strField("representation") || null;
  // A fresh object per render is fine: the island assigns it onto the live
  // instance, and an unchanged value moves nothing there.
  const warningProps = {
    fqid: variable,
    period,
    variant,
    representation,
    framed: false,
  };

  return (
    <>
      {/* `id` is the click-to-locate anchor the ValidationPanel scrolls to. */}
      <div
        className={styles.binding}
        id={bindingAnchorId(sourceIndex, bindingIndex)}
      >
        {/* `aria-busy` while the column's catalog name is in flight: the row is
            showing its FQID alone as a stand-in. */}
        <div
          className={styles.body}
          aria-busy={resolved.loading ? "true" : undefined}
        >
          <div className={styles.columns}>
            {columnName && <ColumnChip name={columnName} state={chipState} />}
            {supersededColumns.length > 0 && (
              <span className={styles.renameHint}>
                was{" "}
                {supersededColumns.map((name) => (
                  <ColumnChip key={name} name={name} state={chipState} />
                ))}
              </span>
            )}
            {ownPeriod !== null && (
              <span className={styles.bindingPeriod}>
                only{" "}
                <span className={styles.years}>
                  {periodLabel(binding.period)}
                </span>
              </span>
            )}
            {outOfWindow && (
              <span className={styles.bindingPeriod}>
                outside the study window
              </span>
            )}
          </div>
          {/* The variable it came from: its name in the text face, linked to its
              catalog page (the cart is read-only, so re-picking happens there,
              #991), and the FQID in mono under it. */}
          {variable ? (
            <span className={styles.variableLine}>
              <a className={styles.variable} href={catalogHref(variable)}>
                {variableName ?? variable}
              </a>
              {variableName && <code className={styles.fqid}>{variable}</code>}
            </span>
          ) : (
            <span className={styles.bindingPeriod}>(no variable)</span>
          )}
        </div>
        <Button
          variant="danger"
          aria-label={columnLabel ? `Remove column ${columnLabel}` : undefined}
          onPress={() => projectStore.removeBinding(sourceIndex, bindingIndex)}
        >
          Remove column
        </Button>
      </div>
      {variable && (
        <SvelteIsland component={ScopedDataWarnings} props={warningProps} />
      )}
    </>
  );
}
