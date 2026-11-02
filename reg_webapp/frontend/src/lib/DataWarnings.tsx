import { useId } from "react";
import type { DataWarningModel } from "./api";
import { formatWindow } from "./catalog";
import styles from "./DataWarnings.module.css";
import { TechnicalDetails } from "./SubjectView";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";

interface Props {
  warnings: DataWarningModel[];
  /** Null renders the bare list, for a caller that supplies its own label. */
  title?: string | null;
  framed?: boolean;
}

function unavailableLabel(field: string): string {
  if (field === "variable") return "verified variable identity";
  if (field === "code_membership" || field === "unbound_value_membership")
    return "verified response codes";
  if (field.startsWith("curation/")) return "unsupported delivery declaration";
  if (field.endsWith("conditional_sensitivity"))
    return "conditional sensitivity flag";
  if (field.endsWith("sensitivity")) return "sensitivity flag";
  if (field.endsWith("identifier")) return "identifier flag";
  if (field.endsWith("value_set")) return "response codes";
  if (field.endsWith("classification")) return "verified classification link";
  if (field.endsWith("data_type")) return "storage type";
  if (field.endsWith("delivery_column_name")) return "delivery column";
  return field.replace(/^state\./, "").replaceAll("_", " ");
}

// A withheld variable is named by its FQID. It was not published, so there is no
// catalog page to link to: show the identifier. (`curation/` paths are labelled
// by `unavailableLabel`.)
function unavailableItems(
  withheld: readonly string[],
): { label: string; fqid: boolean }[] {
  const items = new Map<string, { label: string; fqid: boolean }>();
  for (const field of withheld) {
    const fqid =
      !field.startsWith("curation/") && /^[^/:]+(\/[^/:]+)+$/.test(field);
    const label = fqid ? field : unavailableLabel(field);
    items.set(label, { label, fqid });
  }
  return [...items.values()];
}

function compareBound(
  a: string | null | undefined,
  b: string | null | undefined,
  open: -1 | 1,
): number {
  if (a === b) return 0;
  if (a == null) return open;
  if (b == null) return -open;
  return a < b ? -1 : 1;
}

function windowText({ valid_from: from, valid_to: to }: DataWarningModel) {
  if (from && to) return formatWindow(from, to);
  return from ? `From ${from}` : `Through ${to}`;
}

/** The data warnings of one subject: a status row per warning, `error` or `warn`
 * by severity, with its scope (column, variant, window), the metadata it
 * withheld, and its longer detail behind "Technical details". Renders nothing
 * when there are none. */
export function DataWarnings({
  warnings,
  title = "Data warnings",
  framed = true,
}: Props) {
  const headingId = useId();
  if (warnings.length === 0) return null;
  // The API returns warnings in `warning_id` (hash) order; a reader scans by
  // period. An open start sorts first and an open end last. `sort` is stable, so
  // warnings with the same window keep the API order.
  const ordered = [...warnings].sort(
    (a, b) =>
      compareBound(a.valid_from, b.valid_from, -1) ||
      compareBound(a.valid_to, b.valid_to, 1),
  );
  const list = (
    <ul className={styles.warnings}>
      {ordered.map((warning) => (
        <li key={warning.warning_id}>
          <StatusRow tone={warning.severity === "error" ? "error" : "warn"}>
            <span className={styles.summary}>
              <span className={styles.severity}>
                {warning.severity === "error" ? "Error" : "Warning"}.
              </span>{" "}
              {warning.summary}
            </span>
            {(warning.delivery_column_name ||
              warning.variant ||
              warning.valid_from ||
              warning.valid_to) && (
              <span className={styles.scope}>
                {warning.delivery_column_name && (
                  <code>{warning.delivery_column_name}</code>
                )}
                {warning.variant && (
                  <span>
                    Variant <code>{warning.variant}</code>
                  </span>
                )}
                {(warning.valid_from || warning.valid_to) && (
                  <span>{windowText(warning)}</span>
                )}
              </span>
            )}
            {warning.withheld_output.length > 0 && (
              <span className={styles.scope}>
                <span>
                  Unavailable metadata:{" "}
                  {unavailableItems(warning.withheld_output).map((item, i) => (
                    <span key={item.label}>
                      {i > 0 && ", "}
                      {item.fqid ? (
                        <>
                          variable <code>{item.label}</code>
                        </>
                      ) : (
                        item.label
                      )}
                    </span>
                  ))}
                </span>
              </span>
            )}
          </StatusRow>
          {warning.detail !== warning.summary && (
            <TechnicalDetails>
              <p className={styles.detail}>{warning.detail}</p>
            </TechnicalDetails>
          )}
        </li>
      ))}
    </ul>
  );
  if (title === null) return list;
  if (framed) return <Panel title={title}>{list}</Panel>;
  return (
    <section className={styles.inline} aria-labelledby={headingId}>
      <h2 id={headingId} className={styles.heading}>
        {title}
      </h2>
      {list}
    </section>
  );
}
