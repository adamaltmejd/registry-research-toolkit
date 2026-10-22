import { getWarnings } from "./api";
import { DataWarnings } from "./DataWarnings";
import styles from "./ScopedDataWarnings.module.css";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

interface Props {
  fqid: string;
  period?: string | null;
  variant?: string | null;
  representation?: string | null;
  registerOnly?: boolean;
  framed?: boolean;
  /** Fold the list into one counted disclosure. The variable and register pages
   * pass it with `registerOnly` (#1300, #1708): unassigned warnings name no
   * variable (ownership was not established, so none is inferred), and a register
   * can carry hundreds, so they read as one line instead of an expanded list. */
  collapsed?: boolean;
}

/** The data warnings the catalog holds for one coordinate: fetched on mount and
 * whenever the coordinate changes. `registerOnly` keeps the warnings that name no
 * variable; otherwise only those that do. */
export function ScopedDataWarnings({
  fqid,
  period = null,
  variant = null,
  representation = null,
  registerOnly = false,
  framed = true,
  collapsed = false,
}: Props) {
  const resource = useAsyncResource(
    fqid
      ? JSON.stringify([fqid, period, variant, representation, registerOnly])
      : null,
    () =>
      getWarnings(fqid, {
        period,
        variant,
        representation,
        unassigned_only: registerOnly,
      }),
  );
  if (!fqid) return null;
  if (resource.loading) {
    return (
      <div aria-busy="true">
        <Skeleton />
      </div>
    );
  }
  if (resource.error) {
    return (
      <StatusRow tone="error" live="alert">
        Could not load data warnings: {resource.error}. Check the catalog
        connection.
      </StatusRow>
    );
  }
  const warnings = (resource.data ?? []).filter((warning) =>
    registerOnly ? !warning.variable_fqid : !!warning.variable_fqid,
  );
  if (collapsed) {
    if (warnings.length === 0) return null;
    return (
      <details className={styles.collapsed}>
        <summary className={styles.summary}>
          {warnings.length} data{" "}
          {warnings.length === 1 ? "warning" : "warnings"} not assigned to a
          variable
        </summary>
        <p className="muted">
          The register records these limitations without naming the variable
          they concern, so they may apply to any of its variables.
        </p>
        <DataWarnings warnings={warnings} title={null} />
      </details>
    );
  }
  return (
    <DataWarnings
      warnings={warnings}
      framed={framed}
      title={
        registerOnly ? "Unassigned register data warnings" : "Data warnings"
      }
    />
  );
}
