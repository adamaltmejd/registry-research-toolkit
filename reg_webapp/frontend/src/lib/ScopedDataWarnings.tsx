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
  /** Several variants in place of `variant`: a family source's members (#1840),
   * whose warnings are read per member and merged, each once. */
  variants?: readonly string[];
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
  variants,
  representation = null,
  registerOnly = false,
  framed = true,
  collapsed = false,
}: Props) {
  const scoped = variants && variants.length > 0 ? variants : [variant];
  const resource = useAsyncResource(
    fqid
      ? JSON.stringify([fqid, period, scoped, representation, registerOnly])
      : null,
    async () => {
      const read = (v: string | null) =>
        getWarnings(fqid, {
          period,
          variant: v,
          representation,
          unassigned_only: registerOnly,
        });
      if (scoped.length === 1) return read(scoped[0]);
      // simplify: one read per member; families have two members today. Give
      // `warnings` a variant list if a family ever grows past a handful.
      const merged = new Map(
        (await Promise.all(scoped.map(read)))
          .flat()
          .map((w) => [w.warning_id, w]),
      );
      // Code-point order, as one read orders them.
      return [...merged.values()].sort((a, b) =>
        a.warning_id < b.warning_id ? -1 : a.warning_id > b.warning_id ? 1 : 0,
      );
    },
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
