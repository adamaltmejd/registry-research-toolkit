import type { VariantModel } from "./api";
import { variantsHref } from "./catalog";
import { SubjectSection } from "./SubjectView";
import { DataTable } from "./ui/DataTable";
import type { Column } from "./ui/types";
import styles from "./VariantBrowser.module.css";
import { groupVariants } from "./variants";

interface VariantRow {
  key: string;
  name: string;
  /** A second line under the name, not a column: the table stays at two
   * columns and stacks at 375 without breaking a slug mid-token. */
  slugs: string[];
  years: string;
}

const columns: Column<VariantRow>[] = [
  { key: "name", label: "Variant" },
  { key: "years", label: "Years", mono: true, align: "end" },
];

/**
 * The register page's compact variants summary (Y-79): one row per variant
 * family, with the slugs it is delivered under and the years it spans, over a
 * link to the variants page, which carries the register-version prose.
 *
 * A register whose only variant is `_default` (or that has none) has no
 * variant axis, so the section is left out; with a real variant the full list
 * shows, `_default` included (#673/M4).
 */
export function VariantsSummary({
  registerFqid,
  variants,
}: {
  registerFqid: string;
  variants: readonly VariantModel[];
}) {
  if (!variants.some((variant) => variant.slug !== "_default")) return null;
  const rows: VariantRow[] = groupVariants(variants).map((group) => ({
    key: group.key,
    name: group.label,
    slugs: group.segments.map((segment) => segment.variant.slug),
    years: group.span,
  }));
  return (
    <SubjectSection title="Variants">
      <DataTable
        framed
        columns={columns}
        rows={rows}
        getRowId={(row) => row.key}
        renderCell={(row, column) =>
          column.key === "name" ? (
            <>
              <span className={styles.summaryName}>{row.name}</span>
              {row.slugs.map((slug) => (
                <code key={slug} className={styles.summarySlug}>
                  {slug}
                </code>
              ))}
            </>
          ) : (
            row.years
          )
        }
      />
      <p className={styles.summaryLink}>
        <a href={variantsHref(registerFqid)}>All variant details</a>
      </p>
    </SubjectSection>
  );
}
