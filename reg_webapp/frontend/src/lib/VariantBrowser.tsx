import { getShow } from "./api";
import { catalogHref, nodeLabel } from "./catalog";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import type { KeyValueRow } from "./ui/types";
import { useAsyncResource } from "./use_async_resource";
import styles from "./VariantBrowser.module.css";
import {
  foldVersions,
  groupVariants,
  showsDistinctGroup,
  type Version,
} from "./variants";

/** Only the rows that have a value. */
function present(...rows: KeyValueRow[]): KeyValueRow[] {
  return rows.filter((row) => row.value);
}

function versionRows(version: Version): KeyValueRow[] {
  return present(
    { label: "Description", value: version.description ?? undefined },
    {
      label: "Measurement",
      value: version.measurement_information ?? undefined,
    },
  );
}

type Population = NonNullable<Version["populations"]>[number];
type ObjectType = NonNullable<Version["object_types"]>[number];

function populationRows(population: Population): KeyValueRow[] {
  return present(
    { label: "Name", value: population.name },
    { label: "Definition", value: population.definition ?? undefined },
    { label: "Comment", value: population.comment ?? undefined },
    // A bare delivery year: mono, like every other year on the page.
    {
      label: "Date range",
      value: population.date_range ?? undefined,
      mono: true,
    },
  );
}

function objectTypeRows(objectType: ObjectType): KeyValueRow[] {
  return present(
    { label: "Name", value: objectType.name },
    { label: "Definition", value: objectType.definition ?? undefined },
  );
}

/**
 * A register's variants page (Y-79), `/catalog/<provider>/<register>/variants`:
 * the register-version prose the register page's summary links to. The variant
 * axis is a register sub-resource, not an FQID segment, so it has its own route.
 *
 * Versions are folded (`variants.ts`): SCB delivers one `register_version` per
 * year, each repeating the last one's prose with the year moved on, so a run
 * that says the same thing is one block over its years, and a changed
 * description, population or object type opens the next. Unlike the summary,
 * this page shows a `_default` variant: its prose is real, and this
 * deep-linkable page is the only place to read it.
 */
export function VariantBrowser({ registerFqid }: { registerFqid: string }) {
  const resource = useAsyncResource(registerFqid, (signal) =>
    getShow(registerFqid, { signal }),
  );
  const register = resource.data?.kind === "register" ? resource.data : null;
  const entries = groupVariants(register?.variants ?? []);

  return (
    <article className={styles.page}>
      <Definitions
        title="Variants"
        memberOf={[
          {
            relation: "Register",
            label: register ? nodeLabel(register) : registerFqid,
            href: catalogHref(registerFqid),
          },
        ]}
      />
      {resource.loading ? (
        <div className={styles.loading} aria-busy="true">
          <span className="visually-hidden">Loading variants…</span>
          <Skeleton count={3} />
        </div>
      ) : resource.error ? (
        <StatusRow tone="error" live="alert">
          The variants did not load ({resource.error}). Reload the page to try
          again.
        </StatusRow>
      ) : entries.length === 0 ? (
        <EmptyState
          title="This register has no variants."
          description="Its columns are on the register's page."
        />
      ) : (
        <ul className={styles.variants}>
          {entries.map((entry) => (
            <li key={entry.key} className={styles.entry}>
              <h2 className={styles.entryName}>{entry.label}</h2>
              {entry.isFamily ? (
                // A curated succession family reads as one variant whose
                // frame changed: one line per segment, with its years.
                <ul className={styles.segments}>
                  {entry.segments.map((segment) => (
                    <li key={segment.variant.slug}>
                      {segment.name}{" "}
                      <span className={styles.years}>{segment.span}</span>
                    </li>
                  ))}
                </ul>
              ) : (
                showsDistinctGroup(
                  entry.label,
                  entry.segments[0]?.variant.display_group,
                ) && (
                  <p className={styles.entryMeta}>
                    {entry.segments[0]?.variant.display_group}
                  </p>
                )
              )}
              {entry.segments.map((segment) => (
                <section key={segment.variant.slug} className={styles.segment}>
                  {/* The coordinate a project source extracts; the years are
                      on the blocks below. */}
                  <h3 className={styles.slug}>
                    <code>{segment.variant.slug}</code>
                  </h3>
                  {segment.variant.description && (
                    <p className={styles.description}>
                      {segment.variant.description}
                    </p>
                  )}
                  {foldVersions(segment.variant.versions ?? []).map((block) => (
                    <VersionBlockView
                      key={`${block.label}\u0000${block.version.name ?? ""}`}
                      label={block.label}
                      count={block.count}
                      version={block.version}
                    />
                  ))}
                </section>
              ))}
            </li>
          ))}
        </ul>
      )}
    </article>
  );
}

function VersionBlockView({
  label,
  count,
  version,
}: {
  label: string;
  count: number;
  version: Version;
}) {
  const rows = versionRows(version);
  const populations = version.populations ?? [];
  const objectTypes = version.object_types ?? [];
  return (
    <section className={styles.version}>
      <h4 className={styles.versionTitle}>
        <span className="micro-label">
          {count === 1 ? "Version" : "Versions"}
        </span>
        <span className={styles.versionName}>{label}</span>
      </h4>
      {count > 1 && version.name && (
        // The block spans years but prints one delivery's text verbatim,
        // years inside it included, so it names which.
        <p className={styles.asDelivered}>
          Wording as delivered for <code>{version.name}</code>.
        </p>
      )}
      {rows.length > 0 && <KeyValue rows={rows} />}
      {populations.length > 0 && (
        <div className={styles.metadata}>
          <h5 className={styles.metadataHeading}>Population</h5>
          {populations.map((population) => (
            <KeyValue key={population.name} rows={populationRows(population)} />
          ))}
        </div>
      )}
      {objectTypes.length > 0 && (
        <div className={styles.metadata}>
          <h5 className={styles.metadataHeading}>Object type</h5>
          {objectTypes.map((objectType) => (
            <KeyValue key={objectType.name} rows={objectTypeRows(objectType)} />
          ))}
        </div>
      )}
    </section>
  );
}
