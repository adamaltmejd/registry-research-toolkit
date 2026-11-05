import type { ClassFamiliesShow, ClassFamilyRef } from "./api";
import styles from "./CatalogNodeView.module.css";
import listStyles from "./ClassificationsView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { CLASSIFICATIONS_LABEL } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";

/** The list of classifications (`/catalog/class`): each by name in Swedish
 * collation, its description, and its versions with their years, under
 * their parts where it has more than one. */
export function ClassificationsView({ node }: { node: ClassFamiliesShow }) {
  const families = [...node.families].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
  return (
    <article className={styles.page}>
      <Definitions title={CLASSIFICATIONS_LABEL} />
      <Panel title="All classifications" flush={families.length > 0}>
        {families.length === 0 ? (
          <EmptyState title="The catalog has no classifications." />
        ) : (
          <ul className={`${styles.plainList} ${listStyles.list}`}>
            {families.map((family) => (
              <li key={family.fqid} className={listStyles.entry}>
                <h3 className={listStyles.name}>
                  <a className="link" href={catalogHref(family.fqid)}>
                    {family.name}
                  </a>
                </h3>
                {family.description && (
                  <p className={listStyles.description}>{family.description}</p>
                )}
                <Versions family={family} />
              </li>
            ))}
          </ul>
        )}
      </Panel>
    </article>
  );
}

/** A classification's versions in start order, grouped under its parts (in
 * the order each part first starts) when it has more than one; a lone version is only its years. */
function Versions({ family }: { family: ClassFamilyRef }) {
  // One version: its name would repeat the classification's, so only its
  // years.
  if (family.versions.length === 1)
    return (
      <p className={listStyles.single}>
        {catalogPeriodLabel(family.versions[0].period)}
      </p>
    );
  const parts = [
    ...new Set(family.versions.map((version) => version.part ?? "")),
  ];
  const list = (versions: ClassFamilyRef["versions"], part = "") => (
    <ul
      className={`${styles.plainList} ${listStyles.versions}`}
      aria-label={`Versions of ${family.name}`}
    >
      {versions.map((version) => {
        const years = catalogPeriodLabel(version.period);
        // A version named as its part (Sektorkod (RAMS/LISA)) would repeat
        // the heading above it: its years are the link.
        return version.name === part ? (
          <li key={version.fqid}>
            <a
              className="link"
              href={catalogHref(version.fqid)}
              aria-label={`${version.name}, ${years}`}
            >
              {years}
            </a>
          </li>
        ) : (
          <li key={version.fqid}>
            <a className="link" href={catalogHref(version.fqid)}>
              {version.name}
            </a>{" "}
            <span className={listStyles.years}>{years}</span>
          </li>
        );
      })}
    </ul>
  );
  if (parts.length <= 1) return list(family.versions);
  return (
    <div className={listStyles.parts}>
      {parts.map((part) => (
        <section key={part}>
          {part && <h4 className={listStyles.part}>{part}</h4>}
          {list(
            family.versions.filter((version) => (version.part ?? "") === part),
            part,
          )}
        </section>
      ))}
    </div>
  );
}
