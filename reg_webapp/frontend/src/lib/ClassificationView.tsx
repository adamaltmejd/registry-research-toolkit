import { useEffect, useRef, useState } from "react";
import { Tab, TabList, TabPanel, Tabs } from "react-aria-components";
import {
  type ClassificationGraphNode,
  type ClassificationShow,
  type ClassificationSubjectShow,
  type ConceptGroupMember,
  classificationGroupRef,
  type Derivation,
  type FamilyEdition,
  getGraph,
  getShow,
  type RelationshipGraph,
} from "./api";
import { ClassificationEditionGraph } from "./ClassificationEditionGraph";
import styles from "./ClassificationView.module.css";
import { catalogHref, leafSlug, nodeLabel } from "./catalog";
import { sharedShow } from "./catalog_names.svelte";
import { classificationGraphHasRenderableSuccession } from "./picker_graph";
import { router } from "./router";
import { SubjectSection, TechnicalDetails } from "./SubjectView";
import { Definitions } from "./ui/Definitions";
import { KeyValue } from "./ui/KeyValue";
import { Skeleton } from "./ui/Skeleton";
import { useAsyncResource } from "./use_async_resource";
import { ValueSetCodes } from "./ValueSetCodes";

/**
 * A classification edition ("Utbildningsnivå"): its head, the edition
 * succession graph, its codes, and the classifications it derives from or into.
 * No study window, lineage or warnings: classifications carry none. The graph
 * is its own failure domain: an error, an empty graph or a pending read omits
 * it and never blanks the page.
 */
export function ClassificationLeafView({ node }: { node: ClassificationShow }) {
  const graph = useAsyncResource(node.fqid, () => getGraph(node.fqid));
  return (
    <article className={styles.page}>
      <Definitions
        title={nodeLabel(node)}
        definitions={
          node.short_name
            ? [
                {
                  term: "Short name",
                  value: <code className={styles.mono}>{node.short_name}</code>,
                },
              ]
            : []
        }
      />
      {graph.data && <ClassificationEditionGraph graph={graph.data} />}
      <SubjectSection title="Codes">
        <ClassificationCodes key={node.fqid} fqid={node.fqid} />
      </SubjectSection>
      <RelatedClassifications node={node} />
    </article>
  );
}

/** An edition's codes (#609), read a page at a time with a server-side filter:
 * the same viewer a variable's value set uses, or the code tree where the
 * codes name their parents. Public classification codes, not row-level data.
 * Keyed on the FQID by the caller, so a new edition starts a fresh viewer. */
function ClassificationCodes({ fqid }: { fqid: string }) {
  return (
    <ValueSetCodes
      fqid={fqid}
      codeCount={null}
      emptyText="This classification has no codes."
    />
  );
}

/** Non-temporal derivation links; temporal succession is the graph's. A
 * derivation outside the catalog (no FQID) reads as text. */
function RelatedClassifications({ node }: { node: ClassificationShow }) {
  const derivedFrom = node.derived_from ?? [];
  const derivatives = node.derivatives ?? [];
  if (derivedFrom.length === 0 && derivatives.length === 0) return null;
  return (
    <SubjectSection title="Related classifications">
      <dl className={styles.related}>
        {derivedFrom.length > 0 && (
          <DerivationGroup term="Derived from" refs={derivedFrom} />
        )}
        {derivatives.length > 0 && (
          <DerivationGroup term="Derived classifications" refs={derivatives} />
        )}
      </dl>
    </SubjectSection>
  );
}

function DerivationGroup({
  term,
  refs,
}: {
  term: string;
  refs: readonly Derivation[];
}) {
  return (
    <div className={styles.relatedGroup}>
      <dt className="micro-label">{term}</dt>
      {refs.map((ref) => (
        <dd key={ref.fqid ?? `${ref.short_name}\u0000${ref.name}`}>
          {ref.fqid ? (
            <a className={styles.refLink} href={catalogHref(ref.fqid)}>
              {ref.short_name ?? ref.fqid}
            </a>
          ) : (
            <span className={styles.mono}>{ref.short_name}</span>
          )}
          <span className={styles.refName}>{ref.name}</span>
          {ref.note && <span className={styles.refNote}>{ref.note}</span>}
        </dd>
      ))}
    </div>
  );
}

interface EditionTab {
  fqid: string;
  label: string;
  name: string;
  meta: string;
  versionYear: number | null;
  isCurrent: boolean;
}

/** A member's label: its own curated short facet label (umbrellas are
 * axis-less), else its name, else its leaf slug. */
function memberLabel(member: ConceptGroupMember): string {
  return member.facets[0]?.label ?? member.name ?? leafSlug(member.fqid);
}

function graphNodeFor(
  graph: RelationshipGraph | null,
  fqid: string | null,
): ClassificationGraphNode | null {
  if (fqid == null || graph == null) return null;
  return (
    graph.nodes.find(
      (item): item is ClassificationGraphNode =>
        item.kind === "classification" && item.fqid === fqid,
    ) ?? null
  );
}

function editionTabs(
  node: ClassificationSubjectShow | null,
  graph: RelationshipGraph | null,
): EditionTab[] {
  if (node?.kind === "classification_family") {
    return node.editions.map((edition: FamilyEdition) => ({
      fqid: edition.fqid,
      label: edition.name ?? edition.slug,
      name: edition.name ?? edition.slug,
      meta: edition.is_current ? `${edition.slug}, current` : edition.slug,
      versionYear: edition.version_year ?? null,
      isCurrent: edition.is_current,
    }));
  }
  if (node?.kind === "classification_group") {
    return node.members.map((member) => {
      const point = graphNodeFor(graph, member.fqid);
      return {
        fqid: member.fqid,
        label: memberLabel(member),
        name: member.name ?? member.fqid,
        meta: leafSlug(member.fqid),
        versionYear: point?.version_year ?? null,
        isCurrent: point?.is_current ?? false,
      };
    });
  }
  return [];
}

/** The tab a page with no edition in its URL opens on: a current edition,
 * else the latest by version year, else the first. */
function latestTab(tabs: EditionTab[]): string | null {
  let best: EditionTab | null = null;
  for (const tab of tabs) {
    if (best == null || (tab.isCurrent && !best.isCurrent)) {
      best = tab;
    } else if (
      tab.isCurrent === best.isCurrent &&
      (tab.versionYear ?? Number.NEGATIVE_INFINITY) >
        (best.versionYear ?? Number.NEGATIVE_INFINITY)
    ) {
      best = tab;
    }
  }
  return best?.fqid ?? null;
}

async function showClassificationSubject(
  key: string,
  signal: AbortSignal,
): Promise<ClassificationSubjectShow> {
  const ref = classificationGroupRef(key);
  const shown = await getShow(ref, { signal });
  if (
    shown.kind !== "classification_group" &&
    shown.kind !== "classification_family"
  ) {
    throw new Error(`${ref} did not resolve to a classification group`);
  }
  return shown;
}

// A tab press navigates to the edition's own URL, which remounts this page; the
// remounted page puts focus back on the selected tab, or the keyboard user
// would land on <body>.
let refocusTabs = false;

/**
 * A classification umbrella or succession family by key (#756, #771): its head,
 * the edition graph, then one tab per edition holding that edition's codes.
 * Classifications have no study window, so there is no period picker and no
 * coverage. Catalog-global: no provider or register. A tab is a link to the
 * edition's page (#1116), which renders here with that tab active.
 */
export function ClassificationGroupView({
  groupKey,
  activeFqid = null,
  initialActiveNode = null,
}: {
  groupKey: string;
  activeFqid?: string | null;
  initialActiveNode?: ClassificationShow | null;
}) {
  const resource = useAsyncResource(groupKey, (signal) =>
    showClassificationSubject(groupKey, signal),
  );
  const node = resource.data;
  const graph = useAsyncResource(node ? groupKey : null, () =>
    getGraph(classificationGroupRef(groupKey)),
  );
  const tabs = editionTabs(node, graph.data);
  // A group's members take their years from the graph, so its default tab
  // waits for it; a family's editions carry their own.
  const tabsReady = node?.kind !== "classification_group" || !graph.loading;
  const [picked, setPicked] = useState<string | null>(null);
  const has = (fqid: string | null) =>
    fqid != null && tabs.some((tab) => tab.fqid === fqid);
  const selectedFqid = has(activeFqid)
    ? activeFqid
    : has(picked)
      ? picked
      : tabsReady
        ? latestTab(tabs)
        : null;

  const active = useAsyncResource(selectedFqid, async (signal) => {
    if (selectedFqid === null) return null;
    if (initialActiveNode?.fqid === selectedFqid) return initialActiveNode;
    const shown = await sharedShow(selectedFqid, signal);
    if (shown.kind !== "classification") {
      throw new Error(`${selectedFqid} did not resolve to a classification`);
    }
    return shown;
  });

  const focus = graphNodeFor(graph.data, selectedFqid);
  const focusedGraph =
    graph.data && focus ? { ...graph.data, focus_id: focus.id } : graph.data;
  const graphShown =
    focusedGraph != null &&
    classificationGraphHasRenderableSuccession(focusedGraph);
  const expectsGraph =
    tabs.length > 1 || (initialActiveNode?.family?.editions.length ?? 0) > 1;

  const tabList = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (refocusTabs && selectedFqid !== null) {
      refocusTabs = false;
      tabList.current
        ?.querySelector<HTMLElement>('[role="tab"][aria-selected="true"]')
        ?.focus();
    }
  }, [selectedFqid]);

  function selectEdition(fqid: string): void {
    if (fqid === selectedFqid) return;
    setPicked(fqid);
    refocusTabs = true;
    router.navigate(catalogHref(fqid));
  }

  if (resource.loading) {
    return (
      <section className={styles.page} aria-busy="true">
        <p className="visually-hidden">Loading…</p>
        <Skeleton width="min(24rem, 70%)" count={2} />
        <Skeleton variant="block" count={2} />
      </section>
    );
  }
  if (resource.error || !node) {
    return (
      <p className="banner-error" role="alert">
        <span aria-hidden="true">✕</span>{" "}
        {resource.status === 404 ? (
          <>
            No classification group or family <code>{groupKey}</code> in the
            catalog. Check the link, or open Classifications in the list.
          </>
        ) : (
          <>
            This page did not load ({resource.error}). Reload the page to try
            again.
          </>
        )}
      </p>
    );
  }

  const family = node.kind === "classification_family";
  return (
    <article className={styles.page}>
      <Definitions
        title={node.label}
        facets={[
          {
            label: "Kind",
            value: family ? "Classification family" : "Classification group",
          },
        ]}
      />
      {/* Build-derivation metadata, not researcher-facing: demoted. */}
      <TechnicalDetails>
        <KeyValue
          rows={
            family
              ? [
                  { label: "Family", value: node.key, mono: true },
                  { label: "Editions", value: String(node.editions.length) },
                ]
              : [
                  { label: "Group", value: node.key, mono: true },
                  ...(node.axes.length > 0
                    ? [
                        {
                          label: "Facets",
                          value: node.axes.map((a) => a.label).join(", "),
                        },
                      ]
                    : []),
                  { label: "Source", value: node.source },
                ]
          }
        />
      </TechnicalDetails>
      {/* The graph's slot is held while it loads, so the tabs below do not
          move when it lands; a branched graph scrolls inside the slot. */}
      {expectsGraph && graph.loading ? (
        <div
          className={`${styles.graphSlot} ${styles.graphReserve}`}
          aria-busy="true"
        >
          <Skeleton width="6rem" />
          <Skeleton variant="block" />
        </div>
      ) : (
        graphShown && (
          <div className={styles.graphSlot}>
            <ClassificationEditionGraph graph={focusedGraph} />
          </div>
        )
      )}
      {tabs.length > 0 && selectedFqid !== null && (
        <SubjectSection title="Codes">
          <Tabs
            selectedKey={selectedFqid}
            onSelectionChange={(key) => selectEdition(String(key))}
            keyboardActivation="manual"
            className={styles.tabs}
          >
            <div ref={tabList} className={styles.tabScroll}>
              <TabList
                aria-label="Classification editions"
                items={tabs}
                className={styles.tabList}
              >
                {(tab) => (
                  <Tab id={tab.fqid} className={styles.tab}>
                    <span className={styles.tabLabel} title={tab.name}>
                      {tab.label}
                    </span>
                    <span className={styles.tabMeta}>{tab.meta}</span>
                  </Tab>
                )}
              </TabList>
            </div>
            <TabPanel id={selectedFqid} className={styles.tabPanel}>
              {active.loading ? (
                <div aria-busy="true">
                  <span className="visually-hidden">Loading the codes…</span>
                  <Skeleton count={3} />
                </div>
              ) : active.error ? (
                <p className="banner-error" role="alert">
                  <span aria-hidden="true">✕</span> This edition did not load (
                  {active.error}). Reload the page to try again.
                </p>
              ) : (
                active.data && (
                  <ClassificationCodes
                    key={active.data.fqid}
                    fqid={active.data.fqid}
                  />
                )
              )}
            </TabPanel>
          </Tabs>
        </SubjectSection>
      )}
      {active.data && <RelatedClassifications node={active.data} />}
    </article>
  );
}
