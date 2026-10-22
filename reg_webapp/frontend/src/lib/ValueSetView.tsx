import {
  Fragment,
  type ReactNode,
  type SyntheticEvent,
  useCallback,
  useState,
} from "react";
import type { DenseIntegerRangeModel, VariableStateModel } from "./api";
import {
  catalogHref,
  conformanceNeedsNotice,
  type DistinctValueSet,
  distinctValueSets,
  formatStateWindow,
  formatWindow,
  humanizeClassificationSlug,
  matchesFilter,
  type StateConformance,
  type ValueSetTechnicalChange,
  valueSetKeyForColumn,
  windowTitle,
} from "./catalog";
import { FilterInput } from "./FilterInput";
import { Button } from "./ui/Button";
import { Tag } from "./ui/Tag";
import { ValueSetCodes } from "./ValueSetCodes";
import styles from "./ValueSetView.module.css";

type Usage = DistinctValueSet["usages"][number];

interface Props {
  /** The variable whose states these are: the ref its codes are read by. */
  fqid: string;
  states: VariableStateModel[];
  /** True when these are the `?period`-narrowed subset (drives empty wording). */
  narrowed: boolean;
  /** The period-resolved subset when `states` carries the full history. */
  scopeStates: VariableStateModel[] | null;
  /** A `?codes=<variant>::<column>` deep link (#905): seeds the isolation onto
   * the value set that (variant, column) resolves to and scrolls it into view. */
  focusColumn: string | null;
  focusVariant: string | null;
  /** Facts the containing variable page already shows, so a state repeats only
   * what differs. */
  common: {
    name: string | null;
    description: string | null;
    definition: string | null;
    unit: string | null;
  };
}

const POOLED_TITLE =
  "Pooled source edition: this state covers the whole pooled range; annual availability inside it is not inferred.";

function PooledTag() {
  return (
    <span title={POOLED_TITLE}>
      <Tag>pooled</Tag>
    </span>
  );
}

function usageWindowLabels(
  spans: Usage["spans"],
): { label: string; pooled: boolean }[] {
  return spans.flatMap((sp) => {
    const label = formatWindow(sp.from, sp.to);
    return label === null ? [] : [{ label, pooled: sp.pooled }];
  });
}

/** The title of a distinct value set, from trustworthy data only: a declared
 * classification's humanized slug, else its delivered windows. The provider's
 * version name is shown beside it, never as it (#1301). */
function valueSetLabel(vs: DistinctValueSet): string {
  if (vs.classificationSlugs.length > 0) {
    return vs.classificationSlugs.map(humanizeClassificationSlug).join(", ");
  }
  const span = usageWindowLabels(vs.coverage)
    .map((w) => w.label)
    .join(", ");
  return span ? `Value set: ${span}` : "Value set";
}

function changeDateLabel(at: string): string {
  return /^\d{4}-01-01$/.test(at) ? at.slice(0, 4) : at;
}

function technicalChangeLabel(change: ValueSetTechnicalChange): string {
  return `changed ${changeDateLabel(change.at)}: ${change.notes.join("; ")}`;
}

function conformanceKey(c: StateConformance): string {
  return `${c.stateId}:${c.aliasWindowFrom ?? ""}:${c.column ?? ""}:${c.verdict.declared_classification_slug}`;
}

type Common = Props["common"];

/** A state's text facts that differ from the variable's own. */
function stateTextFacts(state: VariableStateModel, common: Common) {
  return (
    [
      ["Name", state.name, common.name],
      ["Description", state.description, common.description],
      ["Definition", state.definition, common.definition],
      ["Unit", state.measurement_unit, common.unit],
    ] as const
  ).flatMap(([label, value, shared]) =>
    value && value !== shared ? [{ label, value }] : [],
  );
}

/** The states of one usage whose facts differ from the variable's, each under
 * its delivery column (or window), with the window added where a label
 * repeats. */
function StateDefinitions({
  states,
  common,
}: {
  states: VariableStateModel[];
  common: Common;
}) {
  const base = (s: VariableStateModel) =>
    s.delivery_column_name ?? formatStateWindow(s) ?? "Operational definition";
  const counts = new Map<string, number>();
  for (const s of states) counts.set(base(s), (counts.get(base(s)) ?? 0) + 1);
  const label = (s: VariableStateModel) => {
    const window = formatStateWindow(s);
    return (counts.get(base(s)) ?? 0) > 1 && window
      ? `${base(s)} (${window})`
      : base(s);
  };
  return (
    <dl className={styles.definitions}>
      {states.map((s) => (
        <div
          key={[
            s.state_id,
            s.delivery_column_name ?? "",
            s.valid_from,
            s.valid_to,
            s.value_set_version_label ?? "",
          ].join("|")}
          className={styles.definition}
        >
          <dt>
            {s.delivery_column_name ? <code>{label(s)}</code> : label(s)}
            {s.pooled && <PooledTag />}
          </dt>
          <dd>
            {stateTextFacts(s, common).map((fact) => (
              <span key={fact.label} className={styles.fact}>
                <span className="micro-label">{fact.label}:</span> {fact.value}
              </span>
            ))}
            {s.operational_definition && (
              <span className={styles.fact}>{s.operational_definition}</span>
            )}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function ClassificationLinks({ slugs }: { slugs: string[] }) {
  return slugs.map((slug, index) => (
    <span key={slug}>
      {index > 0 && ", "}
      <a href={catalogHref(`class/${slug}`)}>
        {humanizeClassificationSlug(slug)}
      </a>
    </span>
  ));
}

function DenseIntegerRange({
  range,
  count,
}: {
  range: DenseIntegerRangeModel;
  count: number;
}) {
  return (
    <p className={styles.numericRange}>
      Integer values <code>{range.min}</code>-<code>{range.max}</code>{" "}
      <span className="muted">({count} values)</span>
    </p>
  );
}

/**
 * The value-set / coding viewer for a variable's states (#905). Presentation
 * only: it never fetches states, never navigates and carries no resolution
 * state — the leaf decides which states it gets.
 *
 * - One state: its detail (variant, validity, version name, the differing
 *   facts, declared classifications, then its codes).
 * - Several: the distinct value sets (#668), filterable, each expandable and
 *   isolatable; a classification value set links out rather than dumping its
 *   codes. With a period, out-of-period value sets are collapsed below (#744).
 * - None: "no state delivered for this period", not an error.
 */
export function ValueSetView({
  fqid,
  states,
  narrowed,
  scopeStates,
  focusColumn,
  focusVariant,
  common,
}: Props) {
  // Isolation is keyed by the value set's stable key; null is the union. A
  // deep link seeds it. Reset with the rest of this state by the parent's key.
  const [isolatedKey, setIsolatedKey] = useState<string | null>(() =>
    focusColumn != null
      ? valueSetKeyForColumn(states, focusColumn, focusVariant)
      : null,
  );
  const [filter, setFilter] = useState("");
  // Which code disclosures the reader opened (Y-46): a shut one holds no
  // ValueSetCodes, since mounting one is what issues the request.
  const [openPanels, setOpenPanels] = useState<Record<string, boolean>>({});
  const scrollToFocus = useCallback(
    (el: HTMLDivElement | null) => {
      if (el && focusColumn != null) el.scrollIntoView({ block: "nearest" });
    },
    [focusColumn],
  );

  // One state drives the detail — unless a narrowed view's period delivered
  // nothing, when that lone state is out of era (#905).
  const single =
    states.length === 1 &&
    !(narrowed && scopeStates !== null && scopeStates.length === 0)
      ? states[0]
      : null;
  const valueSets = distinctValueSets(states);
  const scopedValueSets =
    scopeStates === null ? null : distinctValueSets(scopeStates);
  const scopeKeys =
    scopedValueSets === null
      ? null
      : new Set(scopedValueSets.map((vs) => vs.key));
  const inPeriod = (vs: DistinctValueSet) =>
    scopeKeys === null || scopeKeys.has(vs.key);
  const visibleConformances = (vs: DistinctValueSet): StateConformance[] =>
    scopedValueSets === null || !inPeriod(vs)
      ? vs.conformances
      : (scopedValueSets.find((s) => s.key === vs.key)?.conformances ?? []);

  // The provider's own name for a coding is a low-trust per-delivery label
  // (#1301): a muted line attributed to the provider, never a title.
  const sourceVersionName = `${(fqid.split("/")[0] ?? "").toUpperCase() || "Source"} version name`;

  const isolated =
    isolatedKey == null
      ? null
      : (valueSets.find((vs) => vs.key === isolatedKey) ?? null);
  const matching = valueSets.filter((vs) =>
    matchesFilter(filter, valueSetLabel(vs), vs.versionLabel, ...vs.variants),
  );
  const shownValueSets = matching.filter(inPeriod);
  const collapsedValueSets =
    scopeKeys === null ? [] : matching.filter((vs) => !inPeriod(vs));
  // Before the text filter (#905): whether the period delivered any value set.
  const hasInPeriodValueSets = scopeKeys === null || valueSets.some(inPeriod);

  const panel = (key: string) => ({
    open: openPanels[key] ?? false,
    onToggle: (event: SyntheticEvent<HTMLDetailsElement>) => {
      const open = event.currentTarget.open;
      setOpenPanels((current) =>
        (current[key] ?? false) === open
          ? current
          : { ...current, [key]: open },
      );
    },
  });

  /** The codes of one state's value set. A coding of a coded alias window is
   * that window's: its column and start name it. */
  function codesOf(state: VariableStateModel | undefined, codeCount: number) {
    return (
      <ValueSetCodes
        fqid={fqid}
        stateId={state?.state_id ?? ""}
        column={
          state?.coding_window_from != null ? state.delivery_column_name : null
        }
        aliasWindowFrom={state?.coding_window_from ?? null}
        codeCount={codeCount}
        filterLabel="Filter value set"
        filterPlaceholder="Filter value set…"
      />
    );
  }

  // Any state of a distinct value set answers the same codes.
  const stateOf = (vs: DistinctValueSet) =>
    states.find((s) => s.value_set_id === vs.valueSetId);

  /** One stored conformance verdict. `scope` says which coding, period and
   * variants it speaks for where an entry groups several; null where the
   * single-state detail already says so. A clean verdict renders nothing. */
  function conformanceNotice(
    source: StateConformance,
    scope: { coding: string | null } | null,
  ): ReactNode {
    const conf = source.verdict;
    if (!conformanceNeedsNotice(conf)) return null;
    const slug = conf.declared_classification_slug;
    const where = `${source.stateId}:${source.aliasWindowFrom ?? ""}:${source.column ?? ""}:${slug}`;
    const period = usageWindowLabels(source.spans)
      .map((w) => w.label)
      .join(", ");
    const variants = source.variants.filter((v) => v !== "_default");
    const parts = [
      {
        partition: "canonical" as const,
        label: "Matching source codes",
        count: conf.matched_code_count,
      },
      {
        partition: "nonstandard" as const,
        label: "Nonstandard source codes",
        count: conf.nonstandard_code_count,
      },
      {
        partition: "sentinels" as const,
        label: "Special source codes",
        count: conf.sentinel_code_count,
      },
    ];
    return (
      <div className={styles.notice}>
        <Tag tone="warn" glyph="▲">
          Source extensions
        </Tag>
        <p>
          The source declares{" "}
          <a href={catalogHref(`class/${slug}`)}>
            {humanizeClassificationSlug(slug)}
          </a>
          . {conf.matched_code_count} source{" "}
          {conf.matched_code_count === 1 ? "code matches" : "codes match"} this
          classification; {conf.nonconforming_code_count}{" "}
          {conf.nonconforming_code_count === 1
            ? "is a source extension"
            : "are source extensions"}
          . Source extensions are not official classification codes.
        </p>
        {scope && (scope.coding || period || variants.length > 0) && (
          // The years the verdict was RECORDED over, often narrower than the
          // coding's own usage window above: said in words so the two windows
          // are not read as the same claim.
          <p className={styles.noticeScope}>
            {scope.coding && <span>{scope.coding}</span>}
            {period && <span className="muted">recorded for {period}</span>}
            {variants.map((v) => (
              <code key={v} className={styles.variant}>
                {v}
              </code>
            ))}
          </p>
        )}
        {source.valueSetId !== null &&
          parts.map((part) => {
            if (part.count <= 0) return null;
            const key = `${part.partition}:${where}`;
            const disclosure = panel(key);
            const lower = part.label.toLowerCase();
            return (
              <details
                key={key}
                className={styles.noticeDetails}
                {...disclosure}
              >
                <summary className={styles.summary}>
                  {part.label} ({part.count})
                </summary>
                {disclosure.open && (
                  <ValueSetCodes
                    fqid={fqid}
                    stateId={source.stateId}
                    classification={`class/${slug}`}
                    column={source.column}
                    aliasWindowFrom={source.aliasWindowFrom}
                    partition={part.partition}
                    codeCount={part.count}
                    filterLabel={`Filter ${lower}`}
                    filterPlaceholder={`Filter ${lower}…`}
                  />
                )}
              </details>
            );
          })}
      </div>
    );
  }

  /** Which variants and spans use a distinct value set (#668), one line per
   * variant, with the per-state facts that differ from the variable's. */
  function usage(vs: DistinctValueSet): ReactNode {
    return (
      <ul className={styles.usage}>
        {vs.usages.map((u) => {
          const changes = u.spans.flatMap((span) => span.changes ?? []);
          const defined = u.states.filter(
            (s) =>
              s.operational_definition || stateTextFacts(s, common).length > 0,
          );
          const spans = usageWindowLabels(u.spans);
          const variant = u.variant === "_default" ? null : u.variant;
          if (!variant && !spans.length && !changes.length && !defined.length) {
            return null;
          }
          return (
            <li key={u.variant} className={styles.usageItem}>
              {variant && <code className={styles.variant}>{variant}</code>}
              {spans.length > 0 && (
                <span className="muted">
                  {spans.map((w, i) => (
                    // A window label can repeat across spans.

                    // biome-ignore lint/suspicious/noArrayIndexKey: positional

                    // oxlint-disable-next-line react/no-array-index-key
                    <span key={`${w.label}|${i}`}>
                      {i > 0 && ", "}
                      {w.label}
                      {w.pooled && <PooledTag />}
                    </span>
                  ))}
                </span>
              )}
              {changes.length > 0 && (
                <span className={styles.changes}>
                  {changes.map((change) => (
                    <span key={`${change.at}:${change.notes.join("|")}`}>
                      {technicalChangeLabel(change)}
                    </span>
                  ))}
                </span>
              )}
              {defined.length > 0 && (
                <StateDefinitions states={defined} common={common} />
              )}
            </li>
          );
        })}
      </ul>
    );
  }

  function sourceName(vs: DistinctValueSet): ReactNode {
    return vs.classificationSlugs.length === 0 && vs.versionLabel ? (
      <p className={`muted ${styles.sourceName}`}>
        {sourceVersionName}: {vs.versionLabel}
      </p>
    ) : null;
  }

  function valueSetBody(vs: DistinctValueSet): ReactNode {
    if (vs.classificationSlugs.length > 0) {
      return (
        <>
          <p className={styles.classification}>
            The source declares{" "}
            {vs.classificationSlugs.length === 1
              ? "this classification"
              : "these classifications"}
            : <ClassificationLinks slugs={vs.classificationSlugs} />. Source
            codes retain their own labels.
          </p>
          {visibleConformances(vs).map((c) => (
            <div key={conformanceKey(c)}>
              {conformanceNotice(c, { coding: c.versionLabel })}
            </div>
          ))}
        </>
      );
    }
    return (
      <>
        {visibleConformances(vs).map((c) => (
          <div key={conformanceKey(c)}>
            {conformanceNotice(c, { coding: null })}
          </div>
        ))}
        {vs.valueSetId !== null &&
          vs.summary &&
          (vs.summary.integer_range ? (
            <DenseIntegerRange
              range={vs.summary.integer_range}
              count={vs.summary.code_count}
            />
          ) : (
            codesOf(stateOf(vs), vs.summary.code_count)
          ))}
      </>
    );
  }

  function valueSetRow(vs: DistinctValueSet): ReactNode {
    const summary = vs.summary;
    const plain = vs.classificationSlugs.length === 0;
    const codesKey = `codes:${vs.key}`;
    const codes = panel(codesKey);
    return (
      <li key={vs.key} className={styles.row}>
        <div className={styles.rowHead}>
          {plain ? (
            <>
              <span className={styles.rowLabel}>{valueSetLabel(vs)}</span>
              {summary && (
                <span className={`muted ${styles.small}`}>
                  ({summary.code_count})
                </span>
              )}
              {summary?.integer_range && (
                <span className={`muted ${styles.range}`}>
                  {summary.integer_range.min}-{summary.integer_range.max}
                </span>
              )}
            </>
          ) : (
            // A classification value set links out, never dumps its codes.
            <span className={styles.rowLabel}>
              <ClassificationLinks slugs={vs.classificationSlugs} />
            </span>
          )}
          <span className={styles.rowAction}>
            <Button size="sm" onPress={() => setIsolatedKey(vs.key)}>
              Isolate
            </Button>
          </span>
        </div>
        {sourceName(vs)}
        {usage(vs)}
        {visibleConformances(vs).map((c) => (
          <div key={conformanceKey(c)}>
            {conformanceNotice(c, { coding: plain ? null : c.versionLabel })}
          </div>
        ))}
        {plain &&
          vs.valueSetId !== null &&
          summary &&
          (summary.integer_range ? (
            <DenseIntegerRange
              range={summary.integer_range}
              count={summary.code_count}
            />
          ) : summary.code_count === 0 ? (
            // A known-empty coding: the shared panel's one sentence, nothing
            // to open.
            codesOf(stateOf(vs), 0)
          ) : (
            // Only once opened (Y-46): a long history costs one bounded read
            // of the coding the reader asked for.
            <details className={styles.codes} {...codes}>
              <summary className={styles.summary}>
                Values ({summary.code_count})
              </summary>
              {codes.open && codesOf(stateOf(vs), summary.code_count)}
            </details>
          ))}
      </li>
    );
  }

  if (states.length === 0) {
    return (
      <p className="muted">
        {narrowed
          ? "No state delivered for this period."
          : "This variable has no recorded states."}
      </p>
    );
  }

  if (single) {
    const s = single;
    const validWindow = formatStateWindow(s);
    const summary = s.value_set_id !== null ? s.value_set_summary : null;
    return (
      <div>
        <dl className={styles.meta}>
          {s.variant !== "_default" && (
            <>
              <dt className="micro-label">Variant</dt>
              <dd>
                <code>{s.variant}</code>
              </dd>
            </>
          )}
          {validWindow && (
            <>
              <dt className="micro-label">Valid</dt>
              {/* #309/#321: the raw ISO window stays on the tooltip. */}
              <dd title={windowTitle(s.valid_from, s.valid_to, s.period_scope)}>
                {validWindow} {s.pooled && <PooledTag />}
              </dd>
            </>
          )}
          {s.value_set_version_label && (
            <>
              <dt className="micro-label">{sourceVersionName}</dt>
              <dd>{s.value_set_version_label}</dd>
            </>
          )}
          {stateTextFacts(s, common).map((fact) => (
            <Fragment key={fact.label}>
              <dt className="micro-label">{fact.label}</dt>
              <dd>{fact.value}</dd>
            </Fragment>
          ))}
          {s.operational_definition && (
            <>
              <dt className="micro-label">Operational definition</dt>
              <dd>{s.operational_definition}</dd>
            </>
          )}
        </dl>
        {s.classifications.map((classification) => (
          <div key={classification.slug}>
            <p className={styles.classification}>
              The source declares{" "}
              <a href={catalogHref(`class/${classification.slug}`)}>
                {classification.short_name ||
                  classification.name ||
                  humanizeClassificationSlug(classification.slug)}
              </a>
              .
            </p>
            {classification.conformance &&
              conformanceNotice(
                {
                  verdict: classification.conformance,
                  stateId: s.state_id,
                  column:
                    s.coding_window_from != null
                      ? s.delivery_column_name
                      : null,
                  aliasWindowFrom: s.coding_window_from ?? null,
                  valueSetId: s.value_set_id,
                  versionLabel: s.value_set_version_label,
                  variants: [s.variant],
                  spans: [
                    { from: s.valid_from, to: s.valid_to, pooled: s.pooled },
                  ],
                },
                null,
              )}
          </div>
        ))}
        {summary && (
          <>
            <h3 className={styles.heading}>
              Value set <span className="muted">({summary.code_count})</span>
            </h3>
            {summary.integer_range ? (
              <DenseIntegerRange
                range={summary.integer_range}
                count={summary.code_count}
              />
            ) : (
              codesOf(s, summary.code_count)
            )}
          </>
        )}
      </div>
    );
  }

  return (
    <div className={styles.multi}>
      <p className="muted">
        {narrowed && scopeKeys !== null && !hasInPeriodValueSets
          ? "No state delivered for this period. Historical value sets outside this period are collapsed below."
          : `${valueSets.length} ${valueSets.length === 1 ? "value set" : "value sets"} across ${states.length} states over time.`}
      </p>
      {isolated ? (
        <div ref={scrollToFocus}>
          <Button size="sm" onPress={() => setIsolatedKey(null)}>
            All value sets
          </Button>
          <h3 className={styles.heading}>
            {isolated.classificationSlugs.length > 0 ? (
              <ClassificationLinks slugs={isolated.classificationSlugs} />
            ) : (
              <>
                {valueSetLabel(isolated)}
                {isolated.summary && (
                  <span className="muted">
                    {" "}
                    ({isolated.summary.code_count})
                  </span>
                )}
                {isolated.summary?.integer_range && (
                  <span className={`muted ${styles.range}`}>
                    {" "}
                    {isolated.summary.integer_range.min}-
                    {isolated.summary.integer_range.max}
                  </span>
                )}
              </>
            )}
          </h3>
          {sourceName(isolated)}
          {valueSetBody(isolated)}
          <h4 className={`micro-label ${styles.usageHeading}`}>Used by</h4>
          {usage(isolated)}
        </div>
      ) : (
        <>
          {valueSets.length > 1 && (
            <FilterInput
              value={filter}
              onChange={setFilter}
              total={valueSets.length}
              shown={shownValueSets.length + collapsedValueSets.length}
              label="Filter value sets"
              placeholder="Filter value sets…"
            />
          )}
          {shownValueSets.length === 0 && collapsedValueSets.length === 0 && (
            <p className="muted">No value set matches the filter.</p>
          )}
          <ul className={styles.list}>{shownValueSets.map(valueSetRow)}</ul>
          {collapsedValueSets.length > 0 && (
            <details className={styles.outOfPeriod}>
              <summary className={styles.summary}>
                {collapsedValueSets.length}{" "}
                {collapsedValueSets.length === 1 ? "value set" : "value sets"}{" "}
                outside this period
              </summary>
              <ul className={styles.list}>
                {collapsedValueSets.map(valueSetRow)}
              </ul>
            </details>
          )}
        </>
      )}
    </div>
  );
}
