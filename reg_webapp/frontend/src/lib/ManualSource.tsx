import { useRef, useState } from "react";
import { errMessage } from "./api";
import { sourcePeriodLabel } from "./catalog_period";
import { LocatedIssue } from "./Finding";
import { focusAfterRemoval } from "./focus_after_removal";
import { LaneList, type LaneModel } from "./LaneGraph";
import styles from "./ManualSource.module.css";
import { codeCount, MANUAL_LABEL, manualColumns } from "./manual_items";
import {
  type ProjectData,
  type ProjectManualBinding,
  type ProjectManualSource,
} from "./project_data";
import sourceStyles from "./SourceCard.module.css";
import {
  commitSourceEdit,
  removalRemovesSource,
  removeBinding,
  removeSource,
} from "./source_edits";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { TypePill } from "./ui/TypePill";
import { issueId, type ValidationIssue } from "./validation";

/** Each column as a plain row: its name, its stated type and its code count.
 * Nothing is read from the catalog, so there is no lane and no coverage. */
export function ManualColumnList({
  label,
  columns,
  labelled,
  onRemove,
}: {
  label: string;
  columns: { binding: ProjectManualBinding; index: number }[];
  /** Each row says it was entered by hand: where catalog columns sit beside it. */
  labelled: boolean;
  onRemove: (index: number, name: string) => void;
}) {
  const lanes: LaneModel[] = columns.map(({ binding, index }) => {
    const name = binding.manual.column;
    const codes = codeCount(binding);
    return {
      id: `manual:${index}`,
      name,
      sub: labelled ? { text: MANUAL_LABEL } : null,
      action: (
        <Button
          variant="ghost"
          aria-label={`Remove ${name} from ${label}`}
          onPress={() => onRemove(index, name)}
        >
          Remove
        </Button>
      ),
      below: (
        <div className={styles.facts}>
          <TypePill type={binding.manual.type} />
          {codes && <span>{codes}</span>}
        </div>
      ),
    };
  });
  return (
    <LaneList label={`Columns of ${label}, entered by hand`} lanes={lanes} />
  );
}

/** A source the catalog does not document: a plain card with its provider and
 * register as the researcher wrote them, its years, and its columns. Remove
 * only; nothing here is read from or linked to the catalog. */
export function ManualSourceCard({
  draft,
  source,
  sourceIndex,
  issues,
  raw,
  onRemoved,
}: {
  draft: ProjectData;
  source: ProjectManualSource;
  sourceIndex: number;
  issues: ValidationIssue[];
  raw: string | null;
  onRemoved: () => void;
}) {
  const heading = useRef<HTMLSpanElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [removing, setRemoving] = useState<{
    binding: number | null;
    label: string;
  } | null>(null);
  const title = `${source.manual.provider}: ${source.manual.register}`;
  const years = sourcePeriodLabel(source.period);
  const sourcePath = `/sources/${sourceIndex}`;
  // The card's own label says what `manual_item` would say.
  const findings = issues.filter(
    (issue) =>
      issue.code !== "manual_item" &&
      (issue.path === sourcePath || issue.path.startsWith(`${sourcePath}/`)),
  );
  const removesSource =
    removing?.binding === null ||
    (removing !== null && removalRemovesSource(source, removing.binding));
  return (
    <Panel
      headingLevel={3}
      title={
        <span ref={heading} tabIndex={-1} className={sourceStyles.heading}>
          {title}
        </span>
      }
      action={
        <Button
          variant="ghost"
          aria-label={`Remove table ${title}`}
          onPress={() => setRemoving({ binding: null, label: title })}
        >
          Remove table
        </Button>
      }
    >
      <p className={sourceStyles.meta}>
        <span>{MANUAL_LABEL}</span>
        <span>Years: {years ? <code>{years}</code> : "not tied to years"}</span>
      </p>
      {findings.length > 0 && (
        <ul className={styles.plainList}>
          {findings.map((issue) => (
            <LocatedIssue
              key={issueId(issue)}
              issue={issue}
              raw={raw}
              omitPlace={["table", "period"]}
            />
          ))}
        </ul>
      )}
      <ManualColumnList
        label={title}
        columns={manualColumns(source.bindings)}
        labelled={false}
        onRemove={(index, name) => setRemoving({ binding: index, label: name })}
      />
      {error && (
        <StatusRow tone="error" live="alert">
          {error}
        </StatusRow>
      )}
      <ConfirmDialog
        isOpen={removing !== null}
        onOpenChange={(open) => {
          if (!open) setRemoving(null);
        }}
        title={
          removesSource
            ? `Remove table ${title}?`
            : `Remove ${removing?.label ?? "column"} from ${title}?`
        }
        actions={
          <>
            <Button onPress={() => setRemoving(null)}>Cancel</Button>
            <Button
              variant="danger"
              onPress={() => {
                if (!removing) return;
                try {
                  commitSourceEdit(
                    removing.binding === null
                      ? removeSource(draft, sourceIndex)
                      : removeBinding(draft, sourceIndex, removing.binding),
                  );
                  setError(null);
                  if (removesSource) onRemoved();
                  else focusAfterRemoval([() => heading.current]);
                } catch (cause) {
                  setError(`The table was not changed. ${errMessage(cause)}`);
                }
                setRemoving(null);
              }}
            >
              {removesSource ? "Remove table" : "Remove column"}
            </Button>
          </>
        }
      >
        {removesSource
          ? source.bindings.length === 1 && removing?.binding !== null
            ? "This is the table's last column. Removing it also removes the table from this project."
            : "The table and its columns leave this project."
          : "The column leaves this table only."}
        {removesSource &&
          draft.panels?.some((panel) => panel.members.includes(source.name)) &&
          " The table is removed from its panels. Any panel left without members is also removed."}
      </ConfirmDialog>
    </Panel>
  );
}
