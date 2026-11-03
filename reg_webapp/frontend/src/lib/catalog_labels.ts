/** Reader vocabulary rendered as ordinary language, shared by facts and evidence. */
const labels: Record<string, string> = {
  unknown: "Not stated",
  clear: "Unambiguous",
  ambiguous: "Ambiguous",
  row_key: "Row identifier",
  time_start: "Start time",
  time_stop: "End time",
  time_point: "Point in time",
  measure: "Measurement",
  code: "Code",
  label: "Label",
  used: "Used in the catalog",
  unused: "Not used in the catalog",
  shadowed: "Primary release takes precedence",
  rejected: "Not admitted",
  withheld: "Excluded from variable assignment",
  gap_marker: "Marks a gap in coverage",
  unplaced: "Could not be placed in time",
  delivered: "Delivered spelling",
  curated: "Curated spelling",
  definition: "Provider definition",
  operational_definition: "Operational definition",
  description: "Description",
  sets: "Sets",
  tags: "Tags",
  variable: "Variable identity",
  classification: "Classification",
  pdf: "PDF",
  parsed: "Parsed document",
  period: "Period",
  identifier: "Identifier",
  sensitive: "Sensitive",
  coded: "Coded",
  role: "Role",
  int: "Integer",
  bool: "Boolean",
  datetime: "Date and time",
  state: "State",
  temporality: "Temporality",
  invariant: "Time independent",
  varying: "Changes over time",
  time_invariant: "Time independent",
  time_varying: "Changes over time",
};

export function catalogWords(value: string | null | undefined): string {
  if (!value) return "Not stated";
  return (
    labels[value] ??
    value
      .replaceAll("_", " ")
      .replaceAll("-", " ")
      .replace(/^./u, (letter) => letter.toUpperCase())
  );
}

export function yesNo(value: boolean | string | null | undefined): string {
  if (value === true || value === "yes") return "Yes";
  if (value === false || value === "no") return "No";
  return "Not stated";
}
