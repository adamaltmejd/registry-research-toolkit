// The `project_data.json` JSON Schema beside the plugin's register-project skill,
// cut from the committed API contract: `ProjectData` and every component it reaches.
// Run by `bun run gen:types`, so the gate's drift check covers it with the SPA's types.
const ROOT = `${import.meta.dir}/../../..`;
const OUT = `${ROOT}/plugins/microdata-tools-se/skills/register-project/project_data.schema.json`;
const PREFIX = "#/components/schemas/";

const openapi = await Bun.file(`${ROOT}/crates/reg-meta/openapi.json`).json();
const schemas: Record<string, unknown> = openapi.components.schemas;

const defs: Record<string, unknown> = {};
const visit = (node: unknown): void => {
  if (Array.isArray(node)) {
    node.forEach(visit);
    return;
  }
  if (node === null || typeof node !== "object") return;
  for (const [key, value] of Object.entries(node)) {
    if (
      key === "$ref" &&
      typeof value === "string" &&
      value.startsWith(PREFIX)
    ) {
      const name = value.slice(PREFIX.length);
      if (!(name in defs)) {
        if (!(name in schemas))
          throw new Error(`openapi.json: no component ${name}`);
        defs[name] = schemas[name];
        visit(schemas[name]);
      }
    } else visit(value);
  }
};
visit(schemas.ProjectData);
delete defs.ProjectData;

const sorted = Object.fromEntries(
  Object.keys(defs)
    .sort()
    .map((name) => [name, defs[name]]),
);
const document = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  title: "project_data.json",
  ...(schemas.ProjectData as object),
  $defs: sorted,
};
const text = JSON.stringify(document, null, 2).replaceAll(
  `"${PREFIX}`,
  '"#/$defs/',
);
await Bun.write(OUT, `${text}\n`);
