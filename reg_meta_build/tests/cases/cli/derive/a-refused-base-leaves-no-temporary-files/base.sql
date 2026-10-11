-- A database derive copies and then refuses: its manifest names a schema no
-- builder writes, so the refusal comes after the staging copy exists.
CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO import_manifest VALUES ('schema_version', '0.0');
