CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO import_manifest VALUES
  ('schema_version', '5.1.0'),
  ('import_date', '2026-06-01T06:24:08Z'),
  ('input_dir', '/some/path');
