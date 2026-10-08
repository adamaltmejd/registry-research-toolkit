-- a.sql's rows, with a column added that they leave NULL.
CREATE TABLE widget (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  payload BLOB,
  source_id INTEGER
);
ALTER TABLE widget ADD COLUMN note TEXT;
INSERT INTO widget (id, name, payload, source_id) VALUES
  (1, 'alpha', X'000102', 100),
  (2, 'beta', X'FFFE', NULL),
  (3, 'gamma', NULL, 300);
