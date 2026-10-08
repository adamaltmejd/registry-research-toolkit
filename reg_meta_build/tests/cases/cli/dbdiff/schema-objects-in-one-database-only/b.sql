-- a.sql's rows; the index dropped, a table and a view added, and
-- widget_names selecting another column list.
CREATE TABLE widget (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  payload BLOB,
  source_id INTEGER
);
INSERT INTO widget VALUES
  (1, 'alpha', X'000102', 100),
  (2, 'beta', X'FFFE', NULL),
  (3, 'gamma', NULL, 300);
CREATE TABLE extra (x INTEGER);
CREATE VIEW widget_names AS SELECT id, name FROM widget;
CREATE VIEW names_and_ids AS SELECT name, id FROM widget;
