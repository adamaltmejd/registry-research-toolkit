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
CREATE INDEX idx_widget_name ON widget(name);
CREATE VIEW widget_names AS SELECT name FROM widget;
