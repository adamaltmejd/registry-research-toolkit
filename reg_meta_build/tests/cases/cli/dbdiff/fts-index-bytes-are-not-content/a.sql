CREATE TABLE doc (id INTEGER PRIMARY KEY, body TEXT);
CREATE VIRTUAL TABLE doc_fts USING fts5(body, content='doc');
INSERT INTO doc VALUES (1, 'hello world'), (2, 'goodbye world');
-- Build the external-content index, filling its shadow tables.
INSERT INTO doc_fts(doc_fts) VALUES ('rebuild');
